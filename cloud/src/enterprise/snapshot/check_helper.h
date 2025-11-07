#include <brpc/builtin_service.pb.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <butil/strings/string_split.h>
#include <bvar/status.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>

#include <algorithm>
#include <cstdio>

#include "common/defer.h"
#include "common/logging.h"
#include "common/stopwatch.h"
#include "common/util.h"
#include "meta-service/meta_service_schema.h"
#include "meta-store/blob_message.h"
#include "meta-store/document_message.h"
#include "meta-store/keys.h"
#include "meta-store/meta_reader.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versionstamp.h"
#include "recycler/checker.h"
#include "recycler/meta_checker.h"
#include "recycler/storage_vault_accessor.h"
#include "recycler/util.h"
#include "snapshot/snapshot_manager.h"
#include "snapshot_helper.h"
#include "snapshot_manager.h"

using namespace doris::cloud;
using namespace std::chrono;

namespace selectdb {

struct TabletRowsetsCache {
    int64_t tablet_id {0};
    std::unordered_set<std::string> rowset_ids;
};

struct RowsetIndexesFormatV1 {
    std::string rowset_id;
    std::unordered_set<int64_t> segment_ids;
    std::unordered_set<std::string> index_ids;
};

struct RowsetIndexesFormatV2 {
    std::string rowset_id;
    std::unordered_set<int64_t> segment_ids;
};

bool is_snapshot_normal(const SnapshotPB& snapshot_pb) {
    switch (snapshot_pb.status()) {
    case SnapshotStatus::SNAPSHOT_PREPARE:
    case SnapshotStatus::SNAPSHOT_ABORTED:
    case SnapshotStatus::SNAPSHOT_RECYCLED:
        return false;
    case SnapshotStatus::SNAPSHOT_NORMAL:
        return !is_snapshot_expired(snapshot_pb);
    default:
        // unknown status
        return false;
    }
}

int check_rowsets_object(TxnKv* txn_kv, InstanceChecker* checker, const std::string& instance_id,
                         std::vector<doris::RowsetMetaCloudPB>& rowset_metas) {
    struct TabletFiles {
        int64_t tablet_id {0};
        std::unordered_set<std::string> files;
    } tablet_files_cache;

    int num_index_file_loss = 0;
    int num_segment_file_loss = 0;
    bool data_loss = false;
    bool segment_file_loss = false;
    bool index_file_loss = false;

    DORIS_CLOUD_DEFER {
        if (data_loss) {
            LOG(INFO) << "segment file is" << (segment_file_loss ? "" : " not") << " loss, "
                      << "index file is" << (index_file_loss ? "" : " not") << " loss, "
                      << "total rowsets=" << rowset_metas.size()
                      << ", lost segment_file=" << num_segment_file_loss
                      << ", lost index_file=" << num_index_file_loss;
        }
    };

    int check_ret = 0;
    MetaReader reader(instance_id, txn_kv);

    for (auto& rs_meta : rowset_metas) {
        if (rs_meta.num_segments() == 0) {
            // empty rowset, skip
            continue;
        }

        if (tablet_files_cache.tablet_id != rs_meta.tablet_id()) {
            // Clear cache
            tablet_files_cache.tablet_id = 0;
            tablet_files_cache.files.clear();
            // Get all file paths under this tablet directory
            auto* accessor = checker->get_accessor(rs_meta.resource_id());
            if (accessor == nullptr) {
                LOG_WARNING("resource id not found in accessor map")
                        .tag("resource_id", rs_meta.resource_id())
                        .tag("tablet_id", rs_meta.tablet_id())
                        .tag("rowset_id", rs_meta.rowset_id_v2());
                check_ret = -1;
                continue;
            }

            std::unique_ptr<ListIterator> list_iter;
            int ret = accessor->list_directory(tablet_path_prefix(rs_meta.tablet_id()), &list_iter);
            if (ret != 0) { // No need to log, because S3Accessor has logged this error
                check_ret = -1;
                continue;
            }

            for (auto file = list_iter->next(); file.has_value(); file = list_iter->next()) {
                tablet_files_cache.files.insert(std::move(file->path));
            }
            tablet_files_cache.tablet_id = rs_meta.tablet_id();
        }

        for (int i = 0; i < rs_meta.num_segments(); ++i) {
            auto path = segment_path(rs_meta.tablet_id(), rs_meta.rowset_id_v2(), i);

            if (tablet_files_cache.files.contains(path)) {
                continue;
            }

            data_loss = true;
            segment_file_loss = true;
            num_segment_file_loss++;
            LOG(WARNING) << "object not exist, path=" << path
                         << ", rs_meta=" << rs_meta.ShortDebugString();
            check_ret = 1;
        }

        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to init txn, err=" << err;
            check_ret = -1;
            continue;
        }

        TabletIndexPB tablet_index;
        if (reader.get_tablet_index(rs_meta.tablet_id(), &tablet_index, false) !=
            TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to get tablet index, tablet_id= " << rs_meta.tablet_id();
            check_ret = -1;
            continue;
        }

        auto tablet_schema_key =
                meta_schema_key({instance_id, tablet_index.index_id(), rs_meta.schema_version()});
        doris::TabletSchemaCloudPB tablet_schema;
        if (reader.get_tablet_schema(txn.get(), tablet_index.index_id(), rs_meta.schema_version(),
                                     &tablet_schema, false) != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to get tablet schema, index_id=" << tablet_index.index_id()
                         << ", schema_version=" << rs_meta.schema_version();
            check_ret = -1;
            continue;
        }

        std::vector<std::pair<int64_t, std::string>> index_ids;
        for (const auto& i : tablet_schema.index()) {
            if (i.has_index_type() && i.index_type() == doris::IndexType::INVERTED) {
                index_ids.emplace_back(i.index_id(), i.index_suffix_name());
            }
        }
        if (!index_ids.empty()) {
            for (int i = 0; i < rs_meta.num_segments(); ++i) {
                std::vector<std::string> index_path_v;
                if (tablet_schema.inverted_index_storage_format() ==
                    doris::InvertedIndexStorageFormatPB::V1) {
                    for (const auto& index_id : index_ids) {
                        LOG(INFO) << "check inverted index, tablet_id=" << rs_meta.tablet_id()
                                  << " rowset_id=" << rs_meta.rowset_id_v2() << " segment_id=" << i
                                  << " index_id=" << index_id.first
                                  << " index_suffix_name=" << index_id.second;
                        index_path_v.emplace_back(
                                inverted_index_path_v1(rs_meta.tablet_id(), rs_meta.rowset_id_v2(),
                                                       i, index_id.first, index_id.second));
                    }
                } else {
                    index_path_v.emplace_back(
                            inverted_index_path_v2(rs_meta.tablet_id(), rs_meta.rowset_id_v2(), i));
                }

                if (std::ranges::all_of(index_path_v, [&](const auto& idx_file_path) {
                        if (!tablet_files_cache.files.contains(idx_file_path)) {
                            LOG(INFO) << "loss index file: " << idx_file_path;
                            num_index_file_loss++;
                            return false;
                        }
                        return true;
                    })) {
                    continue;
                }
                index_file_loss = true;
                data_loss = true;
                check_ret = 1;
            }
        }
    }
    return check_ret;
}

int check_snapshot_key_exist(TxnKv* txn_kv, std::string_view instance_id,
                             const std::string& snapshot_id) {
    if (!txn_kv || instance_id.empty() || snapshot_id.empty()) {
        return -1;
    }
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn for recycle snapshot").tag("error_code", err);
        return -1;
    }

    Versionstamp versionstamp;
    if (!SnapshotManager::parse_snapshot_versionstamp(snapshot_id, &versionstamp)) {
        LOG_WARNING("invalid snapshot versionstamp format")
                .tag("instance_id", instance_id)
                .tag("snapshot_versionstamp", snapshot_id);
    }

    std::string snapshot_key =
            encode_versioned_key(versioned::snapshot_full_key(instance_id), versionstamp);

    err = key_exists(txn.get(), snapshot_key);
    if (err != TxnErrorCode::TXN_OK) {
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            LOG_WARNING("snapshot key not found, snapshot key loss or snapshot file leak")
                    .tag("instance_id", instance_id)
                    .tag("key", hex(snapshot_key))
                    .tag("snapshot_versionstamp", snapshot_id);
            return 1;
        }
        LOG_WARNING("failed to get snapshot key")
                .tag("instance_id", instance_id)
                .tag("error_code", err)
                .tag("key", hex(snapshot_key))
                .tag("snapshot_versionstamp", snapshot_id);

        return -1;
    }

    return 0;
}

int check_rowset_ref_count_map(const std::unordered_map<std::string, int64_t>& rowset_ref_count_map,
                               TxnKv* txn_kv, const std::string& instance_id) {
    int check_ret = 0;
    int64_t is_loss = 0;
    int64_t is_diff = 0;
    LOG(INFO) << "begin to check rowset ref count keys, total rowset ref count keys="
              << rowset_ref_count_map.size() << ", instance_id=" << instance_id;

    DORIS_CLOUD_DEFER {
        if (is_loss > 0 || is_diff > 0) {
            LOG(INFO) << "rowset ref count key check result: "
                      << (is_loss > 0 ? std::to_string(is_loss) + " key loss; " : "") +
                                 (is_diff > 0 ? std::to_string(is_diff) + " key diff, " : "")
                      << " total rowset ref count keys=" << rowset_ref_count_map.size();
        }
    };

    for (const auto& [rowset_ref_count_key, ref_count] : rowset_ref_count_map) {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to create txn for recycle snapshot").tag("error_code", err);
            return -1;
        }
        int64_t rowset_ref_count = 0;
        std::string rowset_ref_count_value;
        err = txn->get(rowset_ref_count_key, &rowset_ref_count_value, false);
        if (err != TxnErrorCode::TXN_OK) {
            if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
                LOG_WARNING("failed to get rowset ref count key")
                        .tag("instance_id", instance_id)
                        .tag("rowset_ref_count_key", hex(rowset_ref_count_key))
                        .tag("error_code", err);
                check_ret = 1;
                continue;
            }
            is_loss++;
            LOG_WARNING("rowset ref count key not found, rowset key loss or rowset file leak")
                    .tag("instance_id", instance_id)
                    .tag("rowset_ref_count_key", hex(rowset_ref_count_key));
            continue;
        }
        if (!txn->decode_atomic_int(rowset_ref_count_value, &rowset_ref_count)) {
            LOG_WARNING("failed to decode rowset data ref count")
                    .tag("value", hex(rowset_ref_count_value));
            check_ret = 1;
            continue;
        }
        if (rowset_ref_count != ref_count) {
            is_diff++;
            LOG_WARNING("rowset ref count not match")
                    .tag("instance_id", instance_id)
                    .tag("expected_ref_count", ref_count)
                    .tag("actual_ref_count", rowset_ref_count);
        }
    }

    if (check_ret != 0) {
        return check_ret;
    }
    return (is_loss > 0 || is_diff > 0) ? 1 : 0;
}

int check_inverted_index_file_storage_format_v1(TxnKv* txn_kv, const std::string& instance_id,
                                                int64_t tablet_id, const std::string& file_path,
                                                const std::string& rowset_info,
                                                RowsetIndexesFormatV1& rowset_index_cache_v1) {
    // format v1: data/{tablet_id}/{rowset_id}_{seg_num}_{idx_id}{idx_suffix}.idx
    std::string rowset_id;
    int64_t segment_id;
    std::string index_id_with_suffix_name;
    // {rowset_id}_{seg_num}_{idx_id}{idx_suffix}.idx
    std::vector<std::string> str;
    butil::SplitString(rowset_info.substr(0, rowset_info.size() - 4), '_', &str);
    if (str.size() < 3) {
        LOG(WARNING) << "Split rowset info with '_' error, str size < 3, rowset_info = "
                     << rowset_info;
        return -1;
    }
    rowset_id = str[0];
    segment_id = std::atoll(str[1].c_str());
    index_id_with_suffix_name = str[2];

    if (rowset_index_cache_v1.rowset_id == rowset_id) {
        if (rowset_index_cache_v1.segment_ids.contains(segment_id)) {
            if (auto it = rowset_index_cache_v1.index_ids.find(index_id_with_suffix_name);
                it == rowset_index_cache_v1.index_ids.end()) {
                // clang-format off
                LOG(WARNING) << fmt::format("index_id with suffix name not found, rowset_info = {}, obj_key = {}", rowset_info, file_path);
                // clang-format on
                return -1;
            }
        } else {
            // clang-format off
            LOG(WARNING) << fmt::format("segment id not found, rowset_info = {}, obj_key = {}", rowset_info, file_path);
            // clang-format on
            return -1;
        }
    }

    rowset_index_cache_v1.rowset_id = rowset_id;
    rowset_index_cache_v1.segment_ids.clear();
    rowset_index_cache_v1.index_ids.clear();

    std::vector<doris::RowsetMetaCloudPB> rowset_metas;
    MetaReader reader(instance_id, txn_kv);
    TxnErrorCode err = reader.get_rowset_metas(tablet_id, 0, INT64_MAX - 1, &rowset_metas, false);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to get rowset metas by tablet id"
                     << ", error_code=" << err;
        return -1;
    }
    for (const auto& rs_meta : rowset_metas) {
        TabletIndexPB tablet_index;
        err = reader.get_tablet_index(rs_meta.tablet_id(), &tablet_index, false);
        if (err != TxnErrorCode::TXN_OK) {
            if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
                LOG(WARNING) << "tablet index not found, tablet_id= " << rs_meta.tablet_id();
            } else {
                LOG(WARNING) << "failed to get tablet index, tablet_id= " << rs_meta.tablet_id();
                return -1;
            }
            continue;
        }
        doris::TabletSchemaCloudPB tablet_schema;
        err = reader.get_tablet_schema(tablet_index.index_id(), rs_meta.schema_version(),
                                       &tablet_schema, false);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to get tablet schema, index_id=" << tablet_index.index_id()
                         << ", schema_version=" << rs_meta.schema_version();
            continue;
        }

        for (size_t i = 0; i < rs_meta.num_segments(); i++) {
            rowset_index_cache_v1.segment_ids.insert(i);
        }

        for (const auto& i : tablet_schema.index()) {
            if (i.has_index_type() && i.index_type() == doris::IndexType::INVERTED) {
                LOG(INFO) << fmt::format("record index info, index_id: {}, index_suffix_name: {}",
                                         i.index_id(), i.index_suffix_name());
                rowset_index_cache_v1.index_ids.insert(
                        fmt::format("{}{}", i.index_id(), i.index_suffix_name()));
            }
        }
    }

    if (!rowset_index_cache_v1.segment_ids.contains(segment_id)) {
        // Garbage data leak
        // clang-format off
        LOG(WARNING) << "rowset_index_cache_v1.segment_ids don't contains segment_id, rowset should be recycled,"
                     << " key = " << file_path
                     << " segment_id = " << segment_id;
        // clang-format on
        return 1;
    }

    if (!rowset_index_cache_v1.index_ids.contains(index_id_with_suffix_name)) {
        // Garbage data leak
        // clang-format off
        LOG(WARNING) << "rowset_index_cache_v1.index_ids don't contains index_id_with_suffix_name,"
                     << " rowset with inde meta should be recycled, key=" << file_path
                     << " index_id_with_suffix_name=" << index_id_with_suffix_name;
        // clang-format on
        return 1;
    }

    return 0;
}

int check_inverted_index_file_storage_format_v2(TxnKv* txn_kv, const std::string& instance_id,
                                                int64_t tablet_id, const std::string& file_path,
                                                const std::string& rowset_info,
                                                RowsetIndexesFormatV2& rowset_index_cache_v2) {
    std::string rowset_id;
    int64_t segment_id;
    // {rowset_id}_{seg_num}.idx
    std::vector<std::string> str;
    butil::SplitString(rowset_info.substr(0, rowset_info.size() - 4), '_', &str);
    if (str.size() < 2) {
        // clang-format off
        LOG(WARNING) << "Split rowset info with '_' error, str size < 2, rowset_info = " << rowset_info;
        // clang-format on
        return -1;
    }
    rowset_id = str[0];
    segment_id = std::atoll(str[1].c_str());

    if (rowset_index_cache_v2.rowset_id == rowset_id) {
        if (!rowset_index_cache_v2.segment_ids.contains(segment_id)) {
            // clang-format off
            LOG(WARNING) << fmt::format("index file not found, rowset_info = {}, obj_key = {}", rowset_info, file_path);
            // clang-format on
            return -1;
        }
    }

    rowset_index_cache_v2.rowset_id = rowset_id;
    rowset_index_cache_v2.segment_ids.clear();

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to create txn";
        return -1;
    }
    std::unique_ptr<RangeGetIterator> it;
    auto begin = meta_rowset_key({instance_id, tablet_id, 0});
    auto end = meta_rowset_key({instance_id, tablet_id, INT64_MAX});
    do {
        TxnErrorCode err = txn->get(begin, end, &it);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to get rowset kv, err=" << err;
            return -1;
        }
        if (!it->has_next()) {
            break;
        }
        while (it->has_next()) {
            // recycle corresponding resources
            auto [k, v] = it->next();
            doris::RowsetMetaCloudPB rs_meta;
            if (!rs_meta.ParseFromArray(v.data(), v.size())) {
                LOG(WARNING) << "malformed rowset meta value, key=" << hex(k);
                return -1;
            }

            for (size_t i = 0; i < rs_meta.num_segments(); i++) {
                rowset_index_cache_v2.segment_ids.insert(i);
            }

            if (!it->has_next()) {
                begin = k;
                begin.push_back('\x00'); // Update to next smallest key for iteration
                break;
            }
        }
    } while (it->more());

    if (!rowset_index_cache_v2.segment_ids.contains(segment_id)) {
        // Garbage data leak
        LOG(WARNING) << "rowset with index meta should be recycled, key=" << file_path;
        return 1;
    }

    return 0;
}

int check_inverted_index_file(TxnKv* txn_kv, const std::string& instance_id,
                              const std::string& path, RowsetIndexesFormatV1& rowset_index_cache_v1,
                              RowsetIndexesFormatV2& rowset_index_cache_v2) {
    std::vector<std::string> str;
    butil::SplitString(path, '/', &str);
    // format v1: data/{tablet_id}/{rowset_id}_{seg_num}_{idx_id}{idx_suffix}.idx
    // format v2: data/{tablet_id}/{rowset_id}_{seg_num}.idx
    if (str.size() < 3) {
        // clang-format off
            LOG(WARNING) << "split obj_key error, str.size() should be less than 3,"
                         << " value = " << str.size();
        // clang-format on
        return -1;
    }

    int64_t tablet_id = atol(str[1].c_str());
    if (tablet_id <= 0) {
        LOG(WARNING) << "failed to parse tablet_id, key=" << path;
        return -1;
    }

    // v1: {rowset_id}_{seg_num}_{idx_id}{idx_suffix}.idx
    // v2: {rowset_id}_{seg_num}.idx
    std::string rowset_info = str.back();

    if (!rowset_info.ends_with(".idx")) {
        return 0; // Not an index file
    }

    doris::InvertedIndexStorageFormatPB inverted_index_storage_format =
            std::count(rowset_info.begin(), rowset_info.end(), '_') > 1
                    ? doris::InvertedIndexStorageFormatPB::V1
                    : doris::InvertedIndexStorageFormatPB::V2;

    size_t pos = rowset_info.find_last_of('_');
    if (pos == std::string::npos || pos + 1 >= str.back().size() - 4) {
        LOG(WARNING) << "Invalid index_id format, key=" << path;
        return -1;
    }
    if (inverted_index_storage_format == doris::InvertedIndexStorageFormatPB::V1) {
        return check_inverted_index_file_storage_format_v1(txn_kv, instance_id, tablet_id, path,
                                                           rowset_info, rowset_index_cache_v1);
    } else {
        return check_inverted_index_file_storage_format_v2(txn_kv, instance_id, tablet_id, path,
                                                           rowset_info, rowset_index_cache_v2);
    }
}

int check_mvcc_meta_rowset_key(InstanceChecker* checker, TxnKv* txn_kv) {
    int check_ret = 0;
    std::string instance_id(checker->instance_id().data(), checker->instance_id().size());
    AnnotateTag tag("instance_id", instance_id);
    LOG_INFO("begin to check mvcc meta keys");

    StopWatch stop_watch;
    size_t total_rowsets = 0;
    size_t num_rowsets_loss = 0;

    DORIS_CLOUD_DEFER {
        int64_t cost = stop_watch.elapsed_us() / 1000'000;

        LOG_INFO("check cluster snapshots, cost={}s", cost)
                .tag("total_rowsets", total_rowsets)
                .tag("num_rowsets_loss", num_rowsets_loss);
    };

    std::vector<std::pair<SnapshotPB, Versionstamp>> snapshots;
    // rowset ref count key -> reference count
    std::unordered_map<std::string, int64_t> rowset_ref_count_map;
    MetaReader reader(instance_id, txn_kv);
    TxnErrorCode err = reader.get_snapshots(&snapshots);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get snapshots").tag("error_code", err);
        return -1;
    }

    for (auto&& [snapshot_pb, snapshot_versionstamp] : snapshots) {
        if (is_snapshot_normal(snapshot_pb)) {
            // first to check snapshot file exist
            std::string snapshot_versionstamp_str =
                    serialize_snapshot_versionstamp(snapshot_versionstamp);
            if (check_snapshot_key_exist(txn_kv, instance_id, snapshot_versionstamp_str) != 0) {
                LOG_WARNING("failed to check rowset objects because snapshot key not exist")
                        .tag("instance_id", instance_id)
                        .tag("snapshot_versionstamp", snapshot_versionstamp_str);
                continue;
            }
            std::vector<int64_t> tablet_ids;
            std::vector<doris::RowsetMetaCloudPB> rowset_metas;

            MetaReader snapshot_reader(instance_id, txn_kv, snapshot_versionstamp);

            // get all rowsets with this snapshot
            TxnErrorCode err = snapshot_reader.get_all_tablet_ids(&tablet_ids, false);

            for (auto tablet_id : tablet_ids) {
                std::vector<doris::RowsetMetaCloudPB> rowset_metas;
                err = snapshot_reader.get_rowset_metas(tablet_id, 0, INT64_MAX - 1, &rowset_metas,
                                                       false);

                if (err != TxnErrorCode::TXN_OK) {
                    LOG_WARNING("failed to get rowset metas by tablet id and versionstamp")
                            .tag("instance_id", instance_id)
                            .tag("versionstamp",
                                 serialize_snapshot_versionstamp(snapshot_versionstamp))
                            .tag("tablet_id", tablet_id)
                            .tag("error_code", err);
                    continue;
                }

                LOG_INFO("get rowset metas by versionstamp")
                        .tag("instance_id", instance_id)
                        .tag("versionstamp", serialize_snapshot_versionstamp(snapshot_versionstamp))
                        .tag("tablet_id", tablet_id)
                        .tag("num_rowsets", rowset_metas.size())
                        .tag("error_code", err);

                total_rowsets += rowset_metas.size();

                for (auto& rowset_meta : rowset_metas) {
                    if (rowset_meta.end_version() == 1) {
                        continue;
                    }
                    std::string rowset_ref_count_key = versioned::data_rowset_ref_count_key(
                            {instance_id, rowset_meta.tablet_id(), rowset_meta.rowset_id_v2()});
                    rowset_ref_count_map[rowset_ref_count_key] += 1;
                }
                int ret = check_rowsets_object(txn_kv, checker, instance_id, rowset_metas);
                if (ret > 0) {
                    num_rowsets_loss++;
                    check_ret = 1;
                } else if (ret < 0) {
                    check_ret = -1;
                }
            }
        }
    }

    if (check_ret != 0) {
        LOG_WARNING("failed to check rowset objects");
    }

    check_ret = check_rowset_ref_count_map(rowset_ref_count_map, txn_kv, instance_id);

    if (check_ret != 0) {
        LOG_WARNING("failed to check rowset ref count keys");
    }

    return num_rowsets_loss > 0 ? 1 : check_ret;
}

int check_rowset_key_exist(TxnKv* txn_kv, std::string_view instance_id, const std::string& path,
                           TabletRowsetsCache& tablet_rowsets_cache) {
    if (!txn_kv || instance_id.empty() || path.empty()) {
        return -1;
    }

    // Return 0 if check success, return 1 if file is garbage data, negative if error occurred
    std::vector<std::string> str;
    butil::SplitString(path, '/', &str);
    // data/{tablet_id}/{rowset_id}_{seg_num}.dat
    if (str.size() < 3) {
        // clang-format off
            LOG(WARNING) << "split path error, str.size() should be less than 3,"
                         << " value = " << str.size();
        // clang-format on
        return -1;
    }

    int64_t tablet_id = atol(str[1].c_str());
    if (tablet_id <= 0) {
        LOG(WARNING) << "failed to parse tablet_id, key=" << path;
        return -1;
    }

    if (!str[2].ends_with(".dat")) {
        // skip check not segment file
        return 0;
    }

    std::string rowset_id;
    if (auto pos = str.back().find('_'); pos != std::string::npos) {
        rowset_id = str.back().substr(0, pos);
    } else {
        LOG(WARNING) << "failed to parse rowset_id, key=" << path;
        return -1;
    }

    if (tablet_rowsets_cache.tablet_id == tablet_id) {
        if (tablet_rowsets_cache.rowset_ids.contains(rowset_id)) {
            return 0;
        } else {
            LOG(WARNING) << "rowset not exists, key=" << path;
            return -1;
        }
    }
    // Get all rowset id of this tablet
    tablet_rowsets_cache.tablet_id = tablet_id;
    tablet_rowsets_cache.rowset_ids.clear();
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to create txn";
        return -1;
    }
    MetaReader reader(instance_id, txn_kv);
    std::vector<doris::RowsetMetaCloudPB> rowset_metas;
    err = reader.get_rowset_metas(txn.get(), tablet_id, 0, INT64_MAX, &rowset_metas, false);
    std::ranges::transform(
            rowset_metas,
            std::inserter(tablet_rowsets_cache.rowset_ids, tablet_rowsets_cache.rowset_ids.end()),
            [](const doris::RowsetMetaCloudPB& meta) { return meta.rowset_id_v2(); });
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to get all tablet rowset metas"
                     << ", error_code=" << err;
        return -1;
    }

    if (!tablet_rowsets_cache.rowset_ids.contains(rowset_id)) {
        // Garbage data leak
        LOG(WARNING) << "rowset should be recycled, key=" << path;
        return 1;
    }

    return 0;
}

int inverted_check_mvcc_meta_rowset_key(InstanceChecker* checker, TxnKv* txn_kv) {
    int check_ret = 0;
    std::string instance_id(checker->instance_id().data(), checker->instance_id().size());
    AnnotateTag tag("instance_id", instance_id);
    LOG_INFO("begin to inverted check mvcc meta keys");
    std::vector<StorageVaultAccessor*> accessors;
    checker->get_all_accessor(&accessors);

    std::vector<int64_t> tablet_ids;
    int64_t num_loss = 0;
    int64_t num_scan = 0;

    DORIS_CLOUD_DEFER {
        LOG_INFO("inverted check cluster snapshots")
                .tag("total_scan_files", num_scan)
                .tag("total_loss_files", num_loss);
    };

    std::vector<std::pair<SnapshotPB, Versionstamp>> snapshots;
    MetaReader reader(instance_id, txn_kv);
    TxnErrorCode err = reader.get_snapshots(&snapshots);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get snapshots").tag("error_code", err);
        return -1;
    }

    for (const auto& [snapshot_pb, snapshot_versionstamp] : snapshots) {
        if (!is_snapshot_normal(snapshot_pb)) {
            continue;
        }
        MetaReader snapshot_reader(instance_id, txn_kv, snapshot_versionstamp);
        std::vector<int64_t> tablet_ids_t;
        err = snapshot_reader.get_all_tablet_ids(&tablet_ids_t, false);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to get all tablet ids by snapshot versionstamp")
                    .tag("instance_id", instance_id)
                    .tag("versionstamp", serialize_snapshot_versionstamp(snapshot_versionstamp))
                    .tag("error_code", err);
            return -1;
        }
        tablet_ids.insert(tablet_ids.end(), tablet_ids_t.begin(), tablet_ids_t.end());
    }

    for (auto& accessor : accessors) {
        for (const auto& tablet_id : tablet_ids) {
            doris::TabletMetaCloudPB tablet_meta;
            Versionstamp tablet_versionstamp;
            MetaReader snapshot_reader(instance_id, txn_kv);

            // first: check tablet meta key exist
            err = snapshot_reader.get_tablet_meta(tablet_id, &tablet_meta, &tablet_versionstamp);
            if (err != TxnErrorCode::TXN_OK) {
                if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
                    // meta tablet key not exist, but meta tablet index key exist
                    LOG_WARNING("tablet key not found, meta_tablet_key loss")
                            .tag("instance_id", instance_id)
                            .tag("tablet_id", tablet_id);
                    check_ret = 1;
                } else {
                    LOG_WARNING("failed to get tablet meta")
                            .tag("instance_id", instance_id)
                            .tag("tablet_id", tablet_id);
                    return -1;
                }
                continue;
            }

            // second: check rowset keys exist
            std::unique_ptr<ListIterator> list_iter;
            std::string tablet_path = "data/" + std::to_string(tablet_meta.tablet_id()) + "/";
            int ret = accessor->list_directory(tablet_path, &list_iter);
            if (ret != 0) {
                LOG_WARNING("failed to list data directory")
                        .tag("instance_id", instance_id)
                        .tag("tablet_path", tablet_path)
                        .tag("error_code", ret)
                        .tag("uri", accessor->uri());
                return -1;
            }

            TabletRowsetsCache tablet_rowsets_cache;
            RowsetIndexesFormatV1 rowset_index_cache_v1;
            RowsetIndexesFormatV2 rowset_index_cache_v2;

            for (auto file = list_iter->next(); file.has_value(); file = list_iter->next()) {
                num_scan++;
                if (check_rowset_key_exist(txn_kv, instance_id, file->path, tablet_rowsets_cache) !=
                    0) {
                    num_loss++;
                    LOG_WARNING("failed to check rowset key because rowset key not exist")
                            .tag("instance_id", instance_id)
                            .tag("rowset_key", file->path);
                    check_ret = 1;
                }
                if (check_inverted_index_file(txn_kv, instance_id, file->path,
                                              rowset_index_cache_v1, rowset_index_cache_v2) != 0) {
                    num_loss++;
                    LOG_WARNING("failed to check inverted index file")
                            .tag("instance_id", instance_id)
                            .tag("index_file_key", file->path);
                    check_ret = 1;
                }
            }
            if (!list_iter->is_valid()) {
                LOG(WARNING) << "failed to list data directory. uri=" << accessor->uri();
                return -1;
            }
        }
    }
    return check_ret;
}

} // namespace selectdb