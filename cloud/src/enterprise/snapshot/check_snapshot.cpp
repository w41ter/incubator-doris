#include <brpc/builtin_service.pb.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <butil/strings/string_split.h>
#include <bvar/status.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>

#include "common/defer.h"
#include "common/logging.h"
#include "common/stopwatch.h"
#include "common/util.h"
#include "meta-service/meta_service_schema.h"
#include "meta-store/blob_message.h"
#include "meta-store/keys.h"
#include "meta-store/meta_reader.h"
#include "meta-store/txn_kv.h"
#include "meta-store/versionstamp.h"
#include "recycler/checker.h"
#include "recycler/storage_vault_accessor.h"
#include "recycler/util.h"
#include "snapshot/snapshot_manager.h"
#include "snapshot_helper.h"
#include "snapshot_manager.h"

using namespace doris::cloud;
using namespace std::chrono;

namespace selectdb {

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

int check_snapshot_file_exist(TxnKv* txn_kv, std::string_view instance_id,
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
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn for recycle snapshot").tag("error_code", err);
        return -1;
    }

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
                                 (is_diff > 0 ? std::to_string(is_diff) + " key diff; " : "")
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

int SnapshotManager::check_snapshots(InstanceChecker* checker) {
    int check_res = 0;
    std::string_view instance_id = checker->instance_id();
    AnnotateTag tag("instance_id", instance_id);
    LOG(INFO) << "begin to check cluster snapshots";

    StopWatch stop_watch;
    size_t total_snapshots = 0;
    size_t num_loss = 0;

    DORIS_CLOUD_DEFER {
        int64_t cost = stop_watch.elapsed_us() / 1000'000;

        LOG_INFO("check cluster snapshots, cost={}s", cost)
                .tag("total_snapshots", total_snapshots)
                .tag("num_loss", num_loss);
    };

    std::vector<std::pair<SnapshotPB, Versionstamp>> snapshots;
    {
        MetaReader reader(instance_id, txn_kv_.get());
        TxnErrorCode err = reader.get_snapshots(&snapshots);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to get snapshots").tag("error_code", err);
            return -1;
        }
    }

    for (auto&& [snapshot_pb, snapshot_versionstamp] : snapshots) {
        ++total_snapshots;
        if (is_snapshot_normal(snapshot_pb)) {
            auto* accessor = checker->get_accessor(snapshot_pb.resource_id());
            if (accessor == nullptr) {
                LOG_WARNING("failed to get accessor").tag("resource_id", snapshot_pb.resource_id());
                check_res = -1;
                continue;
            }
            std::string snapshot_id = serialize_snapshot_id(snapshot_versionstamp);
            std::string snapshot_path = "snapshot/" + snapshot_id + "/";
            std::unique_ptr<ListIterator> list_iter;
            if (accessor->list_directory(snapshot_path, &list_iter) == 0) {
                if (!list_iter->has_next()) {
                    LOG_WARNING("snapshot path not exist, snapshot file loss or snapshot key leak")
                            .tag("resource_id", snapshot_pb.resource_id())
                            .tag("snapshot versionstamp", snapshot_id)
                            .tag("snapshot_path", snapshot_path);
                    num_loss++;
                    check_res = 1;
                }
            } else {
                LOG_WARNING("failed to check snapshot path existence")
                        .tag("resource_id", snapshot_pb.resource_id())
                        .tag("snapshot versionstamp", snapshot_id)
                        .tag("snapshot_path", snapshot_path);
                check_res = -1;
            }
        }
    }

    return num_loss > 0 ? 1 : check_res;
}

int SnapshotManager::inverted_check_snapshots(InstanceChecker* checker) {
    int check_res = 0;
    std::string_view instance_id = checker->instance_id();
    AnnotateTag tag("instance_id", instance_id);
    LOG_INFO("begin to inverted check cluster snapshots");

    StopWatch stop_watch;
    size_t total_snapshots = 0;
    size_t num_leak = 0;

    DORIS_CLOUD_DEFER {
        int64_t cost = stop_watch.elapsed_us() / 1000'000;

        LOG_INFO("check cluster snapshots, cost={}s", cost)
                .tag("total_snapshots", total_snapshots)
                .tag("num_leak", num_leak);
    };

    std::vector<StorageVaultAccessor*> accessors;
    checker->get_all_accessor(&accessors);

    for (auto& accessor : accessors) {
        std::unique_ptr<ListIterator> list_iter;
        int ret = accessor->list_directory("snapshot", &list_iter);
        if (ret != 0) {
            return -1;
        }

        // to skip already checked snapshot versionstamp
        std::string already_checked_snapshot_vs;

        for (auto file = list_iter->next(); file.has_value(); file = list_iter->next()) {
            std::vector<std::string> str;
            butil::SplitString(file->path, '/', &str);
            if (str.size() < 2) {
                LOG_WARNING("invalid snapshot file path format").tag("path", file->path);
                continue;
            }
            ++total_snapshots;

            if (!already_checked_snapshot_vs.empty() && str[1] == already_checked_snapshot_vs) {
                // already checked
                continue;
            }

            already_checked_snapshot_vs = str[1];
            int ret = check_snapshot_file_exist(txn_kv_.get(), instance_id,
                                                already_checked_snapshot_vs);
            if (ret == 1) {
                num_leak++;
                check_res = 1;
            } else if (ret != 0) {
                check_res = -1;
            }
        }

        if (!list_iter->is_valid()) {
            LOG(WARNING) << "failed to list snapshot directory. uri=" << accessor->uri();
            return -1;
        }
    }
    return num_leak > 0 ? 1 : check_res;
}

int SnapshotManager::check_mvcc_meta_key(InstanceChecker* checker) {
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
    std::unordered_map<std::string, int64_t> rowset_ref_count_map;
    MetaReader reader(instance_id, txn_kv_.get());
    TxnErrorCode err = reader.get_snapshots(&snapshots);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get snapshots").tag("error_code", err);
        return -1;
    }

    for (auto&& [snapshot_pb, snapshot_versionstamp] : snapshots) {
        if (is_snapshot_normal(snapshot_pb)) {
            // first to check snapshot file exist
            std::string snapshpt_versionstamp_str =
                    serialize_snapshot_versionstamp(snapshot_versionstamp);
            if (check_snapshot_file_exist(txn_kv_.get(), instance_id, snapshpt_versionstamp_str) !=
                0) {
                LOG_WARNING("failed to check rowset objects because snapshot file not exist")
                        .tag("instance_id", instance_id)
                        .tag("snapshot_versionstamp", snapshpt_versionstamp_str);
                continue;
            }
            std::vector<doris::RowsetMetaCloudPB> rowset_metas;

            MetaReader snapshot_reader(instance_id, txn_kv_.get(), snapshot_versionstamp);

            // get all rowsets with this snapshot
            TxnErrorCode err =
                    snapshot_reader.get_all_tablet_rowset_metas(0, INT64_MAX, &rowset_metas, false);
            if (err != TxnErrorCode::TXN_OK) {
                LOG_WARNING("failed to get rowset metas by versionstamp")
                        .tag("instance_id", instance_id)
                        .tag("versionstamp", serialize_snapshot_versionstamp(snapshot_versionstamp))
                        .tag("error_code", err);
                continue;
            }

            total_rowsets += rowset_metas.size();

            for (auto& rowset_meta : rowset_metas) {
                if (rowset_meta.end_version() == 1) {
                    continue;
                }
                std::string rowset_ref_count_key = versioned::data_rowset_ref_count_key(
                        {instance_id, rowset_meta.tablet_id(), rowset_meta.rowset_id_v2()});
                rowset_ref_count_map[rowset_ref_count_key] += 1;
            }
            int ret = check_rowsets_object(txn_kv_.get(), checker, instance_id, rowset_metas);
            if (ret > 0) {
                num_rowsets_loss++;
                check_ret = 1;
            } else if (ret < 0) {
                check_ret = -1;
            }
        }
    }

    if (check_ret != 0) {
        LOG_WARNING("failed to check rowset objects");
    }

    check_ret = check_rowset_ref_count_map(rowset_ref_count_map, txn_kv_.get(), instance_id);

    if (check_ret != 0) {
        LOG_WARNING("failed to check rowset ref count keys");
    }

    return num_rowsets_loss > 0 ? 1 : check_ret;
}

int SnapshotManager::inverted_check_mvcc_meta_key(InstanceChecker* checker) {
    // TODO:wyxxxcat
    return 0;
}

} // namespace selectdb