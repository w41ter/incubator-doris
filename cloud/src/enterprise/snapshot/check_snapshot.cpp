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
#include "meta-store/keys.h"
#include "meta-store/meta_reader.h"
#include "meta-store/versioned_value.h"
#include "meta-store/versionstamp.h"
#include "recycler/checker.h"
#include "recycler/storage_vault_accessor.h"
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

int check_snapshot_file(TxnKv* txn_kv, std::string_view instance_id,
                        const std::string& snapshot_versionstamp) {
    if (!txn_kv || instance_id.empty() || snapshot_versionstamp.empty()) {
        return -1;
    }
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn for recycle snapshot").tag("error_code", err);
        return -1;
    }
    Versionstamp versionstamp = parse_snapshot_versionstamp(snapshot_versionstamp);
    std::string snapshot_key =
            encode_versioned_key(versioned::snapshot_full_key(instance_id), versionstamp);

    err = key_exists(txn.get(), snapshot_key);
    if (err != TxnErrorCode::TXN_OK) {
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            LOG_WARNING("snapshot key not found, snapshot key loss or snapshot file leak")
                    .tag("instance_id", instance_id)
                    .tag("key", hex(snapshot_key))
                    .tag("snapshot_versionstamp", snapshot_versionstamp);
            return 1;
        }
        LOG_WARNING("failed to get snapshot key")
                .tag("instance_id", instance_id)
                .tag("error_code", err)
                .tag("key", hex(snapshot_key))
                .tag("snapshot_versionstamp", snapshot_versionstamp);

        return -1;
    }

    return 0;
}

int SnapshotManager::check_snapshots(InstanceChecker* checker) {
    int check_res = 0;
    std::string_view instance_id = checker->instance_id();
    AnnotateTag tag("instance_id", instance_id);
    LOG_INFO("begin to check cluster snapshots");

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
            std::string snapshot_versionstamp_str =
                    serialize_snapshot_versionstamp(snapshot_versionstamp);
            std::string snapshot_path = "snapshot/" + snapshot_versionstamp_str + "/";
            std::unique_ptr<ListIterator> list_iter;
            if (accessor->list_directory(snapshot_path, &list_iter) == 0) {
                if (!list_iter->has_next()) {
                    LOG_WARNING("snapshot path not exist, snapshot file loss or snapshot key leak")
                            .tag("resource_id", snapshot_pb.resource_id())
                            .tag("snapshot versionstamp", snapshot_versionstamp_str)
                            .tag("snapshot_path", snapshot_path);
                    num_loss++;
                    check_res = 1;
                }
            } else {
                LOG_WARNING("failed to check snapshot path existence")
                        .tag("resource_id", snapshot_pb.resource_id())
                        .tag("snapshot versionstamp", snapshot_versionstamp_str)
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
            int ret = check_snapshot_file(txn_kv_.get(), instance_id, already_checked_snapshot_vs);
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

} // namespace selectdb