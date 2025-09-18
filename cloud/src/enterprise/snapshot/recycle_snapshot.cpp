#include <brpc/builtin_service.pb.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <butil/strings/string_split.h>
#include <bvar/status.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <string_view>
#include <utility>

#include "common/config.h"
#include "common/defer.h"
#include "common/encryption_util.h"
#include "common/logging.h"
#include "common/stopwatch.h"
#include "common/util.h"
#include "meta-service/meta_service.h"
#include "meta-service/meta_service_helper.h"
#include "meta-service/meta_service_schema.h"
#include "meta-store/codec.h"
#include "meta-store/keys.h"
#include "meta-store/meta_reader.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
#include "recycler/checker.h"
#include "recycler/recycler.h"
#include "recycler/storage_vault_accessor.h"
#include "recycler/util.h"
#include "snapshot/snapshot_manager.h"
#include "snapshot_manager.h"

using namespace doris::cloud;
using namespace std::chrono;

namespace selectdb {

inline int64_t system_clock_now_seconds() {
    return duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
}

// Is the snapshot in preparing status timed out?
bool is_creating_snapshot_timeout(const SnapshotPB& snapshot_pb) {
    if (config::force_immediate_recycle) {
        return true;
    }

    int64_t created_at = snapshot_pb.create_at();
    int64_t deadline = created_at + snapshot_pb.timeout_seconds();
    return system_clock_now_seconds() >= deadline;
}

// Is the aborted snapshot can be pruned?
bool is_aborted_snapshot_pruneable(const SnapshotPB& snapshot_pb) {
    if (config::force_immediate_recycle) {
        return true;
    }

    int64_t aborted_at = snapshot_pb.finish_at();
    int64_t deadline = aborted_at + config::prune_aborted_snapshot_seconds;
    return system_clock_now_seconds() >= deadline;
}

// Is the manually created snapshot expired?
bool is_snapshot_expired(const SnapshotPB& snapshot_pb) {
    if (config::force_immediate_recycle) {
        return true;
    }

    int64_t created_at = snapshot_pb.create_at();
    int64_t deadline = created_at + snapshot_pb.ttl_seconds();
    return system_clock_now_seconds() >= deadline;
}

// Abort the snapshot in preparing status due to timeout.
int abort_timeout_snapshot(TxnKv* txn_kv, std::string_view instance_id,
                           Versionstamp snapshot_versionstamp) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn for abort timeout snapshot").tag("error_code", err);
        return -1;
    }

    std::string snapshot_key =
            encode_versioned_key(versioned::snapshot_full_key(instance_id), snapshot_versionstamp);
    std::string snapshot_value;

    // Read the snapshot again to avoid overwriting changes made by others
    err = txn->get(snapshot_key, &snapshot_value);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get the timeout snapshot")
                .tag("error_code", err)
                .tag("key", hex(snapshot_key));
        return -1;
    }

    SnapshotPB snapshot_pb;
    if (!snapshot_pb.ParseFromArray(snapshot_value.data(), snapshot_value.size())) {
        LOG_WARNING("failed to parse SnapshotPB from snapshot key").tag("key", hex(snapshot_key));
        return -1;
    }

    if (snapshot_pb.status() != SnapshotStatus::SNAPSHOT_PREPARE) {
        // status has been changed, do nothing
        return 0;
    }

    snapshot_value.clear();
    snapshot_pb.set_status(SnapshotStatus::SNAPSHOT_ABORTED);
    snapshot_pb.set_reason("abort due to timeout");
    snapshot_pb.set_finish_at(system_clock_now_seconds());
    if (!snapshot_pb.SerializeToString(&snapshot_value)) {
        LOG_WARNING("failed to serialize SnapshotPB");
        return -1;
    }

    txn->put(snapshot_key, snapshot_value);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to commit txn for abort timeout snapshot").tag("error_code", err);
        return -1;
    }

    return 0;
}

// Mark the normal snapshot as recycled and prune it later.
// Returns 0 on success, -1 on failure, 1 if the snapshot is not recyclable.
int recycle_normal_snapshot(TxnKv* txn_kv, std::string_view instance_id,
                            Versionstamp snapshot_versionstamp) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn for recycle snapshot").tag("error_code", err);
        return -1;
    }

    std::string snapshot_key =
            encode_versioned_key(versioned::snapshot_full_key(instance_id), snapshot_versionstamp);
    std::string snapshot_value;

    // Read the snapshot again to avoid overwriting changes made by others
    err = txn->get(snapshot_key, &snapshot_value);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get snapshot").tag("error_code", err).tag("key", hex(snapshot_key));
        return -1;
    }

    SnapshotPB snapshot_pb;
    if (!snapshot_pb.ParseFromArray(snapshot_value.data(), snapshot_value.size())) {
        LOG_WARNING("failed to parse SnapshotPB from snapshot key").tag("key", hex(snapshot_key));
        return -1;
    }

    if (snapshot_pb.status() != SnapshotStatus::SNAPSHOT_NORMAL) {
        // status has been changed, do nothing
        return 1;
    }

    // Do not recycle the snapshot if it still has references, such as clone or rollback operations.
    // The snapshot will be pruned later when it has no references.
    bool has_references = false;
    MetaReader meta_reader(instance_id);
    err = meta_reader.has_snapshot_references(txn.get(), snapshot_versionstamp, &has_references,
                                              false);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to check snapshot references").tag("error_code", err);
        return -1;
    } else if (has_references) {
        // still has references, do nothing
        return 1;
    }

    snapshot_value.clear();
    snapshot_pb.set_status(SnapshotStatus::SNAPSHOT_RECYCLED);
    if (!snapshot_pb.SerializeToString(&snapshot_value)) {
        LOG_WARNING("failed to serialize SnapshotPB");
        return -1;
    }

    txn->put(snapshot_key, snapshot_value);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to commit txn for recycle snapshot").tag("error_code", err);
        return -1;
    }

    return 0;
}

int SnapshotManager::recycle_snapshots(InstanceRecycler* recycler) {
    std::string_view instance_id = recycler->instance_id();
    AnnotateTag tag("instance_id", instance_id);
    LOG_WARNING("begin to recycle cluster snapshots");

    StopWatch stop_watch;
    size_t total_snapshots = 0;
    size_t recycled_snapshots = 0;

    DORIS_CLOUD_DEFER {
        int64_t cost = stop_watch.elapsed_us() / 1000'000;
        LOG_WARNING("recycle cluster snapshots, cost={}s", cost)
                .tag("total_snapshots", total_snapshots)
                .tag("recycled_snapshots", recycled_snapshots);
    };

    const InstanceInfoPB& instance_info = recycler->instance_info();
    if (instance_info.resource_ids_size() == 0) {
        LOG_WARNING("instance has no resources, cannot recycle snapshots");
        return -1;
    }

    std::string resource_id = recycler->instance_info().resource_ids(0);

    std::vector<std::pair<SnapshotPB, Versionstamp>> snapshots;
    {
        MetaReader reader(instance_id, txn_kv_.get());
        TxnErrorCode err = reader.get_snapshots(&snapshots);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to get snapshots").tag("error_code", err);
            return -1;
        }
    }

    total_snapshots = snapshots.size();
    std::vector<std::pair<SnapshotPB, Versionstamp>> auto_snapshots;
    for (auto&& [snapshot_pb, snapshot_versionstamp] : snapshots) {
        std::string snapshot_id = snapshot_versionstamp.to_string();
        AnnotateTag snapshot_id_tag("snapshot_id", snapshot_id);

        if (snapshot_pb.status() == SnapshotStatus::SNAPSHOT_PREPARE &&
            is_creating_snapshot_timeout(snapshot_pb)) {
            LOG_WARNING("abort snapshot due to timeout")
                    .tag("create_at", snapshot_pb.create_at())
                    .tag("timeout_seconds", snapshot_pb.timeout_seconds());
            // Ignore the error, and try to recycle other snapshots
            if (!abort_timeout_snapshot(txn_kv_.get(), instance_id, snapshot_versionstamp)) {
                recycled_snapshots += 1;
            }
        } else if (snapshot_pb.status() == SnapshotStatus::SNAPSHOT_ABORTED &&
                   is_aborted_snapshot_pruneable(snapshot_pb)) {
            LOG_WARNING("prune aborted snapshot meta and data")
                    .tag("finish_at", snapshot_pb.finish_at());
            // Ignore the error, and try to recycle other snapshots
            recycler->recycle_snapshot_meta_and_data(resource_id, snapshot_versionstamp,
                                                     std::move(snapshot_pb));
        } else if (snapshot_pb.status() == SnapshotStatus::SNAPSHOT_NORMAL && snapshot_pb.auto_()) {
            auto_snapshots.emplace_back(std::move(snapshot_pb), snapshot_versionstamp);
        } else if (snapshot_pb.status() == SnapshotStatus::SNAPSHOT_NORMAL &&
                   is_snapshot_expired(snapshot_pb)) {
            int res = recycle_normal_snapshot(txn_kv_.get(), instance_id, snapshot_versionstamp);
            if (res == 0) {
                LOG_WARNING("recycle expired snapshot")
                        .tag("create_at", snapshot_pb.create_at())
                        .tag("ttl_seconds", snapshot_pb.ttl_seconds());

                recycled_snapshots += 1;
            }
        } else if (snapshot_pb.status() == SnapshotStatus::SNAPSHOT_RECYCLED) {
            LOG_WARNING("prune recycled snapshot meta and data");
            // Ignore the error, and try to recycle other snapshots
            recycler->recycle_snapshot_meta_and_data(resource_id, snapshot_versionstamp,
                                                     std::move(snapshot_pb));
        }
    }

    // Sort auto snapshots by versionstamp.
    std::sort(auto_snapshots.begin(), auto_snapshots.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });

    int64_t max_reserved_snapshot = instance_info.max_reserved_snapshot();
    while (auto_snapshots.size() > max_reserved_snapshot) {
        auto&& [snapshot_pb, snapshot_versionstamp] = auto_snapshots.back();
        std::string snapshot_id = snapshot_versionstamp.to_string();
        AnnotateTag snapshot_id_tag("snapshot_id", snapshot_id);

        int res = recycle_normal_snapshot(txn_kv_.get(), instance_id, snapshot_versionstamp);
        if (res == -1) {
            LOG_WARNING("failed to recycle auto snapshot to keep the max_reserved_snapshot");
            return -1;
        } else if (res == 1) {
            // not recyclable, skip it
            auto_snapshots.pop_back();
            continue;
        }

        LOG_WARNING("recycle auto snapshot to keep max_reserved_snapshot")
                .tag("max_reserved_snapshot", max_reserved_snapshot)
                .tag("create_at", snapshot_pb.create_at())
                .tag("timeout_seconds", snapshot_pb.timeout_seconds());

        recycled_snapshots += 1;
        auto_snapshots.pop_back();
    }

    return 0;
}

int SnapshotManager::recycle_snapshot_meta_and_data(std::string_view instance_id,
                                                    std::string_view resource_id,
                                                    StorageVaultAccessor* accessor,
                                                    Versionstamp snapshot_version,
                                                    const SnapshotPB& snapshot_pb) {
    std::string snapshot_id = snapshot_version.to_string();
    std::string image_dir = "/snapshot/" + snapshot_id + "/";
    int res = accessor->delete_directory(image_dir);
    if (res != 0) {
        LOG_WARNING("failed to delete snapshot files")
                .tag("resource_id", resource_id)
                .tag("snapshot_id", snapshot_id)
                .tag("image_dir", image_dir)
                .tag("result", res);
        return -1;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn for recycle snapshot meta").tag("error_code", err);
        return -1;
    }

    std::string snapshot_key =
            encode_versioned_key(versioned::snapshot_full_key(instance_id), snapshot_version);
    txn->remove(snapshot_key);

    LOG_WARNING("prune snapshot meta and data")
            .tag("resource_id", resource_id)
            .tag("snapshot_id", snapshot_id)
            .tag("image_dir", image_dir)
            .tag("key", hex(snapshot_key));

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to commit txn for recycle snapshot meta").tag("error_code", err);
        return -1;
    }

    return 0;
}

} // namespace selectdb
