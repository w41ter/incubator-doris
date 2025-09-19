#pragma once

#include <gen_cpp/cloud.pb.h>

#include "meta-store/txn_kv.h"
#include "recycler/recycler.h"
#include "recycler/storage_vault_accessor.h"
#include "snapshot/snapshot_manager.h"

namespace selectdb {

class SnapshotManager : public doris::cloud::SnapshotManager {
    using TxnKv = doris::cloud::TxnKv;

public:
    using doris::cloud::SnapshotManager::SnapshotManager;
    ~SnapshotManager() override = default;

    void begin_snapshot(std::string_view instance_id,
                        const doris::cloud::BeginSnapshotRequest& request,
                        doris::cloud::BeginSnapshotResponse* response) override;
    void commit_snapshot(std::string_view instance_id,
                         const doris::cloud::CommitSnapshotRequest& request,
                         doris::cloud::CommitSnapshotResponse* response) override;
    void abort_snapshot(std::string_view instance_id,
                        const doris::cloud::AbortSnapshotRequest& request,
                        doris::cloud::AbortSnapshotResponse* response) override;
    void drop_snapshot(std::string_view instance_id,
                       const doris::cloud::DropSnapshotRequest& request,
                       doris::cloud::DropSnapshotResponse* response) override;
    void list_snapshot(std::string_view instance_id,
                       const doris::cloud::ListSnapshotRequest& request,
                       doris::cloud::ListSnapshotResponse* response) override;
    void clone_instance(const doris::cloud::CloneInstanceRequest& request,
                        doris::cloud::CloneInstanceResponse* response) override;

    std::pair<doris::cloud::MetaServiceCode, std::string> set_multi_version_status(
            std::string_view instance_id, std::string_view cloud_unique_id,
            doris::cloud::MultiVersionStatus multi_version_status) override;

    // Recycle snapshots that are expired or marked as recycled, based on the retention policy.
    // Return 0 for success otherwise error.
    int recycle_snapshots(doris::cloud::InstanceRecycler* recycler) override;

    // Recycle snapshot meta and data, return 0 for success otherwise error.
    int recycle_snapshot_meta_and_data(std::string_view instance_id, std::string_view resource_id,
                                       doris::cloud::StorageVaultAccessor* accessor,
                                       doris::cloud::Versionstamp snapshot_version,
                                       const doris::cloud::SnapshotPB& snapshot_pb) override;
};

} // namespace selectdb
