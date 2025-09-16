#include "snapshot_manager.h"

#include <gen_cpp/cloud.pb.h>

using doris::cloud::MetaServiceCode;

namespace selectdb {

void SnapshotManager::begin_snapshot(std::string_view instance_id,
                                     const doris::cloud::BeginSnapshotRequest& request,
                                     doris::cloud::BeginSnapshotResponse* response) {
    response->mutable_status()->set_code(MetaServiceCode::UNDEFINED_ERR);
    response->mutable_status()->set_msg("Not implemented");
}

void SnapshotManager::commit_snapshot(std::string_view instance_id,
                                      const doris::cloud::CommitSnapshotRequest& request,
                                      doris::cloud::CommitSnapshotResponse* response) {
    response->mutable_status()->set_code(MetaServiceCode::UNDEFINED_ERR);
    response->mutable_status()->set_msg("Not implemented");
}

void SnapshotManager::abort_snapshot(std::string_view instance_id,
                                     const doris::cloud::AbortSnapshotRequest& request,
                                     doris::cloud::AbortSnapshotResponse* response) {
    response->mutable_status()->set_code(MetaServiceCode::UNDEFINED_ERR);
    response->mutable_status()->set_msg("Not implemented");
}

void SnapshotManager::drop_snapshot(std::string_view instance_id,
                                    const doris::cloud::DropSnapshotRequest& request,
                                    doris::cloud::DropSnapshotResponse* response) {
    response->mutable_status()->set_code(MetaServiceCode::UNDEFINED_ERR);
    response->mutable_status()->set_msg("Not implemented");
}

void SnapshotManager::list_snapshot(std::string_view instance_id,
                                    const doris::cloud::ListSnapshotRequest& request,
                                    doris::cloud::ListSnapshotResponse* response) {
    response->mutable_status()->set_code(MetaServiceCode::UNDEFINED_ERR);
    response->mutable_status()->set_msg("Not implemented");
}

void SnapshotManager::clone_instance(std::string_view instance_id,
                                     const doris::cloud::CloneInstanceRequest& request,
                                     doris::cloud::CloneInstanceResponse* response) {
    response->mutable_status()->set_code(MetaServiceCode::UNDEFINED_ERR);
    response->mutable_status()->set_msg("Not implemented");
}

// Recycle snapshots that are expired or marked as recycled, based on the retention policy.
// Return 0 for success otherwise error.
int SnapshotManager::recycle_snapshots(doris::cloud::InstanceRecycler* recycler) {
    return 0; // Not implemented
}

// Recycle snapshot meta and data, return 0 for success otherwise error.
int SnapshotManager::recycle_snapshot_meta_and_data(doris::cloud::StorageVaultAccessor* accessor,
                                                    doris::cloud::Versionstamp* snapshot_version,
                                                    const doris::cloud::SnapshotPB* snapshot_pb) {
    return 0;
}

} // namespace selectdb
