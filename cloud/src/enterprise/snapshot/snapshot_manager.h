#pragma once

#include <gen_cpp/cloud.pb.h>

#include <mutex>

#include "meta-store/txn_kv.h"
#include "recycler/recycler.h"
#include "recycler/storage_vault_accessor.h"
#include "snapshot/snapshot_manager.h"

namespace selectdb {

class SnapshotManager : public doris::cloud::SnapshotManager {
    using TxnKv = doris::cloud::TxnKv;

public:
    SnapshotManager(std::shared_ptr<TxnKv> txn_kv);
    ~SnapshotManager() override = default;

    void begin_snapshot(std::string_view instance_id,
                        const doris::cloud::BeginSnapshotRequest& request,
                        doris::cloud::BeginSnapshotResponse* response) override;
    void update_snapshot(std::string_view instance_id,
                         const doris::cloud::UpdateSnapshotRequest& request,
                         doris::cloud::UpdateSnapshotResponse* response) override;
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
            std::string_view instance_id,
            doris::cloud::MultiVersionStatus multi_version_status) override;

    // Recycle snapshots that are expired or marked as recycled, based on the retention policy.
    // Return 0 for success otherwise error.
    int recycle_snapshots(doris::cloud::InstanceRecycler* recycler) override;

    int check_snapshots(doris::cloud::InstanceChecker* checker) override;

    int inverted_check_snapshots(doris::cloud::InstanceChecker* checker) override;

    int check_mvcc_meta_key(doris::cloud::InstanceChecker* checker) override;

    int inverted_check_mvcc_meta_key(doris::cloud::InstanceChecker* checker) override;

    int check_meta(doris::cloud::MetaChecker* meta_checker) override;

    // Recycle snapshot meta and data, return 0 for success otherwise error.
    int recycle_snapshot_meta_and_data(std::string_view instance_id, std::string_view resource_id,
                                       doris::cloud::StorageVaultAccessor* accessor,
                                       doris::cloud::Versionstamp snapshot_version,
                                       const doris::cloud::SnapshotPB& snapshot_pb) override;

    int migrate_to_versioned_keys(doris::cloud::InstanceDataMigrator* migrator) override;

    int compact_snapshot_chains(doris::cloud::InstanceChainCompactor* compactor) override;

private:
    void start_pools();

    // Validation functions
    doris::cloud::MetaServiceCode validate_clone_request(
            const doris::cloud::CloneInstanceRequest& request, std::string* error_msg);

    doris::cloud::MetaServiceCode validate_writable_clone_request(
            const doris::cloud::CloneInstanceRequest& request, std::string* error_msg);

    void validate_source_snapshot(doris::cloud::Transaction* txn,
                                  const std::string& from_instance_id,
                                  const std::string& from_snapshot_id,
                                  doris::cloud::SnapshotPB* snapshot_pb,
                                  doris::cloud::MetaServiceCode& code, std::string& error_msg);

    doris::cloud::TxnErrorCode validate_source_instance(
            doris::cloud::Transaction* txn, const std::string& from_instance_id,
            const std::string& from_snapshot_id,
            doris::cloud::CloneInstanceRequest::CloneType clone_type,
            doris::cloud::InstanceInfoPB* from_instance_info, std::string* error_msg);

    // Clone type handlers
    doris::cloud::MetaServiceCode handle_readonly_clone(
            doris::cloud::Transaction* txn, const doris::cloud::CloneInstanceRequest& request,
            const doris::cloud::SnapshotPB& snapshot_pb,
            const doris::cloud::InstanceInfoPB& from_instance_info,
            doris::cloud::CloneInstanceResponse* response, std::string* error_msg);

    doris::cloud::MetaServiceCode handle_writable_clone(
            doris::cloud::Transaction* txn, const doris::cloud::CloneInstanceRequest& request,
            const doris::cloud::SnapshotPB& snapshot_pb,
            const doris::cloud::InstanceInfoPB& from_instance_info,
            doris::cloud::CloneInstanceResponse* response, std::string* error_msg);

    doris::cloud::MetaServiceCode handle_rollback_clone(
            doris::cloud::Transaction* txn, const doris::cloud::CloneInstanceRequest& request,
            const doris::cloud::SnapshotPB& snapshot_pb,
            const doris::cloud::InstanceInfoPB& from_instance_info,
            doris::cloud::CloneInstanceResponse* response, std::string* error_msg);

    // Helper functions
    doris::cloud::TxnErrorCode check_target_instance_existence(
            doris::cloud::Transaction* txn, const std::string& new_instance_id,
            const std::string& from_instance_id, const std::string& from_snapshot_id,
            bool is_readonly, bool* already_exists, doris::cloud::CloneInstanceResponse* response,
            const doris::cloud::SnapshotPB& snapshot_pb,
            const doris::cloud::InstanceInfoPB& from_instance_info, std::string* error_msg);

    doris::cloud::InstanceInfoPB create_readonly_instance_info(
            const std::string& new_instance_id,
            const doris::cloud::InstanceInfoPB& from_instance_info,
            const std::string& from_instance_id, const std::string& from_snapshot_id);

    doris::cloud::InstanceInfoPB create_writable_instance_info(
            const std::string& new_instance_id,
            const doris::cloud::InstanceInfoPB& from_instance_info,
            const std::string& from_instance_id, const std::string& from_snapshot_id);

    doris::cloud::MetaServiceCode setup_writable_storage(
            doris::cloud::Transaction* txn, const doris::cloud::CloneInstanceRequest& request,
            const doris::cloud::InstanceInfoPB& from_instance_info,
            doris::cloud::InstanceInfoPB* new_instance, std::string* error_msg);

    doris::cloud::MetaServiceCode clone_storage_vault_entries(
            doris::cloud::Transaction* txn, const std::string& from_instance_id,
            const std::string& new_instance_id,
            const doris::cloud::InstanceInfoPB& from_instance_info, std::string* error_msg);

    void establish_snapshot_reference(doris::cloud::Transaction* txn,
                                      const std::string& from_instance_id,
                                      const doris::cloud::Versionstamp& snapshot_versionstamp,
                                      const std::string& new_instance_id);

    doris::cloud::MetaServiceCode update_source_instance_successor(
            doris::cloud::Transaction* txn, const std::string& from_instance_key,
            doris::cloud::InstanceInfoPB* from_instance_info, const std::string& new_instance_id,
            std::string* error_msg);

    // Thread pools for snapshot operations (shared across all instances)
    std::shared_ptr<doris::cloud::SimpleThreadPool> compact_pool_;
    std::shared_ptr<doris::cloud::SimpleThreadPool> migrate_pool_;
    std::once_flag pools_start_flag_;
};

} // namespace selectdb
