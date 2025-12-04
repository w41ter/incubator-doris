#include "snapshot_manager.h"

#include <gen_cpp/cloud.pb.h>

#include <charconv>
#include <chrono>
#include <limits>
#include <numeric>
#include <string_view>

#include "common/encryption_util.h"
#include "common/util.h"
#include "meta-service/meta_service_helper.h"
#include "meta-store/keys.h"
#include "meta-store/meta_reader.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
#include "snapshot_helper.h"

using namespace doris::cloud;
namespace versioned = doris::cloud::versioned;

namespace selectdb {

static constexpr std::string_view SNAPSHOT_PREFIX = "snapshot";

static inline std::string get_snapshot_url(std::string_view snapshot_id) {
    return fmt::format("{}/{}/", SNAPSHOT_PREFIX, snapshot_id);
}

static bool decrypt_object_store_info_ak_sk(ObjectStoreInfoPB* obj_info) {
    if (!obj_info->has_encryption_info()) {
        return true;
    }

    auto& ak = obj_info->ak();
    auto& sk = obj_info->sk();
    auto& encryption_info = obj_info->encryption_info();
    AkSkPair plain_ak_sk_pair;
    if (int ret = decrypt_ak_sk_helper(ak, sk, encryption_info, &plain_ak_sk_pair); ret != 0) {
        LOG(WARNING) << "failed to decrypt object store info ak/sk, err=" << ret;
        return false;
    }

    obj_info->clear_encryption_info(); // avoid leaking encryption info
    obj_info->set_ak(std::move(plain_ak_sk_pair.first));
    obj_info->set_sk(std::move(plain_ak_sk_pair.second));
    return true;
}

// Set snapshot info in response
static MetaServiceCode set_snapshot_info_in_response(CloneInstanceResponse* response,
                                                     const SnapshotPB& snapshot_pb,
                                                     const InstanceInfoPB& from_instance_info,
                                                     Transaction* txn, std::string* error_msg) {
    // Set snapshot image URL
    response->set_image_url(snapshot_pb.image_url());

    std::string snapshot_resource_id = snapshot_pb.resource_id();
    ObjectStoreInfoPB* obj_info = response->mutable_obj_info();

    // Find corresponding snapshot storage info from the from-instance storage configuration
    bool found_snapshot_storage = false;
    if (!from_instance_info.enable_storage_vault()) {
        for (const auto& source_obj_info : from_instance_info.obj_info()) {
            if (source_obj_info.id() == snapshot_resource_id) {
                *obj_info = source_obj_info;
                if (!decrypt_object_store_info_ak_sk(obj_info)) {
                    LOG_WARNING("failed to decrypt snapshot object store info ak/sk");
                    if (error_msg != nullptr) {
                        *error_msg = "failed to decrypt snapshot object store info";
                    }
                    return MetaServiceCode::UNDEFINED_ERR;
                }
                found_snapshot_storage = true;
                return MetaServiceCode::OK;
            }
        }
    } else {
        for (const auto& resource_id : from_instance_info.resource_ids()) {
            if (resource_id == snapshot_resource_id) {
                auto vault_key = storage_vault_key({from_instance_info.instance_id(), resource_id});
                std::string val;
                auto err = txn->get(vault_key, &val);

                if (err != TxnErrorCode::TXN_OK) {
                    LOG_WARNING("Failed to get storage vault with key=")
                            .tag("vault_key", hex(vault_key))
                            .tag("error", err);
                    if (error_msg != nullptr) {
                        *error_msg =
                                fmt::format("failed to get storage vault, resource_id={} err={}",
                                            snapshot_resource_id, err);
                    }
                    return cast_as<ErrCategory::READ>(err);
                }

                StorageVaultPB storage_vault;
                if (!storage_vault.ParseFromString(val)) {
                    LOG_WARNING("Failed to parse StorageVaultPB from string");
                    if (error_msg != nullptr) {
                        *error_msg = "failed to parse StorageVaultPB";
                    }
                    return MetaServiceCode::PROTOBUF_PARSE_ERR;
                }

                if (!storage_vault.has_obj_info()) {
                    if (error_msg != nullptr) {
                        *error_msg = "storage vault missing object store info";
                    }
                    return MetaServiceCode::UNDEFINED_ERR;
                }

                *obj_info = storage_vault.obj_info();
                if (!decrypt_object_store_info_ak_sk(obj_info)) {
                    LOG_WARNING("failed to decrypt snapshot object store info ak/sk");
                    if (error_msg != nullptr) {
                        *error_msg = "failed to decrypt snapshot object store info";
                    }
                    return MetaServiceCode::UNDEFINED_ERR;
                }
                found_snapshot_storage = true;
                return MetaServiceCode::OK;
            }
        }
    }

    if (!found_snapshot_storage) {
        LOG_WARNING("snapshot storage info not found").tag("resource_id", snapshot_resource_id);
        if (error_msg != nullptr) {
            *error_msg = fmt::format("snapshot storage info not found for resource_id={}",
                                     snapshot_resource_id);
        }
        return MetaServiceCode::UNDEFINED_ERR;
    }

    return MetaServiceCode::OK;
}

// Find the next available resource ID from an instance's resource_ids
static std::string next_available_resource_id(const InstanceInfoPB& instance) {
    auto parse_id = [](int prev, std::string_view value) {
        int last_id = 0;
        if (auto [_, ec] = std::from_chars(value.data(), value.data() + value.size(), last_id);
            ec != std::errc {}) {
            LOG_WARNING("Invalid resource id format: {}", value);
            last_id = 0;
            DCHECK(false);
        }
        return std::max(prev, last_id);
    };

    auto object_info_max = std::accumulate(
            instance.obj_info().begin(), instance.obj_info().end(), 0,
            [&](int prev, const ObjectStoreInfoPB& obj) { return parse_id(prev, obj.id()); });

    auto total_max = std::accumulate(
            instance.resource_ids().begin(), instance.resource_ids().end(), object_info_max,
            [&](int prev, const std::string& id) { return parse_id(prev, id); });

    return std::to_string(total_max + 1);
}

void SnapshotManager::begin_snapshot(std::string_view instance_id,
                                     const BeginSnapshotRequest& request,
                                     BeginSnapshotResponse* response) {
    auto* status = response->mutable_status();
    status->set_code(MetaServiceCode::OK);
    status->set_msg("OK");

    // Validate timeout must be positive
    if (!request.has_timeout_seconds() || request.timeout_seconds() <= 0) {
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg("timeout_seconds must be positive");
        return;
    }

    bool auto_snapshot = request.auto_snapshot();
    // Validate TTL must be positive
    if (!auto_snapshot && (!request.has_ttl_seconds() || request.ttl_seconds() <= 0)) {
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg("ttl_seconds must be positive");
        return;
    }

    // Validate snapshot label is not empty
    if (!auto_snapshot && (!request.has_snapshot_label() || request.snapshot_label().empty())) {
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg("snapshot_label cannot be empty");
        return;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        status->set_code(cast_as<ErrCategory::CREATE>(err));
        status->set_msg("failed to create txn");
        LOG(WARNING) << status->msg() << " err=" << err;
        return;
    }

    // get instance pb to check if it exists and get source snapshot info
    std::string key = instance_key({instance_id});
    std::string val;
    err = txn->get(key, &val);
    if (err != TxnErrorCode::TXN_OK) {
        status->set_code(cast_as<ErrCategory::READ>(err));
        status->set_msg(fmt::format("failed to get instance, instance_id={}", instance_id));
        LOG(WARNING) << status->msg() << " err=" << err;
        return;
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        status->set_code(MetaServiceCode::PROTOBUF_PARSE_ERR);
        status->set_msg("failed to parse InstanceInfoPB");
        return;
    }

    if (instance.snapshot_switch_status() != SnapshotSwitchStatus::SNAPSHOT_SWITCH_ON) {
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg(
                "failed to begin snapshot, because the snapshot feature is disabled. Please enable "
                "snapshot feature by executeing: `ADMIN SET CLUSTER SNAPSHOT FEATURE ON`");
        return;
    }

    // Check if auto snapshot is disabled when request is for auto snapshot
    if (auto_snapshot && instance.has_max_reserved_snapshot() &&
        instance.max_reserved_snapshot() == 0) {
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg(
                "failed to begin auto snapshot, because auto snapshot is disabled "
                "(max_reserved_snapshots is 0)");
        return;
    }

    ObjectStoreInfoPB obj_info;
    std::string snapshot_resource_id;
    if (!instance.enable_storage_vault()) {
        if (instance.obj_info_size() == 0) {
            status->set_code(MetaServiceCode::UNDEFINED_ERR);
            status->set_msg("instance has no object store configuration");
            return;
        }
        // Choose the last store obj as the storage to save the snapshot images.
        obj_info = instance.obj_info(instance.obj_info_size() - 1);
        snapshot_resource_id = obj_info.id();
    } else {
        if (instance.has_default_storage_vault_id() &&
            !instance.default_storage_vault_id().empty()) {
            snapshot_resource_id = instance.default_storage_vault_id();
        } else if (instance.resource_ids_size() > 0) {
            snapshot_resource_id = instance.resource_ids(0);
        }

        if (snapshot_resource_id.empty()) {
            status->set_code(MetaServiceCode::UNDEFINED_ERR);
            status->set_msg("storage vault instance missing resource id");
            return;
        }

        std::string vault_key;
        storage_vault_key({std::string(instance_id), snapshot_resource_id}, &vault_key);
        std::string vault_val;
        err = txn->get(vault_key, &vault_val);
        if (err != TxnErrorCode::TXN_OK) {
            status->set_code(cast_as<ErrCategory::READ>(err));
            status->set_msg(fmt::format("failed to get storage vault, resource_id={} err={}",
                                        snapshot_resource_id, err));
            LOG(WARNING) << status->msg();
            return;
        }

        StorageVaultPB storage_vault;
        if (!storage_vault.ParseFromString(vault_val)) {
            status->set_code(MetaServiceCode::PROTOBUF_PARSE_ERR);
            status->set_msg("failed to parse StorageVaultPB");
            return;
        }

        if (!storage_vault.has_obj_info()) {
            status->set_code(MetaServiceCode::UNDEFINED_ERR);
            status->set_msg("storage vault missing object store info");
            return;
        }

        obj_info = storage_vault.obj_info();
        if (!obj_info.has_id() || obj_info.id().empty()) {
            obj_info.set_id(snapshot_resource_id);
        }
    }

    if (!decrypt_object_store_info_ak_sk(&obj_info)) {
        status->set_code(MetaServiceCode::UNDEFINED_ERR);
        status->set_msg("failed to decrypt object info ak/sk");
        return;
    }

    // construct snapshot pb
    SnapshotPB snapshot_pb;
    snapshot_pb.set_status(SnapshotStatus::SNAPSHOT_PREPARE);
    snapshot_pb.set_type(SnapshotType::SNAPSHOT_REFERENCE);
    snapshot_pb.set_timeout_seconds(request.timeout_seconds());
    snapshot_pb.set_instance_id(std::string(instance_id));
    if (instance.has_source_snapshot_id()) {
        snapshot_pb.set_snapshot_ancestor(instance.source_snapshot_id());
    }
    snapshot_pb.set_auto_(request.auto_snapshot());
    snapshot_pb.set_ttl_seconds(request.ttl_seconds());
    snapshot_pb.set_label(request.snapshot_label());
    snapshot_pb.set_create_at(std::time(nullptr));

    // Save the resource_id of the chosen object store.
    //
    // This create a reference to the object store, so any update or deletion of the object store
    // must be blocked until all snapshots referencing it are deleted.
    if (snapshot_resource_id.empty()) {
        snapshot_resource_id = obj_info.id();
    }
    snapshot_pb.set_resource_id(snapshot_resource_id);

    std::string snapshot_full_info_val;
    if (!snapshot_pb.SerializeToString(&snapshot_full_info_val)) {
        status->set_msg("failed to serialize SnapshotPB");
        status->set_code(MetaServiceCode::PROTOBUF_SERIALIZE_ERR);
        return;
    }

    std::string snapshot_full_key = versioned::snapshot_full_key({instance_id});
    versioned_put(txn.get(), snapshot_full_key, snapshot_full_info_val);
    LOG_INFO("put versioned snapshot full key info")
            .tag("snapshot_full_key", hex(snapshot_full_key))
            .tag("instance_id", instance_id);

    txn->enable_get_versionstamp();
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        status->set_code(cast_as<ErrCategory::COMMIT>(err));
        status->set_msg(fmt::format("failed to commit kv txn, err={}", err));
        LOG(WARNING) << status->msg();
        return;
    }

    // get versionstamp
    Versionstamp versionstamp;
    err = txn->get_versionstamp(&versionstamp);
    if (err != TxnErrorCode::TXN_OK) {
        status->set_code(cast_as<ErrCategory::COMMIT>(err));
        status->set_msg(fmt::format("failed to get versionstamp, err={}", err));
        LOG(WARNING) << status->msg();
        return;
    }

    std::string snapshot_id = serialize_snapshot_id(versionstamp);
    std::string image_url = get_snapshot_url(snapshot_id);
    response->set_image_url(image_url);
    response->set_snapshot_id(snapshot_id);
    response->mutable_obj_info()->Swap(&obj_info);

    LOG_INFO("begin snapshot completed")
            .tag("instance_id", instance_id)
            .tag("snapshot_id", hex(snapshot_id))
            .tag("image_url", image_url)
            .tag("resource_id", snapshot_resource_id);
}

void SnapshotManager::update_snapshot(std::string_view instance_id,
                                      const UpdateSnapshotRequest& request,
                                      UpdateSnapshotResponse* response) {
    auto* status = response->mutable_status();
    status->set_code(MetaServiceCode::OK);
    status->set_msg("OK");

    if (request.upload_file().empty()) {
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg("upload_file is empty");
        return;
    }

    if (request.upload_id().empty()) {
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg("upload_id is empty");
        return;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        status->set_code(cast_as<ErrCategory::CREATE>(err));
        status->set_msg("failed to create txn");
        LOG(WARNING) << status->msg() << " err=" << err;
        return;
    }

    std::string snapshot_id = request.snapshot_id();
    Versionstamp snapshot_versionstamp;
    if (!parse_snapshot_versionstamp(snapshot_id, &snapshot_versionstamp)) {
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg("invalid snapshot_id format");
        return;
    }

    // Construct key using instance_id and use snapshot_id as versionstamp
    std::string snapshot_full_key = versioned::snapshot_full_key({instance_id});
    std::string snapshot_key = encode_versioned_key(snapshot_full_key, snapshot_versionstamp);
    std::string snapshot_val;
    err = txn->get(snapshot_key, &snapshot_val);
    LOG(INFO) << "get versioned snapshot key=" << hex(snapshot_full_key)
              << " version=" << hex(snapshot_id);

    if (err != TxnErrorCode::TXN_OK) {
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            status->set_code(MetaServiceCode::TXN_ID_NOT_FOUND);
            status->set_msg("snapshot not found, snapshot_id=" + snapshot_id);
        } else {
            status->set_code(cast_as<ErrCategory::READ>(err));
            status->set_msg("failed to get snapshot, snapshot_id=" + snapshot_id);
        }
        LOG(WARNING) << status->msg() << " err=" << err;
        return;
    }

    SnapshotPB snapshot_pb;
    if (!snapshot_pb.ParseFromString(snapshot_val)) {
        status->set_code(MetaServiceCode::PROTOBUF_PARSE_ERR);
        status->set_msg("failed to parse SnapshotPB");
        return;
    }

    // Check snapshot status
    if (snapshot_pb.status() != SnapshotStatus::SNAPSHOT_PREPARE) {
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg("snapshot status is " + SnapshotStatus_Name(snapshot_pb.status()) +
                        ", cannot update upload_id");
        return;
    }

    snapshot_pb.set_upload_file(request.upload_file());
    snapshot_pb.set_upload_id(request.upload_id());

    std::string updated_snapshot_val;
    if (!snapshot_pb.SerializeToString(&updated_snapshot_val)) {
        status->set_msg("failed to serialize updated SnapshotPB");
        status->set_code(MetaServiceCode::PROTOBUF_SERIALIZE_ERR);
        return;
    }

    // Save updated snapshot
    txn->put(snapshot_key, updated_snapshot_val);
    LOG_INFO("update snapshot upload path and id completed")
            .tag("snapshot_key", hex(snapshot_full_key))
            .tag("instance_id", instance_id)
            .tag("snapshot_id", hex(snapshot_id))
            .tag("update_file", hex(request.upload_file()))
            .tag("update_id", hex(request.upload_id()));

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        status->set_code(cast_as<ErrCategory::COMMIT>(err));
        status->set_msg(fmt::format("failed to commit kv txn, err={}", err));
        LOG(WARNING) << status->msg();
    }
}

void SnapshotManager::commit_snapshot(std::string_view instance_id,
                                      const CommitSnapshotRequest& request,
                                      CommitSnapshotResponse* response) {
    auto* status = response->mutable_status();
    status->set_code(MetaServiceCode::OK);
    status->set_msg("OK");

    // Check all required fields in CommitSnapshotRequest
    if (!request.has_snapshot_id() || request.snapshot_id().empty()) {
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg("snapshot_id not set");
        return;
    }

    if (!request.has_image_url() || request.image_url().empty()) {
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg("image_url not set");
        return;
    }

    if (!request.has_last_journal_id()) {
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg("last_journal_id not set");
        return;
    }

    std::string snapshot_id = request.snapshot_id();
    std::string image_url = request.image_url();
    int64_t last_journal_id = request.last_journal_id();

    Versionstamp snapshot_versionstamp;
    if (!parse_snapshot_versionstamp(snapshot_id, &snapshot_versionstamp)) {
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg("invalid snapshot_id format");
        return;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        status->set_code(cast_as<ErrCategory::CREATE>(err));
        status->set_msg("failed to create txn");
        LOG(WARNING) << status->msg() << " err=" << err;
        return;
    }

    // Construct key using instance_id and use snapshot_id as versionstamp
    std::string snapshot_full_key = versioned::snapshot_full_key({instance_id});
    std::string snapshot_key = encode_versioned_key(snapshot_full_key, snapshot_versionstamp);
    std::string snapshot_val;
    err = txn->get(snapshot_key, &snapshot_val);
    LOG(INFO) << "get versioned snapshot key=" << hex(snapshot_full_key)
              << " version=" << hex(snapshot_id);

    if (err != TxnErrorCode::TXN_OK) {
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            status->set_code(MetaServiceCode::TXN_ID_NOT_FOUND);
            status->set_msg("snapshot not found, snapshot_id=" + snapshot_id);
        } else {
            status->set_code(cast_as<ErrCategory::READ>(err));
            status->set_msg("failed to get snapshot, snapshot_id=" + snapshot_id);
        }
        LOG(WARNING) << status->msg() << " err=" << err;
        return;
    }

    SnapshotPB snapshot_pb;
    if (!snapshot_pb.ParseFromString(snapshot_val)) {
        status->set_code(MetaServiceCode::PROTOBUF_PARSE_ERR);
        status->set_msg("failed to parse SnapshotPB");
        return;
    }

    // Check if snapshot is already committed (for idempotency)
    if (snapshot_pb.status() == SnapshotStatus::SNAPSHOT_NORMAL) {
        // Already committed, return success to handle RPC retry
        LOG(INFO) << "snapshot already committed, snapshot_id=" << snapshot_id;
        return;
    }

    // Check if snapshot is in valid state for commit
    if (snapshot_pb.status() == SnapshotStatus::SNAPSHOT_ABORTED) {
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg("cannot commit aborted snapshot");
        return;
    }

    DCHECK(snapshot_pb.status() == SnapshotStatus::SNAPSHOT_PREPARE)
            << "invalid snapshot status: " << snapshot_pb.status();

    // Set image_url, journal_id, status and finish_at
    snapshot_pb.set_image_url(image_url);
    snapshot_pb.set_last_journal_id(last_journal_id);
    snapshot_pb.set_status(SnapshotStatus::SNAPSHOT_NORMAL);
    snapshot_pb.set_finish_at(std::time(nullptr));
    // the image is already uploaded, so clear upload_file and upload_id
    snapshot_pb.set_upload_file("");
    snapshot_pb.set_upload_id("");

    std::string updated_snapshot_val;
    if (!snapshot_pb.SerializeToString(&updated_snapshot_val)) {
        status->set_msg("failed to serialize updated SnapshotPB");
        status->set_code(MetaServiceCode::PROTOBUF_SERIALIZE_ERR);
        return;
    }

    // Save updated snapshot
    txn->put(snapshot_key, updated_snapshot_val);
    LOG_INFO("commit snapshot completed")
            .tag("snapshot_key", hex(snapshot_full_key))
            .tag("instance_id", instance_id)
            .tag("snapshot_id", hex(snapshot_id))
            .tag("image_url", image_url)
            .tag("last_journal_id", last_journal_id);

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        status->set_code(cast_as<ErrCategory::COMMIT>(err));
        status->set_msg(fmt::format("failed to commit kv txn, err={}", err));
        LOG(WARNING) << status->msg();
    }
}

void SnapshotManager::abort_snapshot(std::string_view instance_id,
                                     const AbortSnapshotRequest& request,
                                     AbortSnapshotResponse* response) {
    auto* status = response->mutable_status();
    status->set_code(MetaServiceCode::OK);
    status->set_msg("OK");

    // Check all required fields in AbortSnapshotRequest
    if (!request.has_snapshot_id() || request.snapshot_id().empty()) {
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg("snapshot_id not set");
        return;
    }

    std::string snapshot_id = request.snapshot_id();
    std::string reason = request.has_reason() ? request.reason() : "Aborted by user";

    // Convert snapshot_id to versionstamp for later use in versioned_put
    Versionstamp snapshot_versionstamp;
    if (!parse_snapshot_versionstamp(snapshot_id, &snapshot_versionstamp)) {
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg("invalid snapshot_id format");
        return;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        status->set_code(cast_as<ErrCategory::CREATE>(err));
        status->set_msg("failed to create txn");
        LOG(WARNING) << status->msg() << " err=" << err;
        return;
    }

    // Construct key using instance_id and use snapshot_id as versionstamp
    std::string snapshot_full_key = versioned::snapshot_full_key({instance_id});
    std::string snapshot_key = encode_versioned_key(snapshot_full_key, snapshot_versionstamp);
    std::string snapshot_val;
    err = txn->get(snapshot_key, &snapshot_val);
    LOG(INFO) << "get versioned snapshot key=" << hex(snapshot_full_key)
              << " version=" << hex(snapshot_id);

    if (err != TxnErrorCode::TXN_OK) {
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            status->set_code(MetaServiceCode::TXN_ID_NOT_FOUND);
            status->set_msg("snapshot not found, snapshot_id=" + snapshot_id);
        } else {
            status->set_code(cast_as<ErrCategory::READ>(err));
            status->set_msg("failed to get snapshot, snapshot_id=" + snapshot_id);
        }
        LOG(WARNING) << status->msg() << " err=" << err;
        return;
    }

    SnapshotPB snapshot_pb;
    if (!snapshot_pb.ParseFromString(snapshot_val)) {
        status->set_code(MetaServiceCode::PROTOBUF_PARSE_ERR);
        status->set_msg("failed to parse SnapshotPB");
        return;
    }

    // Check if snapshot is already in final state
    if (snapshot_pb.status() == SnapshotStatus::SNAPSHOT_NORMAL) {
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg("cannot abort snapshot that is already committed");
        return;
    }

    if (snapshot_pb.status() == SnapshotStatus::SNAPSHOT_ABORTED) {
        // Already aborted, return success to handle RPC retry
        status->set_msg("snapshot is already aborted");
        LOG(INFO) << "snapshot is already aborted, snapshot_id=" << snapshot_id;
        return;
    }

    DCHECK(snapshot_pb.status() == SnapshotStatus::SNAPSHOT_PREPARE)
            << "invalid snapshot status: " << snapshot_pb.status();

    // Set status to aborted, reason and finish_at
    snapshot_pb.set_status(SnapshotStatus::SNAPSHOT_ABORTED);
    snapshot_pb.set_reason(reason);
    snapshot_pb.set_finish_at(std::time(nullptr));

    std::string updated_snapshot_val;
    if (!snapshot_pb.SerializeToString(&updated_snapshot_val)) {
        status->set_msg("failed to serialize updated SnapshotPB");
        status->set_code(MetaServiceCode::PROTOBUF_SERIALIZE_ERR);
        return;
    }

    // Save updated snapshot
    txn->put(snapshot_key, updated_snapshot_val);
    LOG_INFO("abort snapshot completed")
            .tag("snapshot_key", hex(snapshot_full_key))
            .tag("instance_id", instance_id)
            .tag("snapshot_id", hex(snapshot_id))
            .tag("reason", reason);

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        status->set_code(cast_as<ErrCategory::COMMIT>(err));
        status->set_msg(fmt::format("failed to commit kv txn, err={}", err));
        LOG(WARNING) << status->msg();
    }
}

void get_instance(Transaction* txn, const std::string_view& instance_id,
                  InstanceInfoPB& instance_info, MetaServiceCode& code, std::string& error_msg) {
    InstanceKeyInfo instance_key_info {instance_id};
    std::string key = instance_key(instance_key_info);
    std::string val;
    TxnErrorCode err = txn->get(key, &val);
    if (err != TxnErrorCode::TXN_OK) {
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            code = MetaServiceCode::INVALID_ARGUMENT;
        } else {
            code = cast_as<ErrCategory::READ>(err);
        }
        error_msg = fmt::format("failed to get instance, instance_id={}, err={}", instance_id, err);
        return;
    }

    if (!instance_info.ParseFromString(val)) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        error_msg = "failed to parse instance info";
        return;
    }
}

// Get all snapshots of the specific instance.
// If the instance is created by rollback, also get the snapshots of all its predecessor instances.
// This method is used by list_snapshot, drop_snapshot, clone_instance.
void get_all_snapshots(Transaction* txn, const std::string_view& instance_id,
                       const std::string& required_snapshot_id,
                       std::vector<std::pair<SnapshotPB, Versionstamp>>* snapshots,
                       MetaServiceCode& code, std::string& error_msg) {
    Versionstamp required_snapshot_versionstamp;
    if (!required_snapshot_id.empty()) {
        if (!parse_snapshot_versionstamp(required_snapshot_id, &required_snapshot_versionstamp)) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            error_msg = "invalid snapshot_id format";
            return;
        }
    }

    InstanceInfoPB instance_info;
    get_instance(txn, instance_id, instance_info, code, error_msg);
    if (code != MetaServiceCode::OK) {
        return;
    }
    std::string_view current_instance_id = instance_id;
    if (instance_info.has_original_instance_id() && !instance_info.original_instance_id().empty()) {
        // the earliest instance_id for rollback
        current_instance_id = instance_info.original_instance_id();
    }

    do {
        MetaReader meta_reader(current_instance_id);
        if (required_snapshot_id.empty()) {
            TxnErrorCode err = meta_reader.get_snapshots(txn, snapshots);
            if (err != TxnErrorCode::TXN_OK) {
                code = cast_as<ErrCategory::READ>(err);
                error_msg = "failed to get snapshots";
                return;
            }
        } else {
            SnapshotPB snapshot_pb;
            TxnErrorCode err =
                    meta_reader.get_snapshot(txn, required_snapshot_versionstamp, &snapshot_pb);
            if (err == TxnErrorCode::TXN_OK) {
                snapshots->emplace_back(snapshot_pb, required_snapshot_versionstamp);
                return;
            } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
                code = cast_as<ErrCategory::READ>(err);
                error_msg = "failed to get snapshot";
                return;
            }
        }
        if (current_instance_id == instance_id) {
            break;
        }
        get_instance(txn, current_instance_id, instance_info, code, error_msg);
        if (code != MetaServiceCode::OK) {
            return;
        }
        if (!instance_info.has_successor_instance_id() ||
            instance_info.successor_instance_id().empty()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            error_msg = fmt::format(
                    "successor_instance_id is empty for current instance_id={}, instance_id={}",
                    current_instance_id, instance_id);
            LOG_WARNING(error_msg);
            return;
        }
        current_instance_id = instance_info.successor_instance_id();
    } while (true);
}

void SnapshotManager::drop_snapshot(std::string_view instance_id,
                                    const DropSnapshotRequest& request,
                                    DropSnapshotResponse* response) {
    auto* status = response->mutable_status();
    status->set_code(MetaServiceCode::OK);
    status->set_msg("OK");

    if (!request.has_snapshot_id() || request.snapshot_id().empty()) {
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg("snapshot_id not set");
        return;
    }

    std::string snapshot_id = request.snapshot_id();

    Versionstamp snapshot_versionstamp;
    if (!parse_snapshot_versionstamp(snapshot_id, &snapshot_versionstamp)) {
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg("invalid snapshot_id format");
        return;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        status->set_code(cast_as<ErrCategory::CREATE>(err));
        status->set_msg("failed to create txn");
        LOG(WARNING) << status->msg() << " err=" << err;
        return;
    }

    // Get snapshot
    std::vector<std::pair<SnapshotPB, Versionstamp>> snapshots;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string error_msg;
    get_all_snapshots(txn.get(), instance_id, snapshot_id, &snapshots, code, error_msg);
    if (code != MetaServiceCode::OK) {
        status->set_code(code);
        status->set_msg(error_msg);
        LOG(WARNING) << status->msg() << " err=" << err;
        return;
    }
    if (snapshots.empty()) {
        status->set_code(MetaServiceCode::TXN_ID_NOT_FOUND);
        status->set_msg("snapshot not found, snapshot_id=" + snapshot_id);
        return;
    }
    if (snapshots.size() > 1) {
        // it is impossible.
        LOG_WARNING("more than one snapshot")
                .tag("instance_id", instance_id)
                .tag("snapshot_id", snapshot_id)
                .tag("snapshot_num", snapshots.size());
    }
    SnapshotPB& snapshot_pb = snapshots.front().first;

    if (snapshot_pb.status() == SnapshotStatus::SNAPSHOT_RECYCLED) {
        // Already dropped, return success to handle RPC retry
        status->set_code(MetaServiceCode::TXN_ID_NOT_FOUND);
        status->set_msg("snapshot not found, snapshot_id=" + snapshot_id);
        return;
    }

    // Check if snapshot can be dropped (should be in final state)
    if (snapshot_pb.status() != SnapshotStatus::SNAPSHOT_NORMAL &&
        snapshot_pb.status() != SnapshotStatus::SNAPSHOT_ABORTED) {
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg("cannot drop snapshot that is not in final state (NORMAL or ABORTED)");
        return;
    }

    bool has_references = false;
    MetaReader meta_reader(snapshot_pb.instance_id());
    err = meta_reader.has_snapshot_references(txn.get(), snapshot_versionstamp, &has_references,
                                              false);
    if (err != TxnErrorCode::TXN_OK) {
        status->set_code(cast_as<ErrCategory::READ>(err));
        status->set_msg("failed to check snapshot references, snapshot_id=" + snapshot_id);
        return;
    } else if (has_references) {
        // still has references, cannot drop
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg("cannot drop snapshot that is referenced by other instance");
        return;
    }

    // Mark snapshot as RECYCLED instead of directly deleting it
    // This allows the recycler to clean up the object storage data
    snapshot_pb.set_status(SnapshotStatus::SNAPSHOT_RECYCLED);

    std::string updated_snapshot_val;
    if (!snapshot_pb.SerializeToString(&updated_snapshot_val)) {
        status->set_msg("failed to serialize updated SnapshotPB");
        status->set_code(MetaServiceCode::PROTOBUF_SERIALIZE_ERR);
        return;
    }

    std::string snapshot_full_key = versioned::snapshot_full_key({snapshot_pb.instance_id()});
    std::string snapshot_key = encode_versioned_key(snapshot_full_key, snapshot_versionstamp);
    txn->put(snapshot_key, updated_snapshot_val);

    LOG_INFO("drop snapshot completed, marked as RECYCLED")
            .tag("snapshot_key", hex(snapshot_full_key))
            .tag("instance_id", instance_id)
            .tag("snapshot_id", snapshot_id);

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        status->set_code(cast_as<ErrCategory::COMMIT>(err));
        status->set_msg(fmt::format("failed to commit kv txn, err={}", err));
        LOG(WARNING) << status->msg();
    }
}

void SnapshotManager::list_snapshot(std::string_view instance_id,
                                    const ListSnapshotRequest& request,
                                    ListSnapshotResponse* response) {
    auto* status = response->mutable_status();
    status->set_code(MetaServiceCode::OK);
    status->set_msg("OK");

    std::string required_snapshot_id =
            request.has_required_snapshot_id() ? request.required_snapshot_id() : "";
    bool include_aborted = request.has_include_aborted() ? request.include_aborted() : false;

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        status->set_code(cast_as<ErrCategory::CREATE>(err));
        status->set_msg("failed to create txn");
        LOG(WARNING) << status->msg() << " err=" << err;
        return;
    }

    std::vector<std::pair<SnapshotPB, Versionstamp>> snapshots;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string error_msg;
    get_all_snapshots(txn.get(), instance_id, required_snapshot_id, &snapshots, code, error_msg);
    if (code != MetaServiceCode::OK) {
        status->set_code(code);
        status->set_msg(error_msg);
        LOG(WARNING) << status->msg() << " err=" << err;
        return;
    }

    for (auto&& [snapshot_pb, snapshot_versionstamp] : snapshots) {
        if (snapshot_pb.status() == SnapshotStatus::SNAPSHOT_RECYCLED) {
            continue;
        }
        if (!include_aborted && snapshot_pb.status() == SnapshotStatus::SNAPSHOT_ABORTED) {
            continue;
        }

        std::string snapshot_id = snapshot_versionstamp.to_string();
        SnapshotInfoPB snapshot_info;
        snapshot_info.set_snapshot_id(snapshot_id);
        snapshot_info.set_instance_id(snapshot_pb.instance_id());
        snapshot_info.set_status(snapshot_pb.status());
        snapshot_info.set_type(snapshot_pb.type());

        if (snapshot_pb.has_image_url()) {
            snapshot_info.set_image_url(snapshot_pb.image_url());
        }
        if (snapshot_pb.has_last_journal_id()) {
            snapshot_info.set_journal_id(snapshot_pb.last_journal_id());
        }
        if (snapshot_pb.has_create_at()) {
            snapshot_info.set_create_at(snapshot_pb.create_at());
        }
        if (snapshot_pb.has_finish_at()) {
            snapshot_info.set_finish_at(snapshot_pb.finish_at());
        }
        if (snapshot_pb.has_snapshot_ancestor()) {
            snapshot_info.set_ancestor_id(snapshot_pb.snapshot_ancestor());
        }
        if (snapshot_pb.has_auto_()) {
            snapshot_info.set_auto_snapshot(snapshot_pb.auto_());
        }
        if (snapshot_pb.has_ttl_seconds()) {
            snapshot_info.set_ttl_seconds(snapshot_pb.ttl_seconds());
        }
        if (snapshot_pb.has_timeout_seconds()) {
            snapshot_info.set_timeout_seconds(snapshot_pb.timeout_seconds());
        }
        if (snapshot_pb.has_label()) {
            snapshot_info.set_snapshot_label(snapshot_pb.label());
        }
        if (snapshot_pb.has_reason()) {
            snapshot_info.set_reason(snapshot_pb.reason());
        }

        MetaReader reader(snapshot_info.instance_id(), txn_kv_.get());
        std::vector<std::string> derived_instance_ids;
        TxnErrorCode err = reader.find_derived_instance_ids(txn.get(), snapshot_versionstamp,
                                                            &derived_instance_ids);
        if (err != TxnErrorCode::TXN_OK) {
            status->set_code(MetaServiceCode::KV_TXN_GET_ERR);
            status->set_msg(
                    fmt::format("failed to find derived instance ids for snapshot {}, err={}",
                                snapshot_id, err));
            LOG_WARNING(status->msg()).tag("instance_id", snapshot_info.instance_id());
            return;
        }

        if (!derived_instance_ids.empty()) {
            // Add derived instance IDs to the snapshot info
            for (const auto& instance_id : derived_instance_ids) {
                snapshot_info.add_derived_instance_ids(instance_id);
            }

            LOG_INFO("snapshot has derived instances")
                    .tag("snapshot_id", snapshot_id)
                    .tag("derived_count", derived_instance_ids.size());
        }

        *response->add_snapshots() = std::move(snapshot_info);
    }

    LOG_INFO("list snapshots completed")
            .tag("instance_id", instance_id)
            .tag("snapshots_count", response->snapshots_size())
            .tag("required_snapshot_id", required_snapshot_id)
            .tag("include_aborted", include_aborted);
}

// ============================================================================
// Helper functions for clone_instance
// ============================================================================

MetaServiceCode SnapshotManager::validate_clone_request(const CloneInstanceRequest& request,
                                                        std::string* error_msg) {
    // Validate basic parameters
    if (!request.has_clone_type()) {
        *error_msg = "clone_type not specified";
        return MetaServiceCode::INVALID_ARGUMENT;
    }

    CloneInstanceRequest::CloneType clone_type = request.clone_type();
    if (clone_type != CloneInstanceRequest::READ_ONLY &&
        clone_type != CloneInstanceRequest::WRITABLE &&
        clone_type != CloneInstanceRequest::ROLLBACK) {
        *error_msg = "invalid clone_type";
        return MetaServiceCode::INVALID_ARGUMENT;
    }

    if (!request.has_from_instance_id() || request.from_instance_id().empty()) {
        *error_msg = "from_instance_id not specified";
        return MetaServiceCode::INVALID_ARGUMENT;
    }

    if (!request.has_from_snapshot_id() || request.from_snapshot_id().empty()) {
        *error_msg = "from_snapshot_id not specified";
        return MetaServiceCode::INVALID_ARGUMENT;
    }

    if (!request.has_new_instance_id() || request.new_instance_id().empty()) {
        *error_msg = "new_instance_id not specified";
        return MetaServiceCode::INVALID_ARGUMENT;
    }

    // For all clone types, from_instance_id and new_instance_id must be different
    if (request.from_instance_id() == request.new_instance_id()) {
        *error_msg = "from_instance_id and new_instance_id must be different";
        return MetaServiceCode::INVALID_ARGUMENT;
    }

    if (clone_type == CloneInstanceRequest::WRITABLE) {
        return validate_writable_clone_request(request, error_msg);
    }

    return MetaServiceCode::OK;
}

MetaServiceCode SnapshotManager::validate_writable_clone_request(
        const CloneInstanceRequest& request, std::string* error_msg) {
    // Check obj_info requirement - request.obj_info must be set
    if (!request.has_obj_info()) {
        *error_msg = "WRITABLE clone requires obj_info";
        return MetaServiceCode::INVALID_ARGUMENT;
    }

    // Validate object storage information if obj_info is provided
    const auto& obj_info = request.obj_info();
    if (!obj_info.has_ak() || obj_info.ak().empty()) {
        *error_msg = "obj_info.ak is required";
        return MetaServiceCode::INVALID_ARGUMENT;
    }

    if (!obj_info.has_sk() || obj_info.sk().empty()) {
        *error_msg = "obj_info.sk is required";
        return MetaServiceCode::INVALID_ARGUMENT;
    }

    if (!obj_info.has_bucket() || obj_info.bucket().empty()) {
        *error_msg = "obj_info.bucket is required";
        return MetaServiceCode::INVALID_ARGUMENT;
    }

    if (!obj_info.has_endpoint() || obj_info.endpoint().empty()) {
        *error_msg = "obj_info.endpoint is required";
        return MetaServiceCode::INVALID_ARGUMENT;
    }

    return MetaServiceCode::OK;
}

void SnapshotManager::validate_source_snapshot(Transaction* txn,
                                               const std::string& from_instance_id,
                                               const std::string& from_snapshot_id,
                                               SnapshotPB* snapshot_pb, MetaServiceCode& code,
                                               std::string& error_msg) {
    std::vector<std::pair<SnapshotPB, Versionstamp>> snapshots;
    get_all_snapshots(txn, from_instance_id, from_snapshot_id, &snapshots, code, error_msg);
    if (code != MetaServiceCode::OK) {
        return;
    }
    if (snapshots.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        error_msg = fmt::format("snapshot not found: instance_id={} snapshot_id={}",
                                from_instance_id, from_snapshot_id);
        return;
    }
    if (snapshots.size() > 1) {
        // it is impossible.
        LOG_WARNING("more than one snapshot")
                .tag("instance_id", from_instance_id)
                .tag("snapshot_id", from_snapshot_id)
                .tag("snapshot_num", snapshots.size());
    }
    *snapshot_pb = snapshots[0].first;

    // Validate snapshot status
    if (snapshot_pb->status() != SnapshotStatus::SNAPSHOT_NORMAL) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        error_msg = fmt::format("snapshot status is not NORMAL, current status: {}",
                                SnapshotStatus_Name(snapshot_pb->status()));
        return;
    }

    LOG_INFO("snapshot validation completed successfully")
            .tag("snapshot_id", from_snapshot_id)
            .tag("snapshot_status", SnapshotStatus_Name(snapshot_pb->status()))
            .tag("create_at", snapshot_pb->create_at())
            .tag("image_url", snapshot_pb->image_url());
}

TxnErrorCode SnapshotManager::validate_source_instance(Transaction* txn,
                                                       const std::string& from_instance_id,
                                                       const std::string& from_snapshot_id,
                                                       CloneInstanceRequest::CloneType clone_type,
                                                       InstanceInfoPB* from_instance_info,
                                                       std::string* error_msg) {
    // Get source instance information
    InstanceKeyInfo source_key_info {from_instance_id};
    std::string from_instance_key;
    instance_key(source_key_info, &from_instance_key);

    std::string from_instance_value;
    TxnErrorCode err = txn->get(from_instance_key, &from_instance_value);
    if (err != TxnErrorCode::TXN_OK) {
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            *error_msg = fmt::format("source instance not found: {}", from_instance_id);
        } else {
            *error_msg = "failed to get source instance";
        }
        return err;
    }

    // Parse source instance information
    if (!from_instance_info->ParseFromArray(from_instance_value.data(),
                                            from_instance_value.size())) {
        *error_msg = "failed to parse source InstanceInfoPB";
        return TxnErrorCode::TXN_UNIDENTIFIED_ERROR;
    }

    // Validate source instance status
    if (from_instance_info->status() != InstanceInfoPB::NORMAL) {
        *error_msg = fmt::format("source instance status is not NORMAL: {}",
                                 InstanceInfoPB::Status_Name(from_instance_info->status()));
        return TxnErrorCode::TXN_KEY_NOT_FOUND;
    }
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode SnapshotManager::check_target_instance_existence(
        Transaction* txn, const std::string& new_instance_id, const std::string& from_instance_id,
        const std::string& from_snapshot_id, bool is_readonly, bool* already_exists,
        CloneInstanceResponse* response, const SnapshotPB& snapshot_pb,
        const InstanceInfoPB& from_instance_info, std::string* error_msg) {
    std::string new_instance_key;
    InstanceKeyInfo new_key_info {new_instance_id};
    instance_key(new_key_info, &new_instance_key);

    std::string existing_val;
    TxnErrorCode err = txn->get(new_instance_key, &existing_val);

    if (err == TxnErrorCode::TXN_OK) {
        // Instance exists, check if it's an idempotent operation
        InstanceInfoPB existing_instance;
        if (existing_instance.ParseFromString(existing_val) &&
            existing_instance.source_instance_id() == from_instance_id &&
            existing_instance.source_snapshot_id() == from_snapshot_id &&
            existing_instance.ready_only() == is_readonly) {
            // Idempotent operation
            LOG_INFO("Clone already exists with same configuration")
                    .tag("clone_type", is_readonly ? "READ_ONLY" : "WRITABLE");
            std::string helper_err;
            MetaServiceCode helper_code = set_snapshot_info_in_response(
                    response, snapshot_pb, from_instance_info, txn, &helper_err);
            if (helper_code != MetaServiceCode::OK) {
                if (error_msg != nullptr) {
                    *error_msg = std::move(helper_err);
                }
                *already_exists = true;
                return TxnErrorCode::TXN_UNIDENTIFIED_ERROR;
            }
            *already_exists = true;
            return TxnErrorCode::TXN_OK;
        } else {
            *error_msg = fmt::format("instance with id '{}' already exists", new_instance_id);
            *already_exists = true;
            return TxnErrorCode::TXN_CONFLICT;
        }
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        *error_msg = "failed to check new instance existence";
        return err;
    }

    *already_exists = false;
    return TxnErrorCode::TXN_OK;
}

InstanceInfoPB SnapshotManager::create_readonly_instance_info(
        const std::string& new_instance_id, const InstanceInfoPB& from_instance_info,
        const std::string& from_instance_id, const std::string& from_snapshot_id) {
    InstanceInfoPB new_instance;

    // Basic information
    new_instance.set_instance_id(new_instance_id);
    new_instance.set_name(fmt::format("clone_read_only_{}", new_instance_id));
    new_instance.set_user_id(from_instance_info.user_id());
    new_instance.set_ctime(std::time(nullptr));
    new_instance.set_mtime(std::time(nullptr));
    new_instance.set_status(InstanceInfoPB::NORMAL);
    new_instance.set_ready_only(true); // READ_ONLY flag

    // Derivation relationship information
    new_instance.set_source_instance_id(from_instance_id);
    new_instance.set_source_snapshot_id(from_snapshot_id);

    // Inherit source instance storage configuration (READ_ONLY fully shared)
    for (const auto& obj_info : from_instance_info.obj_info()) {
        *new_instance.add_obj_info() = obj_info;
    }
    for (const auto& resource_id : from_instance_info.resource_ids()) {
        new_instance.add_resource_ids(resource_id);
    }
    if (from_instance_info.has_enable_storage_vault()) {
        new_instance.set_enable_storage_vault(from_instance_info.enable_storage_vault());
    }
    for (const auto& vault_name : from_instance_info.storage_vault_names()) {
        new_instance.add_storage_vault_names(vault_name);
    }
    if (from_instance_info.has_default_storage_vault_id()) {
        new_instance.set_default_storage_vault_id(from_instance_info.default_storage_vault_id());
    }

    // Inherit snapshot-related configuration
    new_instance.set_multi_version_status(MultiVersionStatus::MULTI_VERSION_READ_WRITE);
    new_instance.set_snapshot_switch_status(SnapshotSwitchStatus::SNAPSHOT_SWITCH_OFF);

    // Read-only instances disable snapshots by default
    if (from_instance_info.has_max_reserved_snapshot()) {
        new_instance.set_max_reserved_snapshot(0);
    }
    if (from_instance_info.has_snapshot_interval_seconds()) {
        new_instance.set_snapshot_interval_seconds(from_instance_info.snapshot_interval_seconds());
    }

    return new_instance;
}

InstanceInfoPB SnapshotManager::create_writable_instance_info(
        const std::string& new_instance_id, const InstanceInfoPB& from_instance_info,
        const std::string& from_instance_id, const std::string& from_snapshot_id) {
    InstanceInfoPB new_instance;

    // Basic information
    new_instance.set_instance_id(new_instance_id);
    new_instance.set_name(fmt::format("clone_writable_{}", new_instance_id));
    new_instance.set_user_id(from_instance_info.user_id());
    new_instance.set_ctime(std::time(nullptr));
    new_instance.set_mtime(std::time(nullptr));
    new_instance.set_status(InstanceInfoPB::NORMAL);
    new_instance.set_ready_only(false); // WRITABLE flag

    // Derivation relationship information
    new_instance.set_source_instance_id(from_instance_id);
    new_instance.set_source_snapshot_id(from_snapshot_id);

    // Configure storage hierarchy (Copy-on-Write)
    // Storage vault configuration will be finalized in setup_writable_storage
    new_instance.set_enable_storage_vault(from_instance_info.enable_storage_vault());

    // Set snapshot-related configuration
    new_instance.set_multi_version_status(MultiVersionStatus::MULTI_VERSION_READ_WRITE);
    new_instance.set_snapshot_switch_status(SnapshotSwitchStatus::SNAPSHOT_SWITCH_OFF);

    // Inherit snapshot configuration parameters
    if (from_instance_info.has_max_reserved_snapshot()) {
        new_instance.set_max_reserved_snapshot(from_instance_info.max_reserved_snapshot());
    } else {
        new_instance.set_max_reserved_snapshot(1);
    }
    if (from_instance_info.has_snapshot_interval_seconds()) {
        new_instance.set_snapshot_interval_seconds(from_instance_info.snapshot_interval_seconds());
    }

    return new_instance;
}

MetaServiceCode SnapshotManager::setup_writable_storage(Transaction* txn,
                                                        const CloneInstanceRequest& request,
                                                        const InstanceInfoPB& from_instance_info,
                                                        InstanceInfoPB* new_instance,
                                                        std::string* error_msg) {
    // Setup storage for writable clone:
    // - Obj info mode: copy obj_info and append user-provided obj_info if present
    // - Storage vault mode: copy source storage vaults and create a new writable vault

    const std::string& from_instance_id = request.from_instance_id();
    const std::string& new_instance_id = new_instance->instance_id();

    if (!from_instance_info.enable_storage_vault()) {
        new_instance->set_enable_storage_vault(false);
        new_instance->clear_storage_vault_names();
        new_instance->clear_default_storage_vault_id();

        new_instance->mutable_resource_ids()->CopyFrom(from_instance_info.resource_ids());
        new_instance->clear_obj_info();

        auto* target_obj_infos = new_instance->mutable_obj_info();
        for (const auto& source_obj_info : from_instance_info.obj_info()) {
            *target_obj_infos->Add() = source_obj_info;
            if (std::find(from_instance_info.resource_ids().begin(),
                          from_instance_info.resource_ids().end(),
                          source_obj_info.id()) == from_instance_info.resource_ids().end()) {
                // To keep compatible, supplement resource_ids if missing
                new_instance->add_resource_ids(source_obj_info.id());
            }
        }

        ObjectStoreInfoPB new_obj_info = request.obj_info();
        std::string new_obj_id = next_available_resource_id(*new_instance);
        new_obj_info.set_id(new_obj_id);
        auto now_time = std::chrono::system_clock::now();
        uint64_t now_seconds =
                std::chrono::duration_cast<std::chrono::seconds>(now_time.time_since_epoch())
                        .count();
        new_obj_info.set_ctime(now_seconds);
        new_obj_info.set_mtime(now_seconds);
        *target_obj_infos->Add() = new_obj_info;
        new_instance->add_resource_ids(new_obj_id);

        LOG_INFO("WRITABLE clone legacy storage configuration completed")
                .tag("new_instance_id", new_instance_id)
                .tag("new_resource_id", new_obj_id)
                .tag("obj_info_count", new_instance->obj_info_size());

        return MetaServiceCode::OK;
    }

    // Step 1: Copy source storage vaults and append to new instance
    for (const auto& source_resource_id : from_instance_info.resource_ids()) {
        // Get source storage vault
        std::string source_vault_key_str;
        storage_vault_key({from_instance_id, source_resource_id}, &source_vault_key_str);
        std::string source_vault_val;
        TxnErrorCode err = txn->get(source_vault_key_str, &source_vault_val);

        if (err != TxnErrorCode::TXN_OK) {
            *error_msg = fmt::format("failed to get source storage vault, resource_id={}, err={}",
                                     source_resource_id, err);
            return cast_as<ErrCategory::READ>(err);
        }

        StorageVaultPB source_vault;
        if (!source_vault.ParseFromString(source_vault_val)) {
            *error_msg = "failed to parse source StorageVaultPB";
            return MetaServiceCode::PROTOBUF_PARSE_ERR;
        }

        // Should reuse the source's resource_id.
        // Save to new instance storage vault with source resource_id
        std::string new_vault_key_str = storage_vault_key({new_instance_id, source_resource_id});
        std::string new_vault_val = source_vault.SerializeAsString();
        txn->put(new_vault_key_str, new_vault_val);

        // Add to new_instance configuration
        new_instance->add_resource_ids(source_resource_id);
        new_instance->add_storage_vault_names(source_vault.name());

        LOG_INFO("Copied source storage vault with new resource_id")
                .tag("new_instance_id", new_instance_id)
                .tag("source_resource_id", source_resource_id)
                .tag("vault_name", source_vault.name());
    }

    // Step 2: Create new writable storage vault with user-provided obj_info
    std::string new_resource_id = next_available_resource_id(*new_instance);
    std::string new_storage_vault_id = new_resource_id;

    // Get object store info from request
    const auto& obj_info = request.obj_info();

    // Create new storage vault
    StorageVaultPB new_storage_vault;
    new_storage_vault.set_id(new_storage_vault_id);
    new_storage_vault.set_name(fmt::format("clone_vault_{}", new_storage_vault_id));
    *new_storage_vault.mutable_obj_info() = obj_info;

    // Save new writable storage vault
    std::string new_vault_key_str;
    storage_vault_key({new_instance_id, new_storage_vault_id}, &new_vault_key_str);
    std::string new_vault_val = new_storage_vault.SerializeAsString();
    txn->put(new_vault_key_str, new_vault_val);

    // Add to instance configuration
    new_instance->add_resource_ids(new_storage_vault_id);
    new_instance->add_storage_vault_names(new_storage_vault.name());

    // Ensure the writable clone points at the new writable storage vault
    new_instance->set_default_storage_vault_id(new_storage_vault_id);

    LOG_INFO("WRITABLE clone storage configuration completed")
            .tag("new_instance_id", new_instance_id)
            .tag("new_writable_resource_id", new_storage_vault_id)
            .tag("new_writable_vault_name", new_storage_vault.name())
            .tag("total_resource_ids", new_instance->resource_ids_size())
            .tag("total_vault_names", new_instance->storage_vault_names_size())
            .tag("default_storage_vault_id", new_instance->default_storage_vault_id());

    return MetaServiceCode::OK;
}

MetaServiceCode SnapshotManager::clone_storage_vault_entries(
        Transaction* txn, const std::string& from_instance_id, const std::string& new_instance_id,
        const InstanceInfoPB& from_instance_info, std::string* error_msg) {
    if (!from_instance_info.enable_storage_vault()) {
        return MetaServiceCode::OK;
    }

    // storage_vault entries are stored under keys encoded as:
    //   0x01 "storage_vault" ${instance_id} "vault" ${resource_id}
    // so every instance has its own namespace. To let the new instance
    // reference the same vault metadata, we must materialize a copy under
    // the new instance_id with the identical resource_id.
    for (const auto& resource_id : from_instance_info.resource_ids()) {
        std::string source_vault_key_str;
        storage_vault_key({from_instance_id, resource_id}, &source_vault_key_str);
        std::string source_vault_val;
        TxnErrorCode err = txn->get(source_vault_key_str, &source_vault_val);

        if (err != TxnErrorCode::TXN_OK) {
            *error_msg =
                    fmt::format("failed to copy storage vault for rollback, resource_id={}, err={}",
                                resource_id, err);
            LOG_WARNING("Failed to copy storage vault for rollback")
                    .tag("from_instance_id", from_instance_id)
                    .tag("target_instance_id", new_instance_id)
                    .tag("resource_id", resource_id)
                    .tag("error", err);
            return cast_as<ErrCategory::READ>(err);
        }

        std::string target_vault_key_str;
        storage_vault_key({new_instance_id, resource_id}, &target_vault_key_str);
        txn->put(target_vault_key_str, source_vault_val);
    }

    LOG_INFO("Copied storage vaults for rollback instance")
            .tag("from_instance_id", from_instance_id)
            .tag("target_instance_id", new_instance_id)
            .tag("vault_count", from_instance_info.resource_ids_size());

    return MetaServiceCode::OK;
}

void SnapshotManager::establish_snapshot_reference(Transaction* txn,
                                                   const std::string& from_instance_id,
                                                   const Versionstamp& snapshot_versionstamp,
                                                   const std::string& new_instance_id) {
    LOG_INFO("starting snapshot reference relationship processing")
            .tag("from_instance_id", from_instance_id)
            .tag("from_snapshot_id", snapshot_versionstamp.to_string())
            .tag("new_instance_id", new_instance_id);

    // Write snapshot reference key to record reference relationship
    versioned::SnapshotReferenceKeyInfo ref_key_info {from_instance_id, snapshot_versionstamp,
                                                      new_instance_id};
    std::string reference_key = versioned::snapshot_reference_key(ref_key_info);
    std::string reference_val;

    txn->put(reference_key, reference_val);

    LOG_INFO("snapshot reference relationship established")
            .tag("from_instance_id", from_instance_id)
            .tag("snapshot_id", snapshot_versionstamp.to_string())
            .tag("new_instance_id", new_instance_id)
            .tag("reference_key", hex(reference_key));
}

MetaServiceCode SnapshotManager::update_source_instance_successor(
        Transaction* txn, const std::string& from_instance_key, InstanceInfoPB* from_instance_info,
        const std::string& new_instance_id, std::string* error_msg) {
    // Update source instance to record the successor instance
    from_instance_info->set_successor_instance_id(new_instance_id);
    std::string updated_source_instance_val;
    if (!from_instance_info->SerializeToString(&updated_source_instance_val)) {
        *error_msg = "failed to serialize updated source InstanceInfoPB";
        return MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
    }
    txn->put(from_instance_key, updated_source_instance_val);
    return MetaServiceCode::OK;
}

MetaServiceCode SnapshotManager::handle_readonly_clone(Transaction* txn,
                                                       const CloneInstanceRequest& request,
                                                       const SnapshotPB& snapshot_pb,
                                                       const InstanceInfoPB& from_instance_info,
                                                       CloneInstanceResponse* response,
                                                       std::string* error_msg) {
    const std::string& new_instance_id = request.new_instance_id();
    const std::string& from_instance_id = from_instance_info.instance_id();
    const std::string& from_snapshot_id = request.from_snapshot_id();

    LOG_INFO("starting READ_ONLY clone")
            .tag("new_instance_id", new_instance_id)
            .tag("from_instance_id", from_instance_id)
            .tag("from_snapshot_id", from_snapshot_id);

    // Check if target instance ID already exists
    bool already_exists = false;
    TxnErrorCode err = check_target_instance_existence(
            txn, new_instance_id, from_instance_id, from_snapshot_id, true, &already_exists,
            response, snapshot_pb, from_instance_info, error_msg);

    if (err != TxnErrorCode::TXN_OK) {
        if (already_exists && err == TxnErrorCode::TXN_CONFLICT) {
            return MetaServiceCode::ALREADY_EXISTED;
        }
        return cast_as<ErrCategory::READ>(err);
    }

    if (already_exists) {
        return MetaServiceCode::OK; // Idempotent case
    }

    // Create new read-only instance
    InstanceInfoPB new_instance = create_readonly_instance_info(new_instance_id, from_instance_info,
                                                                from_instance_id, from_snapshot_id);

    // Serialize and save new instance
    std::string new_instance_val;
    if (!new_instance.SerializeToString(&new_instance_val)) {
        *error_msg = "failed to serialize new READ_ONLY InstanceInfoPB";
        return MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
    }

    std::string new_instance_key;
    InstanceKeyInfo new_key_info {new_instance_id};
    instance_key(new_key_info, &new_instance_key);
    txn->put(new_instance_key, new_instance_val);

    MetaServiceCode storage_code = clone_storage_vault_entries(
            txn, from_instance_id, new_instance_id, from_instance_info, error_msg);
    if (storage_code != MetaServiceCode::OK) {
        return storage_code;
    }

    // Set snapshot info in response
    std::string helper_error;
    MetaServiceCode helper_code = set_snapshot_info_in_response(
            response, snapshot_pb, from_instance_info, txn, &helper_error);
    if (helper_code != MetaServiceCode::OK) {
        if (error_msg != nullptr) {
            *error_msg = std::move(helper_error);
        }
        return helper_code;
    }

    LOG_INFO("READ_ONLY clone prepared successfully")
            .tag("new_instance_id", new_instance_id)
            .tag("new_instance_name", new_instance.name())
            .tag("ready_only", new_instance.ready_only())
            .tag("source_instance_id", new_instance.source_instance_id())
            .tag("source_snapshot_id", new_instance.source_snapshot_id())
            .tag("original_instance_id", new_instance.original_instance_id())
            .tag("image_url", snapshot_pb.image_url())
            .tag("snapshot_resource_id", snapshot_pb.resource_id());

    return MetaServiceCode::OK;
}

MetaServiceCode SnapshotManager::handle_writable_clone(Transaction* txn,
                                                       const CloneInstanceRequest& request,
                                                       const SnapshotPB& snapshot_pb,
                                                       const InstanceInfoPB& from_instance_info,
                                                       CloneInstanceResponse* response,
                                                       std::string* error_msg) {
    const std::string& new_instance_id = request.new_instance_id();
    const std::string& from_instance_id = from_instance_info.instance_id();
    const std::string& from_snapshot_id = request.from_snapshot_id();

    LOG_INFO("starting WRITABLE clone")
            .tag("new_instance_id", new_instance_id)
            .tag("from_instance_id", from_instance_id)
            .tag("from_snapshot_id", from_snapshot_id);

    // Check if target instance ID already exists
    bool already_exists = false;
    TxnErrorCode err = check_target_instance_existence(
            txn, new_instance_id, from_instance_id, from_snapshot_id, false, &already_exists,
            response, snapshot_pb, from_instance_info, error_msg);

    if (err != TxnErrorCode::TXN_OK) {
        if (already_exists && err == TxnErrorCode::TXN_CONFLICT) {
            return MetaServiceCode::ALREADY_EXISTED;
        }
        return cast_as<ErrCategory::READ>(err);
    }

    if (already_exists) {
        return MetaServiceCode::OK; // Idempotent case
    }

    // Create new writable instance
    InstanceInfoPB new_instance = create_writable_instance_info(new_instance_id, from_instance_info,
                                                                from_instance_id, from_snapshot_id);

    // Setup writable storage - copy source vaults and create new writable vault
    MetaServiceCode code =
            setup_writable_storage(txn, request, from_instance_info, &new_instance, error_msg);
    if (code != MetaServiceCode::OK) {
        return code;
    }

    // Serialize and save new instance
    std::string new_instance_val;
    if (!new_instance.SerializeToString(&new_instance_val)) {
        *error_msg = "failed to serialize new WRITABLE InstanceInfoPB";
        return MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
    }

    std::string new_instance_key;
    InstanceKeyInfo new_key_info {new_instance_id};
    instance_key(new_key_info, &new_instance_key);
    txn->put(new_instance_key, new_instance_val);

    // Set snapshot info in response
    std::string helper_error;
    MetaServiceCode helper_code = set_snapshot_info_in_response(
            response, snapshot_pb, from_instance_info, txn, &helper_error);
    if (helper_code != MetaServiceCode::OK) {
        if (error_msg != nullptr) {
            *error_msg = std::move(helper_error);
        }
        return helper_code;
    }

    LOG_INFO("WRITABLE clone instance prepared")
            .tag("new_instance_id", new_instance_id)
            .tag("new_instance_name", new_instance.name())
            .tag("ready_only", new_instance.ready_only())
            .tag("source_instance_id", new_instance.source_instance_id())
            .tag("source_snapshot_id", new_instance.source_snapshot_id())
            .tag("original_instance_id", new_instance.original_instance_id())
            .tag("image_url", snapshot_pb.image_url())
            .tag("snapshot_resource_id", snapshot_pb.resource_id());

    return MetaServiceCode::OK;
}

MetaServiceCode SnapshotManager::handle_rollback_clone(Transaction* txn,
                                                       const CloneInstanceRequest& request,
                                                       const SnapshotPB& snapshot_pb,
                                                       const InstanceInfoPB& from_instance_info,
                                                       CloneInstanceResponse* response,
                                                       std::string* error_msg) {
    const std::string& new_instance_id = request.new_instance_id();
    const std::string& from_instance_id = request.from_instance_id();
    const std::string& from_snapshot_id = request.from_snapshot_id();

    LOG_INFO("Creating ROLLBACK clone")
            .tag("from_instance_id", from_instance_id)
            .tag("new_instance_id", new_instance_id)
            .tag("from_snapshot_id", from_snapshot_id);

    // Create new instance by copying from source instance
    InstanceInfoPB target_instance_info = from_instance_info;

    // Update key fields for the new instance
    target_instance_info.set_instance_id(new_instance_id);
    target_instance_info.set_source_snapshot_id(from_snapshot_id);
    // the source instance is the real instance which creates this snapshot
    target_instance_info.set_source_instance_id(snapshot_pb.instance_id());
    target_instance_info.set_ctime(std::time(nullptr));

    // Set original instance relationship
    if (from_instance_info.has_original_instance_id()) {
        target_instance_info.set_original_instance_id(from_instance_info.original_instance_id());
    } else {
        target_instance_info.set_original_instance_id(from_instance_id);
    }

    // Prepare target instance key
    InstanceKeyInfo target_key_info {new_instance_id};
    std::string target_instance_key_str;
    instance_key(target_key_info, &target_instance_key_str);

    // Serialize updated instance info
    std::string updated_target_instance_val = target_instance_info.SerializeAsString();
    if (updated_target_instance_val.empty()) {
        *error_msg = "Failed to serialize rollback instance info";
        return MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
    }

    // Update source instance to record the successor instance
    InstanceKeyInfo source_key_info {from_instance_id};
    std::string from_instance_key;
    instance_key(source_key_info, &from_instance_key);

    InstanceInfoPB mutable_from_instance_info = from_instance_info;
    MetaServiceCode code = update_source_instance_successor(
            txn, from_instance_key, &mutable_from_instance_info, new_instance_id, error_msg);
    if (code != MetaServiceCode::OK) {
        return code;
    }

    code = clone_storage_vault_entries(txn, from_instance_id, new_instance_id, from_instance_info,
                                       error_msg);
    if (code != MetaServiceCode::OK) {
        return code;
    }

    // Update rollback instance in transaction
    txn->put(target_instance_key_str, updated_target_instance_val);

    // Set snapshot info in response
    std::string helper_error;
    MetaServiceCode helper_code = set_snapshot_info_in_response(
            response, snapshot_pb, from_instance_info, txn, &helper_error);
    if (helper_code != MetaServiceCode::OK) {
        if (error_msg != nullptr) {
            *error_msg = fmt::format("failed to set snapshot info: {}", helper_error);
        }
        return helper_code;
    }

    LOG_INFO("ROLLBACK clone prepared successfully")
            .tag("target_instance_id", new_instance_id)
            .tag("snapshot_id", from_snapshot_id)
            .tag("rollback_to_ctime", target_instance_info.ctime())
            .tag("image_url", snapshot_pb.image_url())
            .tag("snapshot_resource_id", snapshot_pb.resource_id());

    return MetaServiceCode::OK;
}

void SnapshotManager::clone_instance(const CloneInstanceRequest& request,
                                     CloneInstanceResponse* response) {
    auto* status = response->mutable_status();
    status->set_code(MetaServiceCode::OK);
    status->set_msg("OK");

    std::string error_msg;

    // 1. Validate request parameters
    MetaServiceCode code = validate_clone_request(request, &error_msg);
    if (code != MetaServiceCode::OK) {
        status->set_code(code);
        status->set_msg(error_msg);
        return;
    }

    // 2. Create transaction
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        status->set_code(cast_as<ErrCategory::CREATE>(err));
        status->set_msg("failed to create transaction");
        return;
    }

    // 3. Parse and validate snapshot
    Versionstamp snapshot_versionstamp;
    if (!parse_snapshot_versionstamp(request.from_snapshot_id(), &snapshot_versionstamp)) {
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg("failed to parse snapshot_id to versionstamp");
        return;
    }

    std::string from_instance_id = request.from_instance_id();
    SnapshotPB snapshot_pb;
    validate_source_snapshot(txn.get(), from_instance_id, request.from_snapshot_id(), &snapshot_pb,
                             code, error_msg);
    if (code != MetaServiceCode::OK) {
        status->set_code(code);
        status->set_msg(error_msg);
        return;
    }

    // If the from_instance is created by rollback, the snapshot may be created in its predecessor
    // instances. For READ_ONLY and WRITABLE clone, set its source instance to the real instance
    // which creates this snapshot.
    if (request.clone_type() != CloneInstanceRequest::ROLLBACK &&
        snapshot_pb.instance_id() != request.from_instance_id()) {
        from_instance_id = snapshot_pb.instance_id();
        LOG_INFO("clone instance change from_instance_id")
                .tag("request from_instance_id", request.from_instance_id())
                .tag("snapshot instance_id", snapshot_pb.instance_id())
                .tag("snapshot_id", request.from_snapshot_id());
    }

    // 4. Validate source instance
    InstanceInfoPB from_instance_info;
    err = validate_source_instance(txn.get(), from_instance_id, request.from_snapshot_id(),
                                   request.clone_type(), &from_instance_info, &error_msg);
    if (err != TxnErrorCode::TXN_OK) {
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            status->set_code(MetaServiceCode::CLUSTER_NOT_FOUND);
        } else {
            status->set_code(cast_as<ErrCategory::READ>(err));
        }
        status->set_msg(error_msg);
        return;
    }
    // Type-specific validation
    if (request.clone_type() == CloneInstanceRequest::ROLLBACK) {
        // Validate source instance does not already have a successor instance
        if (from_instance_info.has_successor_instance_id() &&
            !from_instance_info.successor_instance_id().empty()) {
            status->set_code(MetaServiceCode::INVALID_ARGUMENT);
            status->set_msg(fmt::format(
                    "source instance already has a successor instance: {}, ROLLBACK only one "
                    "successor is allowed",
                    from_instance_info.successor_instance_id()));
            return;
        }
    }

    // 5. Handle clone by type
    switch (request.clone_type()) {
    case CloneInstanceRequest::READ_ONLY:
        code = handle_readonly_clone(txn.get(), request, snapshot_pb, from_instance_info, response,
                                     &error_msg);
        break;
    case CloneInstanceRequest::WRITABLE:
        code = handle_writable_clone(txn.get(), request, snapshot_pb, from_instance_info, response,
                                     &error_msg);
        break;
    case CloneInstanceRequest::ROLLBACK:
        code = handle_rollback_clone(txn.get(), request, snapshot_pb, from_instance_info, response,
                                     &error_msg);
        break;
    default:
        code = MetaServiceCode::INVALID_ARGUMENT;
        error_msg = "invalid clone_type";
    }

    if (code != MetaServiceCode::OK) {
        status->set_code(code);
        status->set_msg(error_msg);
        return;
    }

    // 6. Establish snapshot reference relationship
    establish_snapshot_reference(txn.get(), snapshot_pb.instance_id(), snapshot_versionstamp,
                                 request.new_instance_id());

    // 7. Commit transaction
    LOG_INFO("committing clone_instance transaction")
            .tag("clone_type", CloneInstanceRequest::CloneType_Name(request.clone_type()));

    txn->atomic_add(system_meta_service_instance_update_key(), 1);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        status->set_code(cast_as<ErrCategory::COMMIT>(err));
        status->set_msg(fmt::format("failed to commit clone transaction, err={}", err));
        LOG_WARNING("clone transaction commit failed")
                .tag("error_code", err)
                .tag("clone_type", CloneInstanceRequest::CloneType_Name(request.clone_type()))
                .tag("new_instance_id", request.new_instance_id())
                .tag("message", "all changes have been rolled back");
        return;
    }

    // 8. Notify instance refresh
    notify_refresh_instance(txn_kv_, request.new_instance_id(), nullptr, /*include_self=*/true);
    if (request.clone_type() == CloneInstanceRequest::ROLLBACK) {
        notify_refresh_instance(txn_kv_, from_instance_id, nullptr,
                                /*include_self=*/true);
    }

    // Log operation completion
    LOG_INFO("clone_instance operation completed successfully")
            .tag("operation_type", CloneInstanceRequest::CloneType_Name(request.clone_type()))
            .tag("request_from_instance_id", request.from_instance_id())
            .tag("from_instance_id", from_instance_id)
            .tag("from_snapshot_id", request.from_snapshot_id())
            .tag("new_instance_id", request.new_instance_id())
            .tag("request_ip", request.has_request_ip() ? request.request_ip() : "")
            .tag("transaction_committed", true);
}

static void clear_mv_key_space(Transaction* txn, const std::string& id) {
    // The delete_bitmap_key is reserved since it is not depends on multi version status.
    std::string begin_key = versioned::version_key_prefix(id);
    std::string end_key = versioned::version_key_prefix(id + '\x00');
    txn->remove(begin_key, end_key);
    begin_key = versioned::index_key_prefix(id);
    end_key = versioned::index_key_prefix(id + '\x00');
    txn->remove(begin_key, end_key);
    begin_key = versioned::stats_key_prefix(id);
    end_key = versioned::stats_key_prefix(id + '\x00');
    txn->remove(begin_key, end_key);
    begin_key = versioned::meta_partition_key({id, 0});
    end_key = versioned::meta_partition_key({id, std::numeric_limits<int64_t>::max()});
    txn->remove(begin_key, end_key);
    begin_key = versioned::meta_index_key({id, 0});
    end_key = versioned::meta_index_key({id, std::numeric_limits<int64_t>::max()});
    txn->remove(begin_key, end_key);
    begin_key = versioned::meta_tablet_key({id, 0});
    end_key = versioned::meta_tablet_key({id, std::numeric_limits<int64_t>::max()});
    txn->remove(begin_key, end_key);
    begin_key = versioned::meta_schema_key({id, 0, 0});
    end_key = versioned::meta_schema_key(
            {id, std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::max()});
    txn->remove(begin_key, end_key);
    begin_key = versioned::meta_rowset_load_key({id, 0, 0});
    end_key = versioned::meta_rowset_load_key(
            {id, std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::max()});
    txn->remove(begin_key, end_key);
    begin_key = versioned::meta_rowset_compact_key({id, 0, 0});
    end_key = versioned::meta_rowset_compact_key(
            {id, std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::max()});
    txn->remove(begin_key, end_key);
    begin_key = versioned::data_key_prefix(id);
    end_key = versioned::data_key_prefix(id + '\x00');
    txn->remove(begin_key, end_key);
    begin_key = versioned::snapshot_key_prefix(id);
    end_key = versioned::snapshot_key_prefix(id + '\x00');
    txn->remove(begin_key, end_key);
    begin_key = versioned::log_key_prefix(id);
    end_key = versioned::log_key_prefix(id + '\x00');
    txn->remove(begin_key, end_key);
}

std::pair<MetaServiceCode, std::string> SnapshotManager::set_multi_version_status(
        std::string_view id, MultiVersionStatus multi_version_status) {
    LOG_INFO("set_multi_version_status")
            .tag("instance_id", id)
            .tag("multi_version_status", MultiVersionStatus_Name(multi_version_status));

    std::string instance_id(id);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return {cast_as<ErrCategory::CREATE>(err), "failed to create txn"};
    }

    std::string instance_key_str = instance_key(instance_id);
    std::string instance_val;
    err = txn->get(instance_key_str, &instance_val);
    if (err != TxnErrorCode::TXN_OK) {
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            return {MetaServiceCode::CLUSTER_NOT_FOUND, "instance not found"};
        } else {
            return {cast_as<ErrCategory::READ>(err), "failed to get instance info"};
        }
    }

    InstanceInfoPB instance_info;
    if (!instance_info.ParseFromString(instance_val)) {
        return {MetaServiceCode::PROTOBUF_PARSE_ERR, "failed to parse instance info"};
    }

    using AllowMultiVersionStatus = std::unordered_set<MultiVersionStatus>;
    std::unordered_map<MultiVersionStatus, AllowMultiVersionStatus> allowed_transitions = {
            {
                    MultiVersionStatus::MULTI_VERSION_DISABLED,
                    {MultiVersionStatus::MULTI_VERSION_DISABLED,
                     MultiVersionStatus::MULTI_VERSION_WRITE_ONLY},
            },
            {
                    MultiVersionStatus::MULTI_VERSION_WRITE_ONLY,
                    {MultiVersionStatus::MULTI_VERSION_DISABLED,
                     MultiVersionStatus::MULTI_VERSION_READ_WRITE,
                     MultiVersionStatus::MULTI_VERSION_WRITE_ONLY},
            },
            {
                    MultiVersionStatus::MULTI_VERSION_READ_WRITE,
                    {MultiVersionStatus::MULTI_VERSION_READ_WRITE,
                     MultiVersionStatus::MULTI_VERSION_WRITE_ONLY,
                     MultiVersionStatus::MULTI_VERSION_DISABLED},
            },
    };

    MultiVersionStatus current_status = instance_info.has_multi_version_status()
                                                ? instance_info.multi_version_status()
                                                : MultiVersionStatus::MULTI_VERSION_DISABLED;
    if (auto it = allowed_transitions.find(current_status);
        it == allowed_transitions.end() || !it->second.contains(multi_version_status)) {
        return {MetaServiceCode::INVALID_ARGUMENT,
                fmt::format("directly convert from {} to {} is not allowed",
                            MultiVersionStatus_Name(current_status),
                            MultiVersionStatus_Name(multi_version_status))};
    }

    // Additional checks: Switch to READ_WRITE only if snapshot switch is not DISABLED
    SnapshotSwitchStatus snapshot_switch_status =
            instance_info.has_snapshot_switch_status()
                    ? instance_info.snapshot_switch_status()
                    : SnapshotSwitchStatus::SNAPSHOT_SWITCH_DISABLED;
    if (current_status == MultiVersionStatus::MULTI_VERSION_WRITE_ONLY &&
        multi_version_status == MultiVersionStatus::MULTI_VERSION_READ_WRITE &&
        snapshot_switch_status == SnapshotSwitchStatus::SNAPSHOT_SWITCH_DISABLED) {
        return {MetaServiceCode::INVALID_ARGUMENT,
                fmt::format("cannot set multi_version_status from {} to {} when "
                            "snapshot_switch_status is DISABLED. The snapshot data migration job "
                            "is not finished yet.",
                            MultiVersionStatus_Name(current_status),
                            MultiVersionStatus_Name(multi_version_status))};
    }

    // Additional checks: Cloned instances cannot set multi_version_status to WRITE_ONLY or DISABLED
    if (instance_info.has_source_instance_id() &&
        (multi_version_status == MultiVersionStatus::MULTI_VERSION_WRITE_ONLY ||
         multi_version_status == MultiVersionStatus::MULTI_VERSION_DISABLED)) {
        return {MetaServiceCode::INVALID_ARGUMENT,
                fmt::format("cannot set multi_version_status to {} for cloned instances",
                            MultiVersionStatus_Name(multi_version_status))};
    }

    if (snapshot_switch_status == SnapshotSwitchStatus::SNAPSHOT_SWITCH_ON &&
        multi_version_status != MultiVersionStatus::MULTI_VERSION_ENABLED &&
        multi_version_status != MultiVersionStatus::MULTI_VERSION_READ_WRITE) {
        return {MetaServiceCode::INVALID_ARGUMENT,
                fmt::format("cannot set multi_version_status to {} when snapshot switch is ON. "
                            "Consider turn off snapshot switch by executing sql: "
                            "ADMIN SET CLUSTER SNAPSHOT FEATURE OFF",
                            MultiVersionStatus_Name(multi_version_status))};
    }

    // Additional checks: Disable multi version only when there is no snapshot references
    if ((current_status == MultiVersionStatus::MULTI_VERSION_READ_WRITE ||
         current_status == MultiVersionStatus::MULTI_VERSION_WRITE_ONLY) &&
        multi_version_status == MultiVersionStatus::MULTI_VERSION_DISABLED) {
        // Snapshot and snapshot references must be cleaned.
        MetaReader reader(instance_id);
        bool has_snapshot = false;
        err = reader.has_snapshot(txn.get(), &has_snapshot);
        if (err != TxnErrorCode::TXN_OK) {
            return {cast_as<ErrCategory::READ>(err), "failed to check whether there is snapshot"};
        }
        if (has_snapshot) {
            return {MetaServiceCode::INVALID_ARGUMENT,
                    "you must delete all snapshots before disabling multi version. Consider "
                    "execute sql: ADMIN DROP CLUSTER SNAPSHOT WHERE snapshot_id = '$snapshot_id'"
                    ". The dropped snapshot will be cleaned asynchronously in recycler, so you "
                    " need to wait until all snapshots are cleaned before disabling multi "
                    "version."};
        }
    }

    // Clean the multi version key space before enable double writes.
    if (current_status == MultiVersionStatus::MULTI_VERSION_DISABLED &&
        multi_version_status == MultiVersionStatus::MULTI_VERSION_WRITE_ONLY) {
        clear_mv_key_space(txn.get(), instance_id);
        instance_info.clear_migrated_key_sets();
    }

    // Disable snapshot switch when multi version is disabled.
    if (multi_version_status == MultiVersionStatus::MULTI_VERSION_DISABLED) {
        instance_info.set_snapshot_switch_status(SnapshotSwitchStatus::SNAPSHOT_SWITCH_DISABLED);
    }

    instance_info.set_multi_version_status(multi_version_status);

    std::string updated_instance_val = instance_info.SerializeAsString();
    if (updated_instance_val.empty()) {
        return {MetaServiceCode::PROTOBUF_SERIALIZE_ERR, "failed to serialize instance info"};
    }

    txn->atomic_add(system_meta_service_instance_update_key(), 1);
    txn->put(instance_key_str, updated_instance_val);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        return {cast_as<ErrCategory::COMMIT>(err), fmt::format("failed to commit txn: {}", err)};
    }

    // Notify ResourceManager to refresh instance cache
    notify_refresh_instance(txn_kv_, instance_id, nullptr, /*include_self=*/true);

    SnapshotSwitchStatus new_switch_status =
            instance_info.has_snapshot_switch_status()
                    ? instance_info.snapshot_switch_status()
                    : SnapshotSwitchStatus::SNAPSHOT_SWITCH_DISABLED;
    LOG_INFO("set_multi_version_status completed")
            .tag("instance_id", instance_id)
            .tag("old_switch_status", SnapshotSwitchStatus_Name(snapshot_switch_status))
            .tag("old_mv_status", MultiVersionStatus_Name(current_status))
            .tag("multi_version_status", MultiVersionStatus_Name(multi_version_status))
            .tag("snapshot_switch_status", SnapshotSwitchStatus_Name(new_switch_status));

    return {MetaServiceCode::OK, "success"};
}

} // namespace selectdb
