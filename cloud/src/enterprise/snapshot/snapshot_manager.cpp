#include "snapshot_manager.h"

#include <arpa/inet.h>
#include <gen_cpp/cloud.pb.h>
#include <netinet/in.h>

#include "common/encryption_util.h"
#include "common/util.h"
#include "meta-service/meta_service_helper.h"
#include "meta-store/keys.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"

namespace versioned = doris::cloud::versioned;

using doris::cloud::MetaServiceCode;
using doris::cloud::Transaction;
using doris::cloud::TxnErrorCode;
using doris::cloud::ErrCategory;
using doris::cloud::Versionstamp;
using doris::cloud::InstanceInfoPB;
using doris::cloud::SnapshotPB;
using doris::cloud::SnapshotInfoPB;
using doris::cloud::SnapshotStatus;
using doris::cloud::SnapshotSwitchStatus;
using doris::cloud::SnapshotType;
using doris::cloud::ObjectStoreInfoPB;
using doris::cloud::FullRangeGetOptions;
using doris::cloud::RangeKeySelector;
using doris::cloud::encode_versioned_key;
using doris::cloud::decode_versioned_key;
using doris::cloud::hex;

namespace selectdb {

static bool is_valid_ip_address(const std::string& ip) {
    struct sockaddr_in sa;
    struct sockaddr_in6 sa6;
    return (inet_pton(AF_INET, ip.c_str(), &(sa.sin_addr)) == 1) ||
           (inet_pton(AF_INET6, ip.c_str(), &(sa6.sin6_addr)) == 1);
}

std::string serialize_snapshot_versionstamp(Versionstamp snapshot_versionstamp) {
    return snapshot_versionstamp.to_string();
}

// Parse hex-encoded versionstamp from snapshot ID.
// The snapshot ID is expected to be a 20-character hex string representing 10 bytes.
static bool parse_snapshot_versionstamp(std::string_view snapshot_id, Versionstamp* versionstamp) {
    if (snapshot_id.size() != 20) {
        return false;
    }

    std::array<uint8_t, 10> versionstamp_data;
    for (size_t i = 0; i < 10; ++i) {
        const char* hex_chars = snapshot_id.data() + (i * 2);

        // Convert two hex digits to one byte more efficiently
        uint8_t high_nibble = 0, low_nibble = 0;

        // Parse high nibble
        if (hex_chars[0] >= '0' && hex_chars[0] <= '9') {
            high_nibble = hex_chars[0] - '0';
        } else if (hex_chars[0] >= 'a' && hex_chars[0] <= 'f') {
            high_nibble = hex_chars[0] - 'a' + 10;
        } else if (hex_chars[0] >= 'A' && hex_chars[0] <= 'F') {
            high_nibble = hex_chars[0] - 'A' + 10;
        } else {
            return false;
        }

        // Parse low nibble
        if (hex_chars[1] >= '0' && hex_chars[1] <= '9') {
            low_nibble = hex_chars[1] - '0';
        } else if (hex_chars[1] >= 'a' && hex_chars[1] <= 'f') {
            low_nibble = hex_chars[1] - 'a' + 10;
        } else if (hex_chars[1] >= 'A' && hex_chars[1] <= 'F') {
            low_nibble = hex_chars[1] - 'A' + 10;
        } else {
            return false;
        }

        versionstamp_data[i] = (high_nibble << 4) | low_nibble;
    }

    *versionstamp = Versionstamp(versionstamp_data);
    return true;
}

static bool decrypt_object_store_info_ak_sk(doris::cloud::ObjectStoreInfoPB* obj_info) {
    if (!obj_info->has_encryption_info()) {
        return true;
    }

    auto& ak = obj_info->ak();
    auto& sk = obj_info->sk();
    auto& encryption_info = obj_info->encryption_info();
    doris::cloud::AkSkPair plain_ak_sk_pair;
    if (int ret = decrypt_ak_sk_helper(ak, sk, encryption_info, &plain_ak_sk_pair); ret != 0) {
        LOG(WARNING) << "failed to decrypt object store info ak/sk, err=" << ret;
        return false;
    }

    obj_info->clear_encryption_info(); // avoid leaking encryption info
    obj_info->set_ak(std::move(plain_ak_sk_pair.first));
    obj_info->set_sk(std::move(plain_ak_sk_pair.second));
    return true;
}

void SnapshotManager::begin_snapshot(std::string_view instance_id,
                                     const doris::cloud::BeginSnapshotRequest& request,
                                     doris::cloud::BeginSnapshotResponse* response) {
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

    // Validate request IP format if provided
    if (request.has_request_ip() && !request.request_ip().empty()) {
        if (!is_valid_ip_address(request.request_ip())) {
            status->set_code(MetaServiceCode::INVALID_ARGUMENT);
            status->set_msg("invalid request IP address format");
            return;
        }
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
    std::string key = doris::cloud::instance_key({instance_id});
    LOG(INFO) << "get instance_key=" << hex(key);

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

    if (instance.enable_storage_vault()) {
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg("snapshot not support for storage vault instance");
        return;
    }

    if (instance.snapshot_switch_status() != SnapshotSwitchStatus::SNAPSHOT_SWITCH_ON) {
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg("failed to begin snapshot, because the snapshot feature is disabled");
        return;
    }

    DCHECK(instance.obj_info_size() > 0) << "instance must have at least one obj_info";

    // Choose the last store obj as the storage to save the snapshot images.
    ObjectStoreInfoPB obj_info(instance.obj_info(instance.obj_info_size() - 1));
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
    snapshot_pb.set_resource_id(obj_info.id());

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

    std::string snapshot_id = serialize_snapshot_versionstamp(versionstamp);
    response->set_image_url("/snapshot/" + snapshot_id + "/");
    response->set_snapshot_id(snapshot_id);
    response->mutable_obj_info()->Swap(&obj_info);
}

void SnapshotManager::commit_snapshot(std::string_view instance_id,
                                      const doris::cloud::CommitSnapshotRequest& request,
                                      doris::cloud::CommitSnapshotResponse* response) {
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

    // Validate IP if provided
    if (request.has_request_ip() && !request.request_ip().empty()) {
        if (!is_valid_ip_address(request.request_ip())) {
            status->set_code(MetaServiceCode::INVALID_ARGUMENT);
            status->set_msg("invalid request IP address format");
            return;
        }
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
                                     const doris::cloud::AbortSnapshotRequest& request,
                                     doris::cloud::AbortSnapshotResponse* response) {
    auto* status = response->mutable_status();
    status->set_code(MetaServiceCode::OK);
    status->set_msg("OK");

    // Check all required fields in AbortSnapshotRequest
    if (!request.has_snapshot_id() || request.snapshot_id().empty()) {
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg("snapshot_id not set");
        return;
    }

    // Validate IP if provided
    if (request.has_request_ip() && !request.request_ip().empty()) {
        if (!is_valid_ip_address(request.request_ip())) {
            status->set_code(MetaServiceCode::INVALID_ARGUMENT);
            status->set_msg("invalid request IP address format");
            return;
        }
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

void SnapshotManager::drop_snapshot(std::string_view instance_id,
                                    const doris::cloud::DropSnapshotRequest& request,
                                    doris::cloud::DropSnapshotResponse* response) {
    auto* status = response->mutable_status();
    status->set_code(MetaServiceCode::OK);
    status->set_msg("OK");

    if (!request.has_snapshot_id() || request.snapshot_id().empty()) {
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg("snapshot_id not set");
        return;
    }

    std::string snapshot_id = request.snapshot_id();

    // Validate IP if provided
    if (request.has_request_ip() && !request.request_ip().empty()) {
        if (!is_valid_ip_address(request.request_ip())) {
            status->set_code(MetaServiceCode::INVALID_ARGUMENT);
            status->set_msg("invalid request IP address format");
            return;
        }
    }

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
              << " version=" << snapshot_id;

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

    // Check if snapshot can be dropped (should be in final state)
    if (snapshot_pb.status() != SnapshotStatus::SNAPSHOT_NORMAL &&
        snapshot_pb.status() != SnapshotStatus::SNAPSHOT_ABORTED) {
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg("cannot drop snapshot that is not in final state (NORMAL or ABORTED)");
        return;
    }

    // Delete the snapshot
    txn->remove(snapshot_key);

    LOG_INFO("drop snapshot completed")
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
                                    const doris::cloud::ListSnapshotRequest& request,
                                    doris::cloud::ListSnapshotResponse* response) {
    auto* status = response->mutable_status();
    status->set_code(MetaServiceCode::OK);
    status->set_msg("OK");

    std::string required_snapshot_id =
            request.has_required_snapshot_id() ? request.required_snapshot_id() : "";
    bool include_aborted = request.has_include_aborted() ? request.include_aborted() : false;

    if (request.has_request_ip() && !request.request_ip().empty()) {
        if (!is_valid_ip_address(request.request_ip())) {
            status->set_code(MetaServiceCode::INVALID_ARGUMENT);
            status->set_msg("invalid request IP address format");
            return;
        }
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        status->set_code(cast_as<ErrCategory::CREATE>(err));
        status->set_msg("failed to create txn");
        LOG(WARNING) << status->msg() << " err=" << err;
        return;
    }

    std::string snapshot_full_key = versioned::snapshot_full_key({instance_id});

    std::map<std::string, SnapshotInfoPB> snapshots_map;

    if (!required_snapshot_id.empty()) {
        // Optimize for specific snapshot ID query - directly get the snapshot
        Versionstamp snapshot_versionstamp;
        if (!parse_snapshot_versionstamp(required_snapshot_id, &snapshot_versionstamp)) {
            status->set_code(MetaServiceCode::INVALID_ARGUMENT);
            status->set_msg("invalid snapshot_id format");
            return;
        }

        std::string snapshot_key = encode_versioned_key(snapshot_full_key, snapshot_versionstamp);
        std::string snapshot_val;
        err = txn->get(snapshot_key, &snapshot_val);

        if (err != TxnErrorCode::TXN_OK) {
            if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
                // Snapshot not found, return empty result
                LOG(INFO) << "snapshot not found, snapshot_id=" << required_snapshot_id;
                return;
            } else {
                status->set_code(cast_as<ErrCategory::READ>(err));
                status->set_msg(fmt::format("failed to get snapshot, snapshot_id={}, err={}",
                                            required_snapshot_id, err));
                LOG(WARNING) << status->msg();
                return;
            }
        }

        SnapshotPB snapshot_pb;
        if (!snapshot_pb.ParseFromString(snapshot_val)) {
            status->set_code(MetaServiceCode::PROTOBUF_PARSE_ERR);
            status->set_msg("failed to parse SnapshotPB");
            return;
        }
        LOG_INFO("get snapshot versioned key")
                .tag("snapshot_key", hex(snapshot_full_key))
                .tag("instance_id", instance_id);

        // Check if we should include aborted snapshots
        if (!include_aborted && snapshot_pb.status() == SnapshotStatus::SNAPSHOT_ABORTED) {
            // Skip aborted snapshot, return empty result
            return;
        }

        SnapshotInfoPB snapshot_info;
        snapshot_info.set_snapshot_id(required_snapshot_id);
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

        snapshots_map[required_snapshot_id] = std::move(snapshot_info);
    } else {
        // No specific snapshot ID requested - scan all snapshots
        // Use full_range_get to get ALL versions of the snapshot key
        std::string begin_key = encode_versioned_key(snapshot_full_key, Versionstamp::min());
        std::string end_key = encode_versioned_key(snapshot_full_key, Versionstamp::max());

        FullRangeGetOptions opts;
        opts.reverse = true; // Get the latest version first
        opts.begin_key_selector = RangeKeySelector::FIRST_GREATER_OR_EQUAL;
        opts.end_key_selector = RangeKeySelector::FIRST_GREATER_THAN;

        auto iter = txn->full_range_get(begin_key, end_key, std::move(opts));

        if (!iter->is_valid() && iter->error_code() != TxnErrorCode::TXN_OK) {
            status->set_code(cast_as<ErrCategory::READ>(iter->error_code()));
            status->set_msg(fmt::format("failed to scan snapshots, err={}", iter->error_code()));
            LOG(WARNING) << status->msg();
            return;
        }

        while (iter->has_next()) {
            auto element = iter->next();
            if (!element.has_value()) {
                if (iter->error_code() != TxnErrorCode::TXN_OK) {
                    status->set_code(cast_as<ErrCategory::READ>(iter->error_code()));
                    status->set_msg(
                            fmt::format("failed to scan snapshots, err={}", iter->error_code()));
                    LOG(WARNING) << status->msg();
                    return;
                }
                break;
            }

            auto [versioned_key, snapshot_val] = element.value();

            // Extract version stamp from the versioned key
            Versionstamp version_stamp;
            std::string_view key_copy = versioned_key;
            if (!decode_versioned_key(&key_copy, &version_stamp)) {
                LOG(WARNING) << "failed to decode versioned key=" << hex(versioned_key);
                continue;
            }

            LOG_INFO("get snapshot versioned key")
                    .tag("snapshot_key", hex(snapshot_full_key))
                    .tag("instance_id", version_stamp.to_string());

            SnapshotPB snapshot_pb;
            if (!snapshot_pb.ParseFromString(std::string(snapshot_val))) {
                LOG(WARNING) << "failed to parse SnapshotPB for snapshot_id="
                             << version_stamp.to_string();
                continue;
            }

            if (!include_aborted && snapshot_pb.status() == SnapshotStatus::SNAPSHOT_ABORTED) {
                continue;
            }

            std::string snapshot_id = version_stamp.to_string();
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

            snapshots_map[snapshot_id] = std::move(snapshot_info);
        }
    }

    for (auto& [snapshot_id, snapshot_info] : snapshots_map) {
        *response->add_snapshots() = std::move(snapshot_info);
    }

    LOG_INFO("list snapshots completed")
            .tag("instance_id", instance_id)
            .tag("snapshots_count", response->snapshots_size())
            .tag("required_snapshot_id", required_snapshot_id)
            .tag("include_aborted", include_aborted);
}

void SnapshotManager::clone_instance(const doris::cloud::CloneInstanceRequest& request,
                                     doris::cloud::CloneInstanceResponse* response) {
    response->mutable_status()->set_code(MetaServiceCode::UNDEFINED_ERR);
    response->mutable_status()->set_msg("Not implemented");
}

std::pair<MetaServiceCode, std::string> SnapshotManager::set_multi_version_status(
        std::string_view instance_id, std::string_view cloud_unique_id,
        doris::cloud::MultiVersionStatus multi_version_status) {
    LOG_INFO("set_multi_version_status")
            .tag("cloud_unique_id", cloud_unique_id)
            .tag("instance_id", instance_id)
            .tag("multi_version_status", multi_version_status);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return {cast_as<ErrCategory::CREATE>(err), "failed to create txn"};
    }

    doris::cloud::InstanceKeyInfo key_info {std::string(instance_id)};
    std::string instance_key_str;
    doris::cloud::instance_key(key_info, &instance_key_str);

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

    instance_info.set_multi_version_status(multi_version_status);

    std::string updated_instance_val = instance_info.SerializeAsString();
    if (updated_instance_val.empty()) {
        return {MetaServiceCode::PROTOBUF_SERIALIZE_ERR, "failed to serialize instance info"};
    }

    txn->put(instance_key_str, updated_instance_val);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        return {cast_as<ErrCategory::COMMIT>(err), "failed to commit txn"};
    }

    // Asynchronously notify ResourceManager to refresh instance cache
    notify_refresh_instance(txn_kv_, std::string(instance_id), nullptr);

    LOG_INFO("set_multi_version_status completed")
            .tag("instance_id", instance_id)
            .tag("multi_version_status", multi_version_status);

    return {MetaServiceCode::OK, "success"};
}

} // namespace selectdb
