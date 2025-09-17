#include "snapshot_manager.h"

#include <arpa/inet.h>
#include <gen_cpp/cloud.pb.h>
#include <netinet/in.h>

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
using doris::cloud::SnapshotStatus;
using doris::cloud::SnapshotType;
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

    // Validate TTL must be positive
    if (!request.has_ttl_seconds() || request.ttl_seconds() <= 0) {
        status->set_code(MetaServiceCode::INVALID_ARGUMENT);
        status->set_msg("ttl_seconds must be positive");
        return;
    }

    // Validate snapshot label is not empty
    if (!request.has_snapshot_label() || request.snapshot_label().empty()) {
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

    std::string cloud_unique_id = request.cloud_unique_id();
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
    response->mutable_status()->set_code(MetaServiceCode::UNDEFINED_ERR);
    response->mutable_status()->set_msg("Not implemented");
}

void SnapshotManager::list_snapshot(std::string_view instance_id,
                                    const doris::cloud::ListSnapshotRequest& request,
                                    doris::cloud::ListSnapshotResponse* response) {
    response->mutable_status()->set_code(MetaServiceCode::UNDEFINED_ERR);
    response->mutable_status()->set_msg("Not implemented");
}

void SnapshotManager::clone_instance(const doris::cloud::CloneInstanceRequest& request,
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
