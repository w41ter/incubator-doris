// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <fmt/format.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "common/defer.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "enterprise/snapshot/snapshot_manager.h"
#include "meta-service/meta_service.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
#include "mock_resource_manager.h"
#include "snapshot/snapshot_manager.h"

namespace config = doris::cloud::config;

int main(int argc, char** argv) {
    const std::string conf_file = "doris_cloud.conf";
    if (!doris::cloud::config::init(conf_file.c_str(), true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }

    config::enable_retry_txn_conflict = false;
    config::enable_txn_store_retry = true;
    config::txn_store_retry_base_intervals_ms = 1;
    config::txn_store_retry_times = 20;
    config::enable_check_instance_id = false;

    if (!doris::cloud::init_glog("enterprise_meta_service_snapshot_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace doris::cloud {

std::unique_ptr<MetaServiceProxy> get_meta_service(bool mock_resource_mgr) {
    int ret = 0;
    // MemKv
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    if (txn_kv != nullptr) {
        ret = txn_kv->init();
        [&] { ASSERT_EQ(ret, 0); }();
    }
    [&] { ASSERT_NE(txn_kv.get(), nullptr); }();

    // FdbKv
    //     config::fdb_cluster_file_path = "fdb.cluster";
    //     static auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<FdbTxnKv>());
    //     static std::atomic<bool> init {false};
    //     bool tmp = false;
    //     if (init.compare_exchange_strong(tmp, true)) {
    //         int ret = txn_kv->init();
    //         [&] { ASSERT_EQ(ret, 0); ASSERT_NE(txn_kv.get(), nullptr); }();
    //     }

    std::unique_ptr<Transaction> txn;
    EXPECT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->remove("\x00", "\xfe"); // This is dangerous if the fdb is not correctly set
    EXPECT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    auto rs = mock_resource_mgr ? std::make_shared<MockResourceManager>(txn_kv)
                                : std::make_shared<ResourceManager>(txn_kv);
    auto rl = std::make_shared<RateLimiter>();
    auto snapshot = std::make_shared<selectdb::SnapshotManager>(txn_kv);
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs, rl, snapshot);
    return std::make_unique<MetaServiceProxy>(std::move(meta_service));
}

int get_snapshot(std::unique_ptr<MetaServiceProxy>& meta_service, std::string& snapshot_id,
                 SnapshotPB& snapshot_pb) {
    Versionstamp snapshot_versionstamp;
    if (!SnapshotManager::parse_snapshot_versionstamp(snapshot_id, &snapshot_versionstamp)) {
        return -1;
    }
    std::unique_ptr<Transaction> txn;
    if (meta_service->txn_kv()->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        return -2;
    }
    std::string snapshot_key = encode_versioned_key(
            versioned::snapshot_full_key({"test_instance"}), snapshot_versionstamp);
    std::string snapshot_val;
    if (txn->get(snapshot_key, &snapshot_val) != TxnErrorCode::TXN_OK) {
        return -3;
    }
    if (!snapshot_pb.ParseFromString(snapshot_val)) {
        return -4;
    }
    return 0;
}

TEST(MetaServiceSnapshotTest, BeginSnapshotTest) {
    auto meta_service = get_meta_service(true);
    const char* const cloud_unique_id = "test_cloud_unique_id";

    // Setup SyncPoint for encryption
    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });
    sp->set_call_back("decrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* key = try_any_cast<std::string*>(args[0]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* ret = try_any_cast<int*>(args[1]);
        *ret = 0;
    });

    // Cleanup SyncPoint when test finishes
    DORIS_CLOUD_DEFER {
        sp->disable_processing();
        sp->clear_all_call_backs();
    };

    // Create test instance first
    {
        brpc::Controller cntl;
        CreateInstanceRequest req;
        req.set_instance_id("test_instance");
        req.set_user_id("test_user");
        req.set_name("test_name");
        ObjectStoreInfoPB obj;
        obj.set_ak("123");
        obj.set_sk("321");
        obj.set_bucket("456");
        obj.set_prefix("654");
        obj.set_endpoint("789");
        obj.set_region("987");
        obj.set_external_endpoint("888");
        obj.set_provider(ObjectStoreInfoPB::BOS);
        req.mutable_obj_info()->CopyFrom(obj);

        CreateInstanceResponse res;
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Enable multi version for the test instance
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string instance_key_str = instance_key("test_instance");
        std::string instance_value;
        ASSERT_EQ(txn->get(instance_key_str, &instance_value), TxnErrorCode::TXN_OK);
        InstanceInfoPB instance_info;
        ASSERT_TRUE(instance_info.ParseFromString(instance_value));
        instance_info.set_snapshot_switch_status(SnapshotSwitchStatus::SNAPSHOT_SWITCH_ON);
        txn->put(instance_key_str, instance_info.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // test invalid argument - empty cloud_unique_id
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // test normal begin snapshot
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_auto_snapshot(true);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_FALSE(res.image_url().empty());
        ASSERT_FALSE(res.snapshot_id().empty());
        ASSERT_TRUE(res.image_url().find("/snapshot/") != std::string::npos);
    }

    // test begin snapshot with custom parameters
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(1800);
        req.set_auto_snapshot(false);
        req.set_ttl_seconds(14400);
        req.set_snapshot_label("custom_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_FALSE(res.image_url().empty());
        ASSERT_FALSE(res.snapshot_id().empty());
    }

    // test invalid timeout_seconds - zero
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(0);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // test invalid timeout_seconds - negative
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(-100);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // test invalid ttl_seconds - zero
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(0);
        req.set_snapshot_label("test_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // test invalid ttl_seconds - negative
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(-500);
        req.set_snapshot_label("test_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // test empty snapshot_label
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // test valid IPv4 address
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_snapshot");
        req.set_request_ip("192.168.1.100");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // test valid IPv6 address
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_snapshot");
        req.set_request_ip("2001:db8::1");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // test invalid IP address format
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_snapshot");
        req.set_request_ip("invalid.ip.address");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // test invalid IP address - out of range
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_snapshot");
        req.set_request_ip("256.256.256.256");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // test empty IP address (should pass - IP is optional)
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_snapshot");
        req.set_request_ip("");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
}

TEST(MetaServiceSnapshotTest, UpdateSnapshotTest) {
    auto meta_service = get_meta_service(true);
    const char* const cloud_unique_id = "test_cloud_unique_id";

    // Setup SyncPoint for encryption
    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });
    sp->set_call_back("decrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* key = try_any_cast<std::string*>(args[0]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* ret = try_any_cast<int*>(args[1]);
        *ret = 0;
    });

    // Cleanup SyncPoint when test finishes
    DORIS_CLOUD_DEFER {
        sp->disable_processing();
        sp->clear_all_call_backs();
    };

    // Create test instance first
    {
        brpc::Controller cntl;
        CreateInstanceRequest req;
        req.set_instance_id("test_instance");
        req.set_user_id("test_user");
        req.set_name("test_name");
        ObjectStoreInfoPB obj;
        obj.set_ak("123");
        obj.set_sk("321");
        obj.set_bucket("456");
        obj.set_prefix("654");
        obj.set_endpoint("789");
        obj.set_region("987");
        obj.set_external_endpoint("888");
        obj.set_provider(ObjectStoreInfoPB::BOS);
        req.mutable_obj_info()->CopyFrom(obj);

        CreateInstanceResponse res;
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Enable multi version for the test instance
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string instance_key_str = instance_key("test_instance");
        std::string instance_value;
        ASSERT_EQ(txn->get(instance_key_str, &instance_value), TxnErrorCode::TXN_OK);
        InstanceInfoPB instance_info;
        ASSERT_TRUE(instance_info.ParseFromString(instance_value));
        instance_info.set_snapshot_switch_status(SnapshotSwitchStatus::SNAPSHOT_SWITCH_ON);
        txn->put(instance_key_str, instance_info.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // begin snapshot
    std::string snapshot_id;
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_auto_snapshot(true);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_FALSE(res.image_url().empty());
        ASSERT_FALSE(res.snapshot_id().empty());
        ASSERT_TRUE(res.image_url().find("/snapshot/") != std::string::npos);
        snapshot_id = res.snapshot_id();
    }

    // update snapshot with invalid argument - empty cloud unique id
    {
        brpc::Controller cntl;
        UpdateSnapshotRequest req;
        req.set_snapshot_id(snapshot_id);
        req.set_upload_file("image.100");
        UpdateSnapshotResponse res;
        meta_service->update_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // update snapshot with invalid argument - empty upload_id
    {
        brpc::Controller cntl;
        UpdateSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        req.set_upload_file("image.100");
        UpdateSnapshotResponse res;
        meta_service->update_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // update snapshot with invalid argument - empty upload_file
    {
        brpc::Controller cntl;
        UpdateSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        req.set_upload_id("test_upload_id");
        UpdateSnapshotResponse res;
        meta_service->update_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // abnormal update snapshot - non exist snapshot id
    {
        brpc::Controller cntl;
        UpdateSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id("non_existent_snapshot_id_12345");
        req.set_upload_file("image.100");
        req.set_upload_id("test_upload_id_1");
        UpdateSnapshotResponse res;
        meta_service->update_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // normal update snapshot
    {
        brpc::Controller cntl;
        UpdateSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        req.set_upload_file("image.100");
        req.set_upload_id("test_upload_id_1");
        UpdateSnapshotResponse res;
        meta_service->update_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        SnapshotPB snapshot_pb;
        ASSERT_EQ(get_snapshot(meta_service, snapshot_id, snapshot_pb), 0);
        ASSERT_EQ(snapshot_pb.upload_file(), "image.100");
        ASSERT_EQ(snapshot_pb.upload_id(), "test_upload_id_1");
    }
}

TEST(MetaServiceSnapshotTest, CommitSnapshotTest) {
    auto meta_service = get_meta_service(true);
    const char* const cloud_unique_id = "test_cloud_unique_id";

    // Setup SyncPoint for encryption
    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });
    sp->set_call_back("decrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* key = try_any_cast<std::string*>(args[0]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* ret = try_any_cast<int*>(args[1]);
        *ret = 0;
    });

    // Cleanup SyncPoint when test finishes
    DORIS_CLOUD_DEFER {
        sp->disable_processing();
        sp->clear_all_call_backs();
    };

    // Create test instance first
    {
        brpc::Controller cntl;
        CreateInstanceRequest req;
        req.set_instance_id("test_instance");
        req.set_user_id("test_user");
        req.set_name("test_name");
        ObjectStoreInfoPB obj;
        obj.set_ak("123");
        obj.set_sk("321");
        obj.set_bucket("456");
        obj.set_prefix("654");
        obj.set_endpoint("789");
        obj.set_region("987");
        obj.set_external_endpoint("888");
        obj.set_provider(ObjectStoreInfoPB::BOS);
        req.mutable_obj_info()->CopyFrom(obj);

        CreateInstanceResponse res;
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Enable multi version for the test instance
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string instance_key_str = instance_key("test_instance");
        std::string instance_value;
        ASSERT_EQ(txn->get(instance_key_str, &instance_value), TxnErrorCode::TXN_OK);
        InstanceInfoPB instance_info;
        ASSERT_TRUE(instance_info.ParseFromString(instance_value));
        instance_info.set_snapshot_switch_status(SnapshotSwitchStatus::SNAPSHOT_SWITCH_ON);
        txn->put(instance_key_str, instance_info.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Begin a snapshot first
    std::string snapshot_id;
    std::string image_url;
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_auto_snapshot(true);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_snapshot_for_commit");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_FALSE(res.image_url().empty());
        ASSERT_FALSE(res.snapshot_id().empty());

        snapshot_id = res.snapshot_id();
        image_url = res.image_url();
    }

    // Test missing cloud_unique_id
    {
        brpc::Controller cntl;
        CommitSnapshotRequest req;
        req.set_snapshot_id(snapshot_id);
        req.set_image_url(image_url);
        req.set_last_journal_id(12345);
        CommitSnapshotResponse res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // Test missing snapshot_id
    {
        brpc::Controller cntl;
        CommitSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_image_url(image_url);
        req.set_last_journal_id(12345);
        CommitSnapshotResponse res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // Test missing image_url
    {
        brpc::Controller cntl;
        CommitSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        req.set_last_journal_id(12345);
        CommitSnapshotResponse res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // Test missing last_journal_id
    {
        brpc::Controller cntl;
        CommitSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        req.set_image_url(image_url);
        CommitSnapshotResponse res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // update snapshot
    {
        brpc::Controller cntl;
        UpdateSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        req.set_upload_file("image.100");
        req.set_upload_id("test_upload_id_1");
        UpdateSnapshotResponse res;
        meta_service->update_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        SnapshotPB snapshot_pb;
        ASSERT_EQ(get_snapshot(meta_service, snapshot_id, snapshot_pb), 0);
        ASSERT_EQ(snapshot_pb.upload_file(), "image.100");
        ASSERT_EQ(snapshot_pb.upload_id(), "test_upload_id_1");
    }

    // Test successful commit
    {
        brpc::Controller cntl;
        CommitSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        req.set_image_url(image_url);
        req.set_last_journal_id(12345);
        CommitSnapshotResponse res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        SnapshotPB snapshot_pb;
        ASSERT_EQ(get_snapshot(meta_service, snapshot_id, snapshot_pb), 0);
        ASSERT_EQ(snapshot_pb.upload_file(), "");
        ASSERT_EQ(snapshot_pb.upload_id(), "");
    }

    // Test committing non-existent snapshot
    {
        brpc::Controller cntl;
        CommitSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id("non_existent_snapshot_id_12345");
        req.set_image_url("/snapshot/non_existent/");
        req.set_last_journal_id(12345);
        CommitSnapshotResponse res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // Test with valid IP address
    {
        brpc::Controller cntl;
        BeginSnapshotRequest begin_req;
        begin_req.set_cloud_unique_id(cloud_unique_id);
        begin_req.set_timeout_seconds(3600);
        begin_req.set_auto_snapshot(true);
        begin_req.set_ttl_seconds(7200);
        begin_req.set_snapshot_label("test_snapshot_with_ip");
        BeginSnapshotResponse begin_res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &begin_req, &begin_res, nullptr);
        ASSERT_EQ(begin_res.status().code(), MetaServiceCode::OK);

        CommitSnapshotRequest commit_req;
        commit_req.set_cloud_unique_id(cloud_unique_id);
        commit_req.set_snapshot_id(begin_res.snapshot_id());
        commit_req.set_image_url(begin_res.image_url());
        commit_req.set_last_journal_id(67890);
        commit_req.set_request_ip("192.168.1.100");
        CommitSnapshotResponse commit_res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &commit_req, &commit_res, nullptr);
        ASSERT_EQ(commit_res.status().code(), MetaServiceCode::OK);
    }

    // Test with invalid IP address
    {
        brpc::Controller cntl;
        CommitSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        req.set_image_url(image_url);
        req.set_last_journal_id(12345);
        req.set_request_ip("invalid.ip.format");
        CommitSnapshotResponse res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // Test commit idempotency - committing the same snapshot multiple times should succeed
    {
        // Create a new snapshot for this test
        std::string new_snapshot_id;
        std::string new_image_url;
        {
            brpc::Controller cntl;
            BeginSnapshotRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_timeout_seconds(3600);
            req.set_auto_snapshot(true);
            req.set_ttl_seconds(7200);
            req.set_snapshot_label("test_commit_idempotency");
            BeginSnapshotResponse res;
            meta_service->begin_snapshot(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                    nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            new_snapshot_id = res.snapshot_id();
            new_image_url = res.image_url();
        }

        // First commit - should succeed
        CommitSnapshotResponse first_res;
        {
            brpc::Controller cntl;
            CommitSnapshotRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_snapshot_id(new_snapshot_id);
            req.set_image_url(new_image_url);
            req.set_last_journal_id(54321);
            meta_service->commit_snapshot(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &first_res,
                    nullptr);
            ASSERT_EQ(first_res.status().code(), MetaServiceCode::OK);
        }

        // Second commit (retry) - should also succeed due to idempotency
        CommitSnapshotResponse second_res;
        {
            brpc::Controller cntl;
            CommitSnapshotRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_snapshot_id(new_snapshot_id);
            req.set_image_url(new_image_url);
            req.set_last_journal_id(54321);
            meta_service->commit_snapshot(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &second_res,
                    nullptr);
            ASSERT_EQ(second_res.status().code(), MetaServiceCode::OK);
        }

        // Third commit (another retry) - should still succeed
        CommitSnapshotResponse third_res;
        {
            brpc::Controller cntl;
            CommitSnapshotRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_snapshot_id(new_snapshot_id);
            req.set_image_url(new_image_url);
            req.set_last_journal_id(54321);
            meta_service->commit_snapshot(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &third_res,
                    nullptr);
            ASSERT_EQ(third_res.status().code(), MetaServiceCode::OK);
        }

        // Verify all three commit responses are identical
        ASSERT_EQ(first_res.status().code(), second_res.status().code());
        ASSERT_EQ(first_res.status().code(), third_res.status().code());
        ASSERT_EQ(first_res.status().msg(), second_res.status().msg());
        ASSERT_EQ(first_res.status().msg(), third_res.status().msg());

        // All responses should have the same structure (empty in this case)
        ASSERT_EQ(first_res.SerializeAsString(), second_res.SerializeAsString());
        ASSERT_EQ(first_res.SerializeAsString(), third_res.SerializeAsString());
    }

    // Test commit aborted snapshot should fail
    {
        // Create and abort a snapshot
        std::string aborted_snapshot_id;
        std::string aborted_image_url;
        {
            brpc::Controller cntl;
            BeginSnapshotRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_timeout_seconds(3600);
            req.set_auto_snapshot(true);
            req.set_ttl_seconds(7200);
            req.set_snapshot_label("test_commit_aborted");
            BeginSnapshotResponse res;
            meta_service->begin_snapshot(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                    nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            aborted_snapshot_id = res.snapshot_id();
            aborted_image_url = res.image_url();
        }

        // Abort the snapshot
        {
            brpc::Controller cntl;
            AbortSnapshotRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_snapshot_id(aborted_snapshot_id);
            req.set_reason("Test abort before commit");
            AbortSnapshotResponse res;
            meta_service->abort_snapshot(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                    nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        // Try to commit the aborted snapshot - should fail
        {
            brpc::Controller cntl;
            CommitSnapshotRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_snapshot_id(aborted_snapshot_id);
            req.set_image_url(aborted_image_url);
            req.set_last_journal_id(99999);
            CommitSnapshotResponse res;
            meta_service->commit_snapshot(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                    nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
            ASSERT_TRUE(res.status().msg().find("cannot commit aborted snapshot") !=
                        std::string::npos);
        }
    }
}

TEST(MetaServiceSnapshotTest, AbortSnapshotTest) {
    auto meta_service = get_meta_service(true);
    const char* const cloud_unique_id = "test_cloud_unique_id";

    // Setup SyncPoint for encryption
    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });
    sp->set_call_back("decrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* key = try_any_cast<std::string*>(args[0]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* ret = try_any_cast<int*>(args[1]);
        *ret = 0;
    });

    // Cleanup SyncPoint when test finishes
    DORIS_CLOUD_DEFER {
        sp->disable_processing();
        sp->clear_all_call_backs();
    };

    // Create test instance first
    {
        brpc::Controller cntl;
        CreateInstanceRequest req;
        req.set_instance_id("test_instance");
        req.set_user_id("test_user");
        req.set_name("test_name");
        ObjectStoreInfoPB obj;
        obj.set_ak("123");
        obj.set_sk("321");
        obj.set_bucket("456");
        obj.set_prefix("654");
        obj.set_endpoint("789");
        obj.set_region("987");
        obj.set_external_endpoint("888");
        obj.set_provider(ObjectStoreInfoPB::BOS);
        req.mutable_obj_info()->CopyFrom(obj);

        CreateInstanceResponse res;
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Enable multi version for the test instance
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string instance_key_str = instance_key("test_instance");
        std::string instance_value;
        ASSERT_EQ(txn->get(instance_key_str, &instance_value), TxnErrorCode::TXN_OK);
        InstanceInfoPB instance_info;
        ASSERT_TRUE(instance_info.ParseFromString(instance_value));
        instance_info.set_snapshot_switch_status(SnapshotSwitchStatus::SNAPSHOT_SWITCH_ON);
        txn->put(instance_key_str, instance_info.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Create a snapshot first to test abort
    std::string snapshot_id;
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_auto_snapshot(true);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_abort_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        snapshot_id = res.snapshot_id();
        ASSERT_FALSE(snapshot_id.empty());
    }

    // Test invalid argument - empty cloud_unique_id
    {
        brpc::Controller cntl;
        AbortSnapshotRequest req;
        req.set_snapshot_id(snapshot_id);
        req.set_reason("Test abort");
        AbortSnapshotResponse res;
        meta_service->abort_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find("cloud_unique_id not set") != std::string::npos);
    }

    // Test invalid argument - empty snapshot_id
    {
        brpc::Controller cntl;
        AbortSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_reason("Test abort");
        AbortSnapshotResponse res;
        meta_service->abort_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find("snapshot_id not set") != std::string::npos);
    }

    // Test invalid argument - invalid IP address
    {
        brpc::Controller cntl;
        AbortSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        req.set_reason("Test abort");
        req.set_request_ip("invalid.ip.format");
        AbortSnapshotResponse res;
        meta_service->abort_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find("invalid request IP address format") !=
                    std::string::npos);
    }

    // Test abort non-existent snapshot
    {
        brpc::Controller cntl;
        AbortSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id("12345678900000000000"); // Non-existent snapshot ID
        req.set_reason("Test abort");
        AbortSnapshotResponse res;
        meta_service->abort_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_ID_NOT_FOUND);
    }

    // Test successful abort
    {
        brpc::Controller cntl;
        AbortSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        req.set_reason("Test abort reason");
        AbortSnapshotResponse res;
        meta_service->abort_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
    }

    // Test abort already aborted snapshot
    {
        brpc::Controller cntl;
        AbortSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        req.set_reason("Test abort again");
        AbortSnapshotResponse res;
        meta_service->abort_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_TRUE(res.status().msg().find("snapshot is already aborted") != std::string::npos);
    }

    // Test abort with valid IP addresses
    {
        // First create another snapshot to test IP validation
        std::string new_snapshot_id;
        {
            brpc::Controller cntl;
            BeginSnapshotRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_timeout_seconds(3600);
            req.set_auto_snapshot(false);
            req.set_ttl_seconds(7200);
            req.set_snapshot_label("test_abort_snapshot_ip");
            BeginSnapshotResponse res;
            meta_service->begin_snapshot(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                    nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            new_snapshot_id = res.snapshot_id();
        }

        // Test with IPv4
        {
            brpc::Controller cntl;
            AbortSnapshotRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_snapshot_id(new_snapshot_id);
            req.set_reason("Test abort with IPv4");
            req.set_request_ip("192.168.1.100");
            AbortSnapshotResponse res;
            meta_service->abort_snapshot(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                    nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }
    }

    // Test abort with default reason (no reason provided)
    {
        // Create another snapshot
        std::string another_snapshot_id;
        {
            brpc::Controller cntl;
            BeginSnapshotRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_timeout_seconds(3600);
            req.set_auto_snapshot(false);
            req.set_ttl_seconds(7200);
            req.set_snapshot_label("test_abort_snapshot_no_reason");
            BeginSnapshotResponse res;
            meta_service->begin_snapshot(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                    nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            another_snapshot_id = res.snapshot_id();
        }

        // Test abort without reason
        {
            brpc::Controller cntl;
            AbortSnapshotRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_snapshot_id(another_snapshot_id);
            // Don't set reason - should use default
            AbortSnapshotResponse res;
            meta_service->abort_snapshot(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                    nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }
    }

    // Test abort a committed snapshot (should fail)
    {
        // First create and commit a snapshot
        std::string committed_snapshot_id;
        {
            brpc::Controller cntl;
            BeginSnapshotRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_timeout_seconds(3600);
            req.set_auto_snapshot(false);
            req.set_ttl_seconds(7200);
            req.set_snapshot_label("test_commit_then_abort");
            BeginSnapshotResponse res;
            meta_service->begin_snapshot(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                    nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            committed_snapshot_id = res.snapshot_id();
        }

        // Commit it
        {
            brpc::Controller cntl;
            CommitSnapshotRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_snapshot_id(committed_snapshot_id);
            req.set_image_url("s3://bucket/path/image");
            req.set_last_journal_id(12345);
            CommitSnapshotResponse res;
            meta_service->commit_snapshot(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                    nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }

        // Try to abort it (should fail)
        {
            brpc::Controller cntl;
            AbortSnapshotRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_snapshot_id(committed_snapshot_id);
            req.set_reason("Try to abort committed snapshot");
            AbortSnapshotResponse res;
            meta_service->abort_snapshot(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                    nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
            ASSERT_TRUE(
                    res.status().msg().find("cannot abort snapshot that is already committed") !=
                    std::string::npos);
        }
    }
}

TEST(MetaServiceSnapshotTest, ListSnapshotTest) {
    auto meta_service = get_meta_service(true);
    const char* const cloud_unique_id = "test_cloud_unique_id";

    // Setup SyncPoint for encryption
    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });
    sp->set_call_back("decrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* key = try_any_cast<std::string*>(args[0]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* ret = try_any_cast<int*>(args[1]);
        *ret = 0;
    });

    // Cleanup SyncPoint when test finishes
    DORIS_CLOUD_DEFER {
        sp->disable_processing();
        sp->clear_all_call_backs();
    };

    // Create test instance first
    {
        brpc::Controller cntl;
        CreateInstanceRequest req;
        req.set_instance_id("test_instance");
        req.set_user_id("test_user");
        req.set_name("test_name");
        ObjectStoreInfoPB obj;
        obj.set_ak("123");
        obj.set_sk("321");
        obj.set_bucket("456");
        obj.set_prefix("654");
        obj.set_endpoint("789");
        obj.set_region("987");
        obj.set_external_endpoint("888");
        obj.set_provider(ObjectStoreInfoPB::BOS);
        req.mutable_obj_info()->CopyFrom(obj);

        CreateInstanceResponse res;
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Enable multi version for the test instance
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string instance_key_str = instance_key("test_instance");
        std::string instance_value;
        ASSERT_EQ(txn->get(instance_key_str, &instance_value), TxnErrorCode::TXN_OK);
        InstanceInfoPB instance_info;
        ASSERT_TRUE(instance_info.ParseFromString(instance_value));
        instance_info.set_snapshot_switch_status(SnapshotSwitchStatus::SNAPSHOT_SWITCH_ON);
        txn->put(instance_key_str, instance_info.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Test invalid argument - empty cloud_unique_id
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find("cloud_unique_id") != std::string::npos)
                << res.ShortDebugString();
    }

    // Test with invalid IP address
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_request_ip("invalid.ip.format");
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find("invalid request IP address format") !=
                    std::string::npos);
    }

    // Test list snapshots when no snapshots exist
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.snapshots_size(), 0);
    }

    // Create several snapshots for testing
    std::vector<std::string> snapshot_ids;
    std::vector<std::string> image_urls;

    // Create first snapshot
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_auto_snapshot(true);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("first_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        snapshot_ids.push_back(res.snapshot_id());
        std::cout << res.snapshot_id() << std::endl;
        image_urls.push_back(res.image_url());
    }

    // Create second snapshot
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(1800);
        req.set_auto_snapshot(false);
        req.set_ttl_seconds(3600);
        req.set_snapshot_label("second_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        snapshot_ids.push_back(res.snapshot_id());
        std::cout << res.snapshot_id() << std::endl;
        image_urls.push_back(res.image_url());
    }

    // Create third snapshot
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(5400);
        req.set_auto_snapshot(true);
        req.set_ttl_seconds(10800);
        req.set_snapshot_label("third_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        snapshot_ids.push_back(res.snapshot_id());
        std::cout << res.snapshot_id() << std::endl;
        image_urls.push_back(res.image_url());
    }

    // Commit the first snapshot
    {
        brpc::Controller cntl;
        CommitSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_ids[0]);
        req.set_image_url(image_urls[0]);
        req.set_last_journal_id(12345);
        CommitSnapshotResponse res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Abort the second snapshot
    {
        brpc::Controller cntl;
        AbortSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_ids[1]);
        req.set_reason("Test abort for list");
        AbortSnapshotResponse res;
        meta_service->abort_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Test list all snapshots (excluding aborted by default)
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.snapshots_size(), 2); // Should exclude the aborted snapshot

        // Check that we have the committed and prepare snapshots, but NOT the aborted one
        bool found_committed = false;
        bool found_prepare = false;
        bool found_aborted = false;
        for (const auto& snapshot : res.snapshots()) {
            if (snapshot.snapshot_id() == snapshot_ids[0]) {
                found_committed = true;
                ASSERT_EQ(snapshot.status(), SNAPSHOT_NORMAL);
                ASSERT_EQ(snapshot.snapshot_label(), "first_snapshot");
                ASSERT_EQ(snapshot.journal_id(), 12345);
                ASSERT_TRUE(snapshot.has_image_url());
            } else if (snapshot.snapshot_id() == snapshot_ids[1]) {
                found_aborted = true; // This should NOT happen in default behavior
            } else if (snapshot.snapshot_id() == snapshot_ids[2]) {
                found_prepare = true;
                ASSERT_EQ(snapshot.status(), SNAPSHOT_PREPARE);
                ASSERT_EQ(snapshot.snapshot_label(), "third_snapshot");
            }
        }
        ASSERT_TRUE(found_committed);
        ASSERT_TRUE(found_prepare);
        ASSERT_FALSE(found_aborted); // Ensure aborted snapshot is NOT included by default
    }

    // Test list all snapshots including aborted
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_include_aborted(true);
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.snapshots_size(), 3); // Should include all snapshots

        // Check that we have all three snapshots
        bool found_committed = false;
        bool found_aborted = false;
        bool found_prepare = false;
        for (const auto& snapshot : res.snapshots()) {
            if (snapshot.snapshot_id() == snapshot_ids[0]) {
                found_committed = true;
                ASSERT_EQ(snapshot.status(), SNAPSHOT_NORMAL);
            } else if (snapshot.snapshot_id() == snapshot_ids[1]) {
                found_aborted = true;
                ASSERT_EQ(snapshot.status(), SNAPSHOT_ABORTED);
                ASSERT_EQ(snapshot.reason(), "Test abort for list");
            } else if (snapshot.snapshot_id() == snapshot_ids[2]) {
                found_prepare = true;
                ASSERT_EQ(snapshot.status(), SNAPSHOT_PREPARE);
            }
        }
        ASSERT_TRUE(found_committed);
        ASSERT_TRUE(found_aborted);
        ASSERT_TRUE(found_prepare);
    }

    // Test optimized query - list specific snapshot (committed)
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_required_snapshot_id(snapshot_ids[0]); // Query specific committed snapshot
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.snapshots_size(), 1);

        const auto& snapshot = res.snapshots(0);
        ASSERT_EQ(snapshot.snapshot_id(), snapshot_ids[0]);
        ASSERT_EQ(snapshot.status(), SNAPSHOT_NORMAL);
        ASSERT_EQ(snapshot.snapshot_label(), "first_snapshot");
        ASSERT_EQ(snapshot.journal_id(), 12345);
        ASSERT_TRUE(snapshot.has_image_url());
        ASSERT_TRUE(snapshot.has_finish_at());
    }

    // Test optimized query - list specific snapshot (aborted, not included by default)
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_required_snapshot_id(snapshot_ids[1]); // Query specific aborted snapshot
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.snapshots_size(),
                  0); // Should be empty since aborted snapshots are excluded by default
    }

    // Test optimized query - list specific snapshot (aborted, included with flag)
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_required_snapshot_id(snapshot_ids[1]); // Query specific aborted snapshot
        req.set_include_aborted(true);
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.snapshots_size(), 1);

        const auto& snapshot = res.snapshots(0);
        ASSERT_EQ(snapshot.snapshot_id(), snapshot_ids[1]);
        ASSERT_EQ(snapshot.status(), SNAPSHOT_ABORTED);
        ASSERT_EQ(snapshot.snapshot_label(), "second_snapshot");
        ASSERT_EQ(snapshot.reason(), "Test abort for list");
        ASSERT_TRUE(snapshot.has_finish_at());
    }

    // Test optimized query - list specific snapshot (prepare)
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_required_snapshot_id(snapshot_ids[2]); // Query specific prepare snapshot
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.snapshots_size(), 1);

        const auto& snapshot = res.snapshots(0);
        ASSERT_EQ(snapshot.snapshot_id(), snapshot_ids[2]);
        ASSERT_EQ(snapshot.status(), SNAPSHOT_PREPARE);
        ASSERT_EQ(snapshot.snapshot_label(), "third_snapshot");
        ASSERT_EQ(snapshot.timeout_seconds(), 5400);
        ASSERT_EQ(snapshot.ttl_seconds(), 10800);
        ASSERT_TRUE(snapshot.auto_snapshot());
    }

    // Test optimized query - list non-existent snapshot
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_required_snapshot_id("12345678900987654321"); // Non-existent snapshot
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.snapshots_size(), 0); // Should return empty result
    }

    // Test optimized query - invalid snapshot ID format
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_required_snapshot_id("invalid_id"); // Invalid format (not 10 bytes)
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find("invalid snapshot_id format") != std::string::npos);
    }

    // Test with valid IP addresses
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_request_ip("192.168.1.100");
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_GE(res.snapshots_size(), 0);
    }

    // Test with IPv6 address
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_request_ip("2001:db8::1");
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_GE(res.snapshots_size(), 0);
    }
}

TEST(MetaServiceSnapshotTest, DropSnapshotTest) {
    auto meta_service = get_meta_service(true);
    const char* const cloud_unique_id = "test_cloud_unique_id";

    // Setup SyncPoint for encryption
    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });
    sp->set_call_back("decrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* key = try_any_cast<std::string*>(args[0]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* ret = try_any_cast<int*>(args[1]);
        *ret = 0;
    });

    // Cleanup SyncPoint when test finishes
    DORIS_CLOUD_DEFER {
        sp->disable_processing();
        sp->clear_all_call_backs();
    };

    // Create test instance first
    {
        brpc::Controller cntl;
        CreateInstanceRequest req;
        req.set_instance_id("test_instance");
        req.set_user_id("test_user");
        req.set_name("test_name");
        ObjectStoreInfoPB obj;
        obj.set_ak("123");
        obj.set_sk("321");
        obj.set_bucket("456");
        obj.set_prefix("654");
        obj.set_endpoint("789");
        obj.set_region("987");
        obj.set_external_endpoint("888");
        obj.set_provider(ObjectStoreInfoPB::BOS);
        req.mutable_obj_info()->CopyFrom(obj);

        CreateInstanceResponse res;
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Enable multi version for the test instance
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string instance_key_str = instance_key("test_instance");
        std::string instance_value;
        ASSERT_EQ(txn->get(instance_key_str, &instance_value), TxnErrorCode::TXN_OK);
        InstanceInfoPB instance_info;
        ASSERT_TRUE(instance_info.ParseFromString(instance_value));
        instance_info.set_snapshot_switch_status(SnapshotSwitchStatus::SNAPSHOT_SWITCH_ON);
        txn->put(instance_key_str, instance_info.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Test invalid argument - empty cloud_unique_id
    {
        brpc::Controller cntl;
        DropSnapshotRequest req;
        req.set_snapshot_id("1234567890abcdef1234");
        DropSnapshotResponse res;
        meta_service->drop_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find("cloud_unique_id not set") != std::string::npos);
    }

    // Test invalid argument - empty snapshot_id
    {
        brpc::Controller cntl;
        DropSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        DropSnapshotResponse res;
        meta_service->drop_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find("snapshot_id not set") != std::string::npos);
    }

    // Test invalid argument - invalid IP address
    {
        brpc::Controller cntl;
        DropSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id("1234567890abcdef1234");
        req.set_request_ip("invalid.ip.format");
        DropSnapshotResponse res;
        meta_service->drop_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find("invalid request IP address format") !=
                    std::string::npos);
    }

    // Test invalid snapshot_id format - wrong length
    {
        brpc::Controller cntl;
        DropSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id("invalid_length");
        DropSnapshotResponse res;
        meta_service->drop_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find("invalid snapshot_id format") != std::string::npos);
    }

    // Test drop non-existent snapshot
    {
        brpc::Controller cntl;
        DropSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id("1234567890abcdef1234"); // Non-existent snapshot ID
        DropSnapshotResponse res;
        meta_service->drop_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_ID_NOT_FOUND);
        ASSERT_TRUE(res.status().msg().find("snapshot not found") != std::string::npos);
    }

    // Create snapshots for testing drop functionality
    std::string committed_snapshot_id;
    std::string aborted_snapshot_id;
    std::string prepare_snapshot_id;

    // Create and commit a snapshot
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_auto_snapshot(true);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_drop_committed");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        committed_snapshot_id = res.snapshot_id();

        // Commit it
        CommitSnapshotRequest commit_req;
        commit_req.set_cloud_unique_id(cloud_unique_id);
        commit_req.set_snapshot_id(committed_snapshot_id);
        commit_req.set_image_url(res.image_url());
        commit_req.set_last_journal_id(12345);
        CommitSnapshotResponse commit_res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &commit_req, &commit_res, nullptr);
        ASSERT_EQ(commit_res.status().code(), MetaServiceCode::OK);
    }

    // Create and abort a snapshot
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_auto_snapshot(false);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_drop_aborted");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        aborted_snapshot_id = res.snapshot_id();

        // Abort it
        AbortSnapshotRequest abort_req;
        abort_req.set_cloud_unique_id(cloud_unique_id);
        abort_req.set_snapshot_id(aborted_snapshot_id);
        abort_req.set_reason("Test abort for drop");
        AbortSnapshotResponse abort_res;
        meta_service->abort_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &abort_req, &abort_res, nullptr);
        ASSERT_EQ(abort_res.status().code(), MetaServiceCode::OK);
    }

    // Create a snapshot in prepare state
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_auto_snapshot(true);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_drop_prepare");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        prepare_snapshot_id = res.snapshot_id();
    }

    // Test drop snapshot in prepare state (should fail)
    {
        brpc::Controller cntl;
        DropSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(prepare_snapshot_id);
        DropSnapshotResponse res;
        meta_service->drop_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find("cannot drop snapshot that is not in final state") !=
                    std::string::npos);
    }

    // Test successful drop of committed snapshot
    {
        brpc::Controller cntl;
        DropSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(committed_snapshot_id);
        DropSnapshotResponse res;
        meta_service->drop_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Test successful drop of aborted snapshot
    {
        brpc::Controller cntl;
        DropSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(aborted_snapshot_id);
        DropSnapshotResponse res;
        meta_service->drop_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Test drop already dropped snapshot (should fail)
    {
        brpc::Controller cntl;
        DropSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(committed_snapshot_id);
        DropSnapshotResponse res;
        meta_service->drop_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_ID_NOT_FOUND);
        ASSERT_TRUE(res.status().msg().find("snapshot not found") != std::string::npos);
    }

    // Test with valid IPv4 address
    {
        // Create another committed snapshot for IP test
        std::string ip_test_snapshot_id;
        {
            brpc::Controller cntl;
            BeginSnapshotRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_timeout_seconds(3600);
            req.set_auto_snapshot(true);
            req.set_ttl_seconds(7200);
            req.set_snapshot_label("test_drop_with_ip");
            BeginSnapshotResponse res;
            meta_service->begin_snapshot(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                    nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ip_test_snapshot_id = res.snapshot_id();

            // Commit it
            CommitSnapshotRequest commit_req;
            commit_req.set_cloud_unique_id(cloud_unique_id);
            commit_req.set_snapshot_id(ip_test_snapshot_id);
            commit_req.set_image_url(res.image_url());
            commit_req.set_last_journal_id(54321);
            CommitSnapshotResponse commit_res;
            meta_service->commit_snapshot(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &commit_req,
                    &commit_res, nullptr);
            ASSERT_EQ(commit_res.status().code(), MetaServiceCode::OK);
        }

        brpc::Controller cntl;
        DropSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(ip_test_snapshot_id);
        req.set_request_ip("192.168.1.100");
        DropSnapshotResponse res;
        meta_service->drop_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Test with valid IPv6 address
    {
        // Create another committed snapshot for IPv6 test
        std::string ipv6_test_snapshot_id;
        {
            brpc::Controller cntl;
            BeginSnapshotRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_timeout_seconds(3600);
            req.set_auto_snapshot(false);
            req.set_ttl_seconds(7200);
            req.set_snapshot_label("test_drop_with_ipv6");
            BeginSnapshotResponse res;
            meta_service->begin_snapshot(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                    nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ipv6_test_snapshot_id = res.snapshot_id();

            // Commit it
            CommitSnapshotRequest commit_req;
            commit_req.set_cloud_unique_id(cloud_unique_id);
            commit_req.set_snapshot_id(ipv6_test_snapshot_id);
            commit_req.set_image_url(res.image_url());
            commit_req.set_last_journal_id(67890);
            CommitSnapshotResponse commit_res;
            meta_service->commit_snapshot(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &commit_req,
                    &commit_res, nullptr);
            ASSERT_EQ(commit_res.status().code(), MetaServiceCode::OK);
        }

        brpc::Controller cntl;
        DropSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(ipv6_test_snapshot_id);
        req.set_request_ip("2001:db8::1");
        DropSnapshotResponse res;
        meta_service->drop_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Test with empty IP address (should pass - IP is optional)
    {
        // Create another committed snapshot for empty IP test
        std::string empty_ip_test_snapshot_id;
        {
            brpc::Controller cntl;
            BeginSnapshotRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_timeout_seconds(3600);
            req.set_auto_snapshot(true);
            req.set_ttl_seconds(7200);
            req.set_snapshot_label("test_drop_empty_ip");
            BeginSnapshotResponse res;
            meta_service->begin_snapshot(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                    nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            empty_ip_test_snapshot_id = res.snapshot_id();

            // Commit it
            CommitSnapshotRequest commit_req;
            commit_req.set_cloud_unique_id(cloud_unique_id);
            commit_req.set_snapshot_id(empty_ip_test_snapshot_id);
            commit_req.set_image_url(res.image_url());
            commit_req.set_last_journal_id(98765);
            CommitSnapshotResponse commit_res;
            meta_service->commit_snapshot(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &commit_req,
                    &commit_res, nullptr);
            ASSERT_EQ(commit_res.status().code(), MetaServiceCode::OK);
        }

        brpc::Controller cntl;
        DropSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(empty_ip_test_snapshot_id);
        req.set_request_ip("");
        DropSnapshotResponse res;
        meta_service->drop_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Verify dropped snapshots are no longer listed
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_include_aborted(true);
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        // Should only have the prepare snapshot remaining
        bool found_prepare = false;
        bool found_dropped = false;
        for (const auto& snapshot : res.snapshots()) {
            if (snapshot.snapshot_id() == prepare_snapshot_id) {
                found_prepare = true;
                ASSERT_EQ(snapshot.status(), SNAPSHOT_PREPARE);
            } else if (snapshot.snapshot_id() == committed_snapshot_id ||
                       snapshot.snapshot_id() == aborted_snapshot_id) {
                found_dropped = true; // Should not happen
            }
        }
        ASSERT_TRUE(found_prepare);
        ASSERT_FALSE(found_dropped); // Ensure dropped snapshots are not listed
    }
}

TEST(MetaServiceSnapshotTest, BeginAutoSnapshotDisabledTest) {
    auto meta_service = get_meta_service(true);
    const char* const cloud_unique_id = "test_cloud_unique_id_auto_disabled";

    // Setup SyncPoint for encryption
    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });
    sp->set_call_back("decrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* key = try_any_cast<std::string*>(args[0]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* ret = try_any_cast<int*>(args[1]);
        *ret = 0;
    });

    // Cleanup SyncPoint when test finishes
    DORIS_CLOUD_DEFER {
        sp->disable_processing();
        sp->clear_all_call_backs();
    };

    CreateInstanceRequest req;
    CreateInstanceResponse res;
    req.set_instance_id("test_instance");
    req.set_name("test_instance");
    req.set_user_id("test_user");

    auto obj_info = req.mutable_obj_info();
    obj_info->set_ak("ak_test");
    obj_info->set_sk("sk_test");
    obj_info->set_bucket("test_bucket");
    obj_info->set_prefix("test_prefix");
    obj_info->set_endpoint("test_endpoint");
    obj_info->set_region("test_region");
    obj_info->set_external_endpoint("test_external_endpoint");
    obj_info->set_provider(ObjectStoreInfoPB::OSS);
    obj_info->set_id("obj_info_id");

    brpc::Controller cntl;
    meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                  &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

    // Enable multi version for the test instance
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string instance_key_str = instance_key("test_instance");
        std::string instance_value;
        ASSERT_EQ(txn->get(instance_key_str, &instance_value), TxnErrorCode::TXN_OK);
        InstanceInfoPB instance_info;
        ASSERT_TRUE(instance_info.ParseFromString(instance_value));
        instance_info.set_snapshot_switch_status(SnapshotSwitchStatus::SNAPSHOT_SWITCH_OFF);
        txn->put(instance_key_str, instance_info.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Set up snapshot settings after instance creation using alter_instance
    {
        brpc::Controller cntl;
        AlterInstanceRequest alter_req;
        AlterInstanceResponse alter_res;
        alter_req.set_op(AlterInstanceRequest::SET_SNAPSHOT_PROPERTY);
        alter_req.set_instance_id("test_instance");
        (*alter_req.mutable_properties())["status"] = "ENABLED";
        (*alter_req.mutable_properties())["max_reserved_snapshots"] = "0"; // Disable auto snapshot

        meta_service->alter_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &alter_req, &alter_res, nullptr);
        ASSERT_EQ(alter_res.status().code(), MetaServiceCode::OK);
    }

    // Test manual snapshot should work even when auto snapshot is disabled
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(1800);
        req.set_auto_snapshot(false); // Manual snapshot
        req.set_ttl_seconds(14400);
        req.set_snapshot_label("manual_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_FALSE(res.image_url().empty());
        ASSERT_FALSE(res.snapshot_id().empty());
    }

    // Test auto snapshot should fail when max_reserved_snapshots is 0
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(1800);
        req.set_auto_snapshot(true); // Auto snapshot
        req.set_ttl_seconds(14400);
        req.set_snapshot_label("auto_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find("auto snapshot is disabled") != std::string::npos)
                << res.ShortDebugString();
    }

    // Test auto snapshot should work when max_reserved_snapshots > 0
    {
        // First update instance to enable auto snapshot
        AlterInstanceRequest alter_req;
        AlterInstanceResponse alter_res;
        alter_req.set_op(AlterInstanceRequest::SET_SNAPSHOT_PROPERTY);
        alter_req.set_instance_id("test_instance");
        (*alter_req.mutable_properties())["max_reserved_snapshots"] = "5";

        brpc::Controller cntl;
        meta_service->alter_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &alter_req, &alter_res, nullptr);
        ASSERT_EQ(alter_res.status().code(), MetaServiceCode::OK);

        // Now auto snapshot should work
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(1800);
        req.set_auto_snapshot(true); // Auto snapshot
        req.set_ttl_seconds(14400);
        req.set_snapshot_label("auto_snapshot_enabled");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_FALSE(res.image_url().empty());
        ASSERT_FALSE(res.snapshot_id().empty());
    }
}

} // namespace doris::cloud
