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
#include <brpc/controller.h>
#include <fmt/format.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "common/defer.h"
#include "cpp/sync_point.h"
#include "enterprise/snapshot/snapshot_manager.h"
#include "meta-service/meta_service.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "mock_resource_manager.h"

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

    if (!doris::cloud::init_glog("meta_service_snapshot_test")) {
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
        instance_info.set_multi_version_status(MultiVersionStatus::MULTI_VERSION_ENABLED);
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
} // namespace doris::cloud
