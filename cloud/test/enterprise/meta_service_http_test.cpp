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

#include "meta-service/meta_service_http.h"

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <fmt/format.h>
#include <gen_cpp/cloud.pb.h>
#include <google/protobuf/message.h>
#include <google/protobuf/stubs/callback.h>
#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>

#include "common/config.h"
#include "common/configbase.h"
#include "common/defer.h"
#include "common/logging.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-service/meta_service.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "mock_resource_manager.h"
#include "resource-manager/resource_manager.h"
#include "snapshot_manager.h"

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

    if (!doris::cloud::init_glog("meta_service_http_test")) {
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

template <typename Request, typename Response>
using MetaServiceMethod = void (MetaService::*)(google::protobuf::RpcController*, const Request*,
                                                Response*, google::protobuf::Closure*);

template <typename Result>
struct JsonTemplate {
    MetaServiceResponseStatus status;
    std::optional<Result> result;

    static JsonTemplate parse(const std::string& json) {
        static_assert(std::is_base_of_v<::google::protobuf::Message, Result>);

        MetaServiceResponseStatus status;
        google::protobuf::util::JsonParseOptions options;
        options.ignore_unknown_fields = true;
        auto ss = google::protobuf::util::JsonStringToMessage(json, &status, options);
        EXPECT_TRUE(ss.ok()) << "JSON Parse result: " << ss.ToString() << ", body: " << json;

        rapidjson::Document d;
        rapidjson::ParseResult ps = d.Parse(json.c_str());
        EXPECT_TRUE(ps) << __PRETTY_FUNCTION__
                        << " parse failed: " << rapidjson::GetParseError_En(ps.Code())
                        << ", body: " << json;

        if (!ps.IsError() && d.HasMember("result")) {
            rapidjson::StringBuffer sb;
            rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(sb);
            d["result"].Accept(writer);
            std::string content = sb.GetString();
            Result result;
            auto s = google::protobuf::util::JsonStringToMessage(content, &result);
            EXPECT_TRUE(s.ok()) << "JSON Parse result: " << s.ToString()
                                << ", content: " << content;
            return {std::move(status), std::move(result)};
        }
        return {std::move(status), {}};
    }
};

class HttpContext {
public:
    HttpContext(bool mock_resource_mgr = false)
            : meta_service_(get_meta_service(mock_resource_mgr)) {
        auto sp = SyncPoint::get_instance();
        sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
            auto* ret = try_any_cast<int*>(args[0]);
            *ret = 0;
            auto* key = try_any_cast<std::string*>(args[1]);
            *key = "test";
            auto* key_id = try_any_cast<int64_t*>(args[2]);
            *key_id = 1;
        });
        sp->set_call_back("decrypt_ak_sk:get_encryption_key", [](auto&& args) {
            auto* key = try_any_cast<std::string*>(args[0]);
            *key = "test";
            auto* ret = try_any_cast<int*>(args[1]);
            *ret = 0;
        });
        sp->enable_processing();

        brpc::ServerOptions options;
        server.AddService(meta_service_.get(), brpc::ServiceOwnership::SERVER_DOESNT_OWN_SERVICE);
        if (server.Start("0.0.0.0:0", &options) == -1) {
            perror("Start brpc server");
        }
    }

    ~HttpContext() {
        server.Stop(0);
        server.Join();

        auto sp = SyncPoint::get_instance();
        sp->clear_all_call_backs();
        sp->clear_trace();
        sp->disable_processing();
    }

    template <typename Response>
    std::tuple<int, Response> query(std::string_view resource, std::string_view params,
                                    std::optional<std::string_view> body = {}) {
        butil::EndPoint endpoint = server.listen_address();

        brpc::Channel channel;
        brpc::ChannelOptions options;
        options.protocol = brpc::PROTOCOL_HTTP;
        EXPECT_EQ(channel.Init(endpoint, &options), 0) << "Fail to initialize channel";

        brpc::Controller ctrl;
        if (params.find("token=") != std::string_view::npos) {
            ctrl.http_request().uri() = fmt::format("0.0.0.0:{}/MetaService/http/{}?{}",
                                                    endpoint.port, resource, params);
        } else {
            ctrl.http_request().uri() =
                    fmt::format("0.0.0.0:{}/MetaService/http/{}?token={}&{}", endpoint.port,
                                resource, config::http_token, params);
        }
        if (body.has_value()) {
            ctrl.http_request().set_method(brpc::HTTP_METHOD_POST);
            ctrl.request_attachment().append(body->data(), body->size());
        }
        channel.CallMethod(nullptr, &ctrl, nullptr, nullptr, nullptr);
        int status_code = ctrl.http_response().status_code();

        std::string response_body = ctrl.response_attachment().to_string();
        if constexpr (std::is_base_of_v<::google::protobuf::Message, Response>) {
            Response resp;
            auto s = google::protobuf::util::JsonStringToMessage(response_body, &resp);
            static_assert(std::is_base_of_v<::google::protobuf::Message, Response>);
            EXPECT_TRUE(s.ok()) << __PRETTY_FUNCTION__ << " Parse JSON: " << s.ToString();
            return {status_code, std::move(resp)};
        } else if constexpr (std::is_same_v<std::string, Response>) {
            return {status_code, std::move(response_body)};
        } else {
            return {status_code, {}};
        }
    }

    template <typename Response>
    std::tuple<int, JsonTemplate<Response>> query_with_result(std::string_view resource,
                                                              std::string_view param) {
        auto [status_code, body] = query<std::string>(resource, param);
        LOG_INFO(__PRETTY_FUNCTION__).tag("body", body);
        return {status_code, JsonTemplate<Response>::parse(body)};
    }

    template <typename Response, typename Request>
    std::tuple<int, Response> forward(std::string_view query, const Request& req) {
        static_assert(std::is_base_of_v<::google::protobuf::Message, Request>);

        butil::EndPoint endpoint = server.listen_address();

        brpc::Channel channel;
        brpc::ChannelOptions options;
        options.protocol = brpc::PROTOCOL_HTTP;
        EXPECT_EQ(channel.Init(endpoint, &options), 0) << "Fail to initialize channel";

        brpc::Controller ctrl;
        ctrl.http_request().set_method(brpc::HTTP_METHOD_POST);
        ctrl.http_request().uri() = fmt::format(
                "0.0.0.0:{}/MetaService/http/{}{}token={}", endpoint.port, query,
                (query.find('?') != std::string_view::npos) ? "&" : "?", config::http_token);
        ctrl.request_attachment().append(proto_to_json(req));
        LOG_INFO("request attachment").tag("msg", ctrl.request_attachment().to_string());
        channel.CallMethod(nullptr, &ctrl, nullptr, nullptr, nullptr);
        int status_code = ctrl.http_response().status_code();

        std::string response_body = ctrl.response_attachment().to_string();
        if constexpr (std::is_base_of_v<::google::protobuf::Message, Response>) {
            Response resp;
            auto s = google::protobuf::util::JsonStringToMessage(response_body, &resp);
            return {status_code, std::move(resp)};
        } else if (std::is_same_v<std::string, Response>) {
            return {status_code, std::move(response_body)};
        } else {
            return {status_code, {}};
        }
    }
    template <typename Response, typename Request>
    std::tuple<int, JsonTemplate<Response>> forward_with_result(std::string_view query,
                                                                const Request& req) {
        auto [status_code, body] = forward<std::string>(query, req);
        LOG_INFO(__PRETTY_FUNCTION__).tag("body", body);
        return {status_code, JsonTemplate<Response>::parse(body)};
    }

    InstanceInfoPB get_instance_info(std::string_view instance_id) {
        InstanceKeyInfo key_info {instance_id};
        std::string key;
        std::string val;
        instance_key(key_info, &key);
        std::unique_ptr<Transaction> txn;
        EXPECT_EQ(meta_service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        EXPECT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
        InstanceInfoPB instance;
        instance.ParseFromString(val);
        return instance;
    }

    MetaServiceProxy* meta_service() const { return meta_service_.get(); }

private:
    std::unique_ptr<MetaServiceProxy> meta_service_;
    brpc::Server server;
};

/// NOTICE: Not ALL `code`, returned by http server, are supported by `MetaServiceCode`.

// Helper function to create test instance for snapshot tests
static std::string create_test_instance_for_snapshot(HttpContext& ctx,
                                                     const std::string& instance_id) {
    CreateInstanceRequest req;
    req.set_instance_id(instance_id);
    req.set_user_id("test_user");
    req.set_name("test_instance");
    ObjectStoreInfoPB obj;
    obj.set_ak("test_ak");
    obj.set_sk("test_sk");
    obj.set_bucket("test_bucket");
    obj.set_prefix("test_prefix");
    obj.set_endpoint("test_endpoint");
    obj.set_region("test_region");
    obj.set_external_endpoint("test_external_endpoint");
    obj.set_provider(ObjectStoreInfoPB::BOS);
    req.mutable_obj_info()->CopyFrom(obj);

    auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("create_instance", req);
    EXPECT_EQ(status_code, 200);
    EXPECT_EQ(resp.code(), MetaServiceCode::OK);
    return instance_id;
}

TEST(MetaServiceHttpTest, SetSnapshotPropertyHttpTest) {
    HttpContext ctx;
    std::string instance_id = "test_set_snapshot_property_instance";
    create_test_instance_for_snapshot(ctx, instance_id);

    // Initialize snapshot switch status to OFF so we can test snapshot functionality
    {
        InstanceKeyInfo key_info {instance_id};
        std::string key;
        std::string val;
        instance_key(key_info, &key);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(ctx.meta_service()->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
        InstanceInfoPB instance;
        instance.ParseFromString(val);
        instance.set_snapshot_switch_status(SNAPSHOT_SWITCH_OFF);
        val = instance.SerializeAsString();
        txn->put(key, val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Test set snapshot property with valid values
    {
        AlterInstanceRequest req;
        req.set_instance_id(instance_id);
        req.set_op(AlterInstanceRequest::SET_SNAPSHOT_PROPERTY);
        auto& properties = *req.mutable_properties();
        properties["enabled"] = "true";
        properties["max_reserved_snapshots"] = "10";
        properties["snapshot_interval_seconds"] = "3600";

        auto [status_code, resp] = ctx.forward<AlterInstanceResponse>("set_snapshot_property", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK);
    }

    // Test set snapshot property with different valid values
    {
        AlterInstanceRequest req;
        req.set_instance_id(instance_id);
        req.set_op(AlterInstanceRequest::SET_SNAPSHOT_PROPERTY);
        auto& properties = *req.mutable_properties();
        properties["enabled"] = "false";
        properties["max_reserved_snapshots"] = "5";
        properties["snapshot_interval_seconds"] = "3600";

        auto [status_code, resp] = ctx.forward<AlterInstanceResponse>("set_snapshot_property", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK);
    }
}

TEST(MetaServiceHttpTest, SetSnapshotPropertyInvalidValuesHttpTest) {
    HttpContext ctx;
    std::string instance_id = "test_set_snapshot_property_invalid_instance";
    create_test_instance_for_snapshot(ctx, instance_id);

    // Test invalid boolean value for enabled
    {
        AlterInstanceRequest req;
        req.set_instance_id(instance_id);
        req.set_op(AlterInstanceRequest::SET_SNAPSHOT_PROPERTY);
        auto& properties = *req.mutable_properties();
        properties["enabled"] = "invalid_value";

        auto [status_code, resp] = ctx.forward<AlterInstanceResponse>("set_snapshot_property", req);
        ASSERT_EQ(status_code, 400);
    }

    // Test unsupported property
    {
        AlterInstanceRequest req;
        req.set_instance_id(instance_id);
        req.set_op(AlterInstanceRequest::SET_SNAPSHOT_PROPERTY);
        auto& properties = *req.mutable_properties();
        properties["unsupported_property"] = "value";

        auto [status_code, resp] = ctx.forward<AlterInstanceResponse>("set_snapshot_property", req);
        ASSERT_EQ(status_code, 400);
    }

    // Test empty properties
    {
        AlterInstanceRequest req;
        req.set_instance_id(instance_id);
        req.set_op(AlterInstanceRequest::SET_SNAPSHOT_PROPERTY);
        // No properties set

        auto [status_code, resp] = ctx.forward<AlterInstanceResponse>("set_snapshot_property", req);
        ASSERT_EQ(status_code, 400);
    }
}

TEST(MetaServiceHttpTest, GetSnapshotPropertyHttpTest) {
    HttpContext ctx;
    std::string instance_id = "test_get_snapshot_property_instance";
    create_test_instance_for_snapshot(ctx, instance_id);

    // Initialize snapshot switch status to OFF so we can test snapshot functionality
    {
        InstanceKeyInfo key_info {instance_id};
        std::string key;
        std::string val;
        instance_key(key_info, &key);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(ctx.meta_service()->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
        InstanceInfoPB instance;
        instance.ParseFromString(val);
        instance.set_snapshot_switch_status(SNAPSHOT_SWITCH_OFF);
        val = instance.SerializeAsString();
        txn->put(key, val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // First set some properties
    {
        AlterInstanceRequest req;
        req.set_instance_id(instance_id);
        req.set_op(AlterInstanceRequest::SET_SNAPSHOT_PROPERTY);
        auto& properties = *req.mutable_properties();
        properties["enabled"] = "true";
        properties["max_reserved_snapshots"] = "10";
        properties["snapshot_interval_seconds"] = "3600";

        auto [status_code, resp] = ctx.forward<AlterInstanceResponse>("set_snapshot_property", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK);
    }

    // Test get snapshot property by instance_id
    {
        auto [status_code, response_body] = ctx.query<std::string>(
                "get_snapshot_property", fmt::format("instance_id={}", instance_id));
        ASSERT_EQ(status_code, 200);

        // Check the response contains snapshot properties
        EXPECT_TRUE(response_body.find("enabled") != std::string::npos);
        EXPECT_TRUE(response_body.find("max_reserved_snapshots") != std::string::npos);
        EXPECT_TRUE(response_body.find("snapshot_interval_seconds") != std::string::npos);
    }

    // Test get snapshot property by cloud_unique_id
    {
        auto [status_code, response_body] = ctx.query<std::string>(
                "get_snapshot_property", fmt::format("cloud_unique_id=1:{}:1", instance_id));
        ASSERT_EQ(status_code, 200);

        // Check the response contains snapshot properties
        EXPECT_TRUE(response_body.find("enabled") != std::string::npos);
        EXPECT_TRUE(response_body.find("max_reserved_snapshots") != std::string::npos);
        EXPECT_TRUE(response_body.find("snapshot_interval_seconds") != std::string::npos);
    }

    // Test get snapshot property with invalid parameters
    {
        auto [status_code, response_body] = ctx.query<std::string>("get_snapshot_property", "");
        ASSERT_EQ(status_code, 400);
        EXPECT_TRUE(response_body.find("empty instance_id and cloud_unique_id") !=
                    std::string::npos);
    }
}

TEST(MetaServiceHttpTest, ListSnapshotHttpTest) {
    HttpContext ctx;
    std::string instance_id = "test_list_snapshot_instance";
    create_test_instance_for_snapshot(ctx, instance_id);

    // Enable multi-version for snapshot functionality
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(ctx.meta_service()->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        InstanceKeyInfo key_info {instance_id};
        std::string key;
        std::string val;
        instance_key(key_info, &key);
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
        InstanceInfoPB instance_info;
        ASSERT_TRUE(instance_info.ParseFromString(val));
        instance_info.set_multi_version_status(MultiVersionStatus::MULTI_VERSION_ENABLED);
        txn->put(key, instance_info.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Test list snapshot when empty
    {
        ListSnapshotRequest req;
        req.set_cloud_unique_id(fmt::format("1:{}:1", instance_id));

        auto [status_code, resp] = ctx.forward<ListSnapshotResponse>("list_snapshot", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK);
        EXPECT_EQ(resp.snapshots_size(), 0);
    }

    // Create a snapshot first
    std::string snapshot_id;
    {
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(fmt::format("1:{}:1", instance_id));
        req.set_snapshot_label("test_list_snapshot");
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(7200);
        req.set_auto_snapshot(false);

        brpc::Controller ctrl;
        BeginSnapshotResponse resp;
        ctx.meta_service()->begin_snapshot(&ctrl, &req, &resp, nullptr);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK);
        snapshot_id = resp.snapshot_id();
    }

    // Test list all snapshots
    {
        ListSnapshotRequest req;
        req.set_cloud_unique_id(fmt::format("1:{}:1", instance_id));

        // Test HTTP endpoint returns 200 and OK status
        auto [status_code, resp] = ctx.forward<ListSnapshotResponse>("list_snapshot", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK);

        // Verify business logic with direct service call (since HTTP JSON parsing has issues with repeated fields)
        brpc::Controller ctrl;
        ListSnapshotResponse direct_resp;
        ctx.meta_service()->list_snapshot(&ctrl, &req, &direct_resp, nullptr);
        ASSERT_EQ(direct_resp.status().code(), MetaServiceCode::OK);
        EXPECT_EQ(direct_resp.snapshots_size(), 1);
        EXPECT_EQ(direct_resp.snapshots(0).snapshot_label(), "test_list_snapshot");
        EXPECT_EQ(direct_resp.snapshots(0).snapshot_id(), snapshot_id);
    }

    // Test list specific snapshot by ID
    {
        ListSnapshotRequest req;
        req.set_cloud_unique_id(fmt::format("1:{}:1", instance_id));
        req.set_required_snapshot_id(snapshot_id);

        // Test HTTP endpoint returns 200 and OK status
        auto [status_code, resp] = ctx.forward<ListSnapshotResponse>("list_snapshot", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK);

        // Verify business logic with direct service call (since HTTP JSON parsing has issues with repeated fields)
        brpc::Controller ctrl;
        ListSnapshotResponse direct_resp;
        ctx.meta_service()->list_snapshot(&ctrl, &req, &direct_resp, nullptr);
        ASSERT_EQ(direct_resp.status().code(), MetaServiceCode::OK);
        EXPECT_EQ(direct_resp.snapshots_size(), 1);
        EXPECT_EQ(direct_resp.snapshots(0).snapshot_id(), snapshot_id);
    }

    // Test list non-existent snapshot
    {
        ListSnapshotRequest req;
        req.set_cloud_unique_id(fmt::format("1:{}:1", instance_id));
        req.set_required_snapshot_id("00112233445566778899"); // Non-existent ID

        // Test HTTP endpoint returns 200 and OK status
        auto [status_code, resp] = ctx.forward<ListSnapshotResponse>("list_snapshot", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK);

        // Verify business logic with direct service call (since HTTP JSON parsing has issues with repeated fields)
        brpc::Controller ctrl;
        ListSnapshotResponse direct_resp;
        ctx.meta_service()->list_snapshot(&ctrl, &req, &direct_resp, nullptr);
        ASSERT_EQ(direct_resp.status().code(), MetaServiceCode::OK);
        EXPECT_EQ(direct_resp.snapshots_size(), 0);
    }

    // Test list snapshot with invalid cloud_unique_id
    {
        ListSnapshotRequest req;
        // Empty cloud_unique_id

        // Test HTTP endpoint returns 400 status
        auto [status_code, resp] = ctx.forward<ListSnapshotResponse>("list_snapshot", req);
        ASSERT_EQ(status_code, 400);

        // Verify business logic with direct service call (since HTTP JSON parsing may have issues)
        brpc::Controller ctrl;
        ListSnapshotResponse direct_resp;
        ctx.meta_service()->list_snapshot(&ctrl, &req, &direct_resp, nullptr);
        ASSERT_EQ(direct_resp.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }
}

} // namespace doris::cloud
