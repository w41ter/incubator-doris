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
    std::string snapshot_key = encode_versioned_key(versioned::snapshot_full_key({"test_instance"}),
                                                    snapshot_versionstamp);
    std::string snapshot_val;
    if (txn->get(snapshot_key, &snapshot_val) != TxnErrorCode::TXN_OK) {
        return -3;
    }
    if (!snapshot_pb.ParseFromString(snapshot_val)) {
        return -4;
    }
    return 0;
}

constexpr int DATA_DISK_SIZE_CONST = 100;
constexpr int INDEX_DISK_SIZE_CONST = 10;
constexpr int DISK_SIZE_CONST = 110;
constexpr std::string_view RESOURCE_ID = "1";

// Create a instance and refresh the resource manager.
// This instance is MULTI_VERSION_ENABLED by default.
void create_and_refresh_instance(MetaServiceProxy* service, std::string instance_id) {
    InstanceInfoPB instance_info;
    instance_info.set_instance_id(instance_id);
    instance_info.mutable_resource_ids()->Add(std::string(RESOURCE_ID));
    instance_info.set_multi_version_status(MultiVersionStatus::MULTI_VERSION_READ_WRITE);
    instance_info.set_snapshot_switch_status(SnapshotSwitchStatus::SNAPSHOT_SWITCH_ON);
    auto* obj_info = instance_info.mutable_obj_info()->Add();
    obj_info->set_id(std::string(RESOURCE_ID));
    obj_info->set_ak("mock_ak");
    obj_info->set_sk("mock_sk");
    obj_info->set_endpoint(config::test_s3_endpoint);
    obj_info->set_region(config::test_s3_region);
    obj_info->set_bucket(config::test_s3_bucket);
    obj_info->set_prefix("");

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(instance_key(instance_id), instance_info.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    service->resource_mgr()->refresh_instance(instance_id);
    ASSERT_TRUE(service->resource_mgr()->is_version_write_enabled(instance_id));
}

void prepare_and_commit_index(MetaServiceProxy* service, const std::string& cloud_unique_id,
                              int64_t db_id, int64_t table_id, int64_t index_id) {
    IndexRequest request;
    request.set_cloud_unique_id(cloud_unique_id);
    request.set_db_id(db_id);
    request.set_table_id(table_id);
    request.add_index_ids(index_id);

    IndexResponse response;
    brpc::Controller cntl;
    service->prepare_index(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.ShortDebugString();

    // Commit index
    service->commit_index(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.ShortDebugString();
}

void prepare_and_commit_partition(MetaServiceProxy* service, const std::string& cloud_unique_id,
                                  int64_t db_id, int64_t table_id, int64_t partition_id,
                                  int64_t index_id) {
    PartitionRequest request;
    request.set_cloud_unique_id(cloud_unique_id);
    request.set_db_id(db_id);
    request.set_table_id(table_id);
    request.add_partition_ids(partition_id);
    request.add_index_ids(index_id);

    PartitionResponse response;
    brpc::Controller cntl;
    service->prepare_partition(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.ShortDebugString();

    // Commit partition
    service->commit_partition(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.ShortDebugString();
}

std::string next_rowset_id() {
    static int cnt = 0;
    return std::to_string(++cnt);
}

void add_tablet(CreateTabletsRequest& req, int64_t table_id, int64_t index_id, int64_t partition_id,
                int64_t tablet_id, bool mow = false,
                TabletStatePB state = TabletStatePB::PB_RUNNING) {
    auto tablet = req.add_tablet_metas();
    tablet->set_table_id(table_id);
    tablet->set_index_id(index_id);
    tablet->set_partition_id(partition_id);
    tablet->set_tablet_id(tablet_id);
    tablet->set_tablet_state(state);
    if (mow) {
        tablet->set_enable_unique_key_merge_on_write(true);
    }
    auto schema = tablet->mutable_schema();
    schema->set_schema_version(0);
    auto first_rowset = tablet->add_rs_metas();
    first_rowset->set_rowset_id(0); // required
    first_rowset->set_rowset_id_v2(next_rowset_id());
    first_rowset->set_start_version(0);
    first_rowset->set_end_version(1);
    first_rowset->mutable_tablet_schema()->CopyFrom(*schema);
}

void create_tablet(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                   int64_t db_id, int64_t table_id, int64_t index_id, int64_t partition_id,
                   int64_t tablet_id, bool mow = false,
                   TabletStatePB state = TabletStatePB::PB_RUNNING) {
    brpc::Controller cntl;
    CreateTabletsRequest req;
    CreateTabletsResponse res;
    req.set_db_id(db_id);
    req.set_cloud_unique_id(cloud_unique_id);
    add_tablet(req, table_id, index_id, partition_id, tablet_id, mow, state);
    meta_service->create_tablets(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << tablet_id;
}

void begin_txn(MetaServiceProxy* meta_service, const std::string& cloud_unique_id, int64_t db_id,
               const std::string& label, int64_t table_id, int64_t& txn_id) {
    brpc::Controller cntl;
    BeginTxnRequest req;
    BeginTxnResponse res;
    req.set_cloud_unique_id(cloud_unique_id);
    auto txn_info = req.mutable_txn_info();
    txn_info->set_db_id(db_id);
    txn_info->set_label(label);
    txn_info->add_table_ids(table_id);
    txn_info->set_timeout_ms(36000);
    meta_service->begin_txn(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
    ASSERT_TRUE(res.has_txn_id()) << label;
    txn_id = res.txn_id();
}

void commit_txn(MetaServiceProxy* meta_service, const std::string& cloud_unique_id, int64_t db_id,
                int64_t txn_id, const std::string& label) {
    brpc::Controller cntl;
    CommitTxnRequest req;
    CommitTxnResponse res;
    req.set_cloud_unique_id(cloud_unique_id);
    req.set_db_id(db_id);
    req.set_txn_id(txn_id);
    meta_service->commit_txn(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
}

doris::RowsetMetaCloudPB create_rowset(int64_t txn_id, int64_t tablet_id, int partition_id = 10,
                                       int64_t version = -1, int num_rows = 100) {
    doris::RowsetMetaCloudPB rowset;
    rowset.set_rowset_id(0); // required
    rowset.set_rowset_id_v2(next_rowset_id());
    rowset.set_tablet_id(tablet_id);
    rowset.set_partition_id(partition_id);
    rowset.set_txn_id(txn_id);
    if (version > 0) {
        rowset.set_start_version(version);
        rowset.set_end_version(version);
    }
    rowset.set_resource_id(std::string(RESOURCE_ID));
    rowset.set_num_segments(1);
    rowset.set_num_rows(num_rows);
    rowset.set_data_disk_size(num_rows * DATA_DISK_SIZE_CONST);
    rowset.set_index_disk_size(num_rows * INDEX_DISK_SIZE_CONST);
    rowset.set_total_disk_size(num_rows * DISK_SIZE_CONST);
    rowset.mutable_tablet_schema()->set_schema_version(0);
    rowset.set_txn_expiration(::time(nullptr)); // Required by DCHECK
    auto* key_bounds = rowset.add_segments_key_bounds();
    key_bounds->set_min_key("a");
    key_bounds->set_max_key("z");
    return rowset;
}

void prepare_rowset(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                    const doris::RowsetMetaCloudPB& rowset) {
    brpc::Controller cntl;
    CreateRowsetRequest req;
    CreateRowsetResponse resp;
    req.set_cloud_unique_id(cloud_unique_id);
    req.mutable_rowset_meta()->CopyFrom(rowset);
    meta_service->prepare_rowset(&cntl, &req, &resp, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(resp.status().code(), MetaServiceCode::OK) << rowset.ShortDebugString();
}

void commit_rowset(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                   const doris::RowsetMetaCloudPB& rowset) {
    brpc::Controller cntl;
    CreateRowsetRequest req;
    CreateRowsetResponse resp;
    req.set_cloud_unique_id(cloud_unique_id);
    req.mutable_rowset_meta()->CopyFrom(rowset);
    meta_service->commit_rowset(&cntl, &req, &resp, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(resp.status().code(), MetaServiceCode::OK) << rowset.ShortDebugString();
}

void insert_rowset(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                   int64_t db_id, const std::string& label, int64_t table_id, int64_t partition_id,
                   int64_t tablet_id, std::string* rowset_id = nullptr) {
    int64_t txn_id = 0;
    ASSERT_NO_FATAL_FAILURE(
            begin_txn(meta_service, cloud_unique_id, db_id, label, table_id, txn_id));
    auto rowset = create_rowset(txn_id, tablet_id, partition_id);
    if (rowset_id) {
        *rowset_id = rowset.rowset_id_v2();
    }
    ASSERT_NO_FATAL_FAILURE(prepare_rowset(meta_service, cloud_unique_id, rowset));
    ASSERT_NO_FATAL_FAILURE(commit_rowset(meta_service, cloud_unique_id, rowset));
    ASSERT_NO_FATAL_FAILURE(commit_txn(meta_service, cloud_unique_id, db_id, txn_id, label));
}

void get_tablet_stats(MetaService* meta_service, const std::string& cloud_unique_id,
                      int64_t tablet_id, TabletStatsPB& stats) {
    brpc::Controller cntl;
    GetTabletStatsRequest req;
    GetTabletStatsResponse res;
    req.set_cloud_unique_id(cloud_unique_id);
    auto idx = req.add_tablet_idx();
    idx->set_tablet_id(tablet_id);
    meta_service->get_tablet_stats(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK)
            << tablet_id << ", Response: " << res.ShortDebugString();
    stats = res.tablet_stats(0);
}

struct SnapshotContext {
    std::string snapshot_id;
    std::string image_url;
    std::string label;
    ObjectStoreInfoPB store_info;
    int derived_instance_size;
};

void begin_snapshot(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                    const std::string& snapshot_label, SnapshotContext* ctx,
                    bool auto_snapshot = true) {
    BeginSnapshotRequest req;
    req.set_cloud_unique_id(cloud_unique_id);
    req.set_auto_snapshot(auto_snapshot);
    req.set_timeout_seconds(12);
    req.set_ttl_seconds(3600);
    req.set_request_ip("127.0.0.1");
    req.set_snapshot_label(snapshot_label);

    brpc::Controller cnt;
    BeginSnapshotResponse resp;
    meta_service->begin_snapshot(&cnt, &req, &resp, brpc::DoNothing());

    ASSERT_FALSE(cnt.Failed()) << cnt.ErrorText();
    ASSERT_EQ(resp.status().code(), MetaServiceCode::OK) << resp.ShortDebugString();

    ctx->snapshot_id = resp.snapshot_id();
    ctx->image_url = resp.image_url();
    ctx->store_info = resp.obj_info();
    ctx->label = snapshot_label;
}

void commit_snapshot(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                     const std::string& snapshot_id, const std::string& image_url,
                     int64_t last_journal_id) {
    CommitSnapshotRequest req;
    req.set_cloud_unique_id(cloud_unique_id);
    req.set_snapshot_id(snapshot_id);
    req.set_image_url(image_url);
    req.set_last_journal_id(last_journal_id);
    req.set_request_ip("127.0.0.1");

    brpc::Controller cntl;
    CommitSnapshotResponse res;
    meta_service->commit_snapshot(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.ShortDebugString();
}

void list_snapshot(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                   std::vector<SnapshotContext>* snapshots,
                   const std::string& required_snapshot_id = "", int expected_num = -1,
                   bool include_aborted = false) {
    ListSnapshotRequest req;
    req.set_cloud_unique_id(cloud_unique_id);
    if (!required_snapshot_id.empty()) {
        req.set_required_snapshot_id(required_snapshot_id);
    }
    if (include_aborted) {
        req.set_include_aborted(include_aborted);
    }

    brpc::Controller cntl;
    ListSnapshotResponse res;
    meta_service->list_snapshot(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.ShortDebugString();
    if (expected_num >= 0) {
        ASSERT_EQ(expected_num, res.snapshots().size())
                << ", cloud_unique_id=" << cloud_unique_id
                << ", required_snapshot_id=" << required_snapshot_id
                << ", include_aborted=" << include_aborted;
    }
    if (!snapshots) {
        return;
    }
    snapshots->clear();
    for (auto snapshot : res.snapshots()) {
        SnapshotContext ctx;
        ctx.snapshot_id = snapshot.snapshot_id();
        ctx.image_url = snapshot.image_url();
        ctx.label = snapshot.snapshot_label();
        ctx.derived_instance_size = snapshot.derived_instance_ids_size();
        snapshots->emplace_back(ctx);
    }
}

void list_snapshot(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                   const std::string& required_snapshot_id, int expected_num,
                   bool include_aborted = false) {
    list_snapshot(meta_service, cloud_unique_id, nullptr, required_snapshot_id, expected_num,
                  include_aborted);
}

void drop_snapshot(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                   const std::string& snapshot_id) {
    brpc::Controller cntl;
    DropSnapshotRequest req;
    req.set_cloud_unique_id(cloud_unique_id);
    req.set_snapshot_id(snapshot_id);
    DropSnapshotResponse res;
    meta_service->drop_snapshot(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
}

void clone_instance(
        MetaServiceProxy* meta_service, const std::string& from_instance_id,
        const std::string& snapshot_id, const std::string& clone_instance_id,
        CloneInstanceRequest::CloneType clone_type = CloneInstanceRequest_CloneType_READ_ONLY) {
    CloneInstanceRequest req;
    req.set_clone_type(clone_type);
    if (clone_type == CloneInstanceRequest_CloneType_WRITABLE) {
        auto* obj_info = req.mutable_obj_info();
        obj_info->set_id("2");
        obj_info->set_ak("mock_ak");
        obj_info->set_sk("mock_sk");
        obj_info->set_endpoint("e");
        obj_info->set_region("r");
        obj_info->set_bucket("b");
        obj_info->set_prefix("");
    }
    req.set_from_instance_id(from_instance_id);
    req.set_from_snapshot_id(snapshot_id);
    req.set_new_instance_id(clone_instance_id);

    brpc::Controller cntl;
    CloneInstanceResponse res;
    meta_service->clone_instance(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.ShortDebugString();
}

void get_rowsets(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                 int64_t tablet_id, int64_t start_version, int64_t end_version,
                 std::vector<doris::RowsetMetaCloudPB>& rowsets) {
    TabletStatsPB stats;
    ASSERT_NO_FATAL_FAILURE(get_tablet_stats(meta_service, cloud_unique_id, tablet_id, stats));

    brpc::Controller cntl;
    GetRowsetRequest req;
    GetRowsetResponse res;
    req.set_cloud_unique_id(cloud_unique_id);
    req.set_base_compaction_cnt(stats.base_compaction_cnt());
    req.set_cumulative_compaction_cnt(stats.cumulative_compaction_cnt());
    if (stats.has_full_compaction_cnt()) {
        req.set_full_compaction_cnt(stats.full_compaction_cnt());
    }
    req.set_cumulative_point(stats.cumulative_point());
    req.set_start_version(start_version);
    req.set_end_version(end_version);
    req.mutable_idx()->set_tablet_id(tablet_id);
    meta_service->get_rowset(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.ShortDebugString();
    for (int i = 0; i < res.rowset_meta_size(); i++) {
        rowsets.push_back(res.rowset_meta(i));
    }
}

void update_snapshot_properties(MetaServiceProxy* meta_service, const std::string& instance_id,
                                bool enable_snapshot, int64_t max_reserved_snapshots,
                                int64_t snapshot_interval_seconds) {
    AlterInstanceRequest req;
    req.set_instance_id(instance_id);
    req.set_op(AlterInstanceRequest::SET_SNAPSHOT_PROPERTY);
    std::string status = enable_snapshot ? "true" : "false";
    req.mutable_properties()->insert(
            {AlterInstanceRequest_SnapshotProperty_Name(
                     AlterInstanceRequest_SnapshotProperty::
                             AlterInstanceRequest_SnapshotProperty_ENABLE_SNAPSHOT),
             status});
    req.mutable_properties()->insert(
            {AlterInstanceRequest_SnapshotProperty_Name(
                     AlterInstanceRequest_SnapshotProperty::
                             AlterInstanceRequest_SnapshotProperty_SNAPSHOT_INTERVAL_SECONDS),
             std::to_string(snapshot_interval_seconds)});
    req.mutable_properties()->insert(
            {AlterInstanceRequest_SnapshotProperty_Name(
                     AlterInstanceRequest_SnapshotProperty::
                             AlterInstanceRequest_SnapshotProperty_MAX_RESERVED_SNAPSHOTS),
             std::to_string(max_reserved_snapshots)});
    req.set_request_ip("127.0.0.1");

    brpc::Controller cntl;
    AlterInstanceResponse res;
    meta_service->alter_instance(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.ShortDebugString();
}

void get_instance(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                  InstanceInfoPB& instance_info) {
    brpc::Controller cntl;
    GetInstanceRequest req;
    GetInstanceResponse res;
    req.set_cloud_unique_id(cloud_unique_id);
    meta_service->get_instance(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.ShortDebugString();
    ASSERT_TRUE(res.has_instance()) << res.ShortDebugString();
    instance_info.CopyFrom(res.instance());
}

void clone_and_refresh_instance(
        MetaServiceProxy* meta_service, ResourceManager* resource_manager,
        const std::string& from_instance_id, const std::string& from_snapshot_id,
        const std::string& to_instance_id, InstanceInfoPB& to_instance,
        CloneInstanceRequest::CloneType clone_type = CloneInstanceRequest_CloneType_READ_ONLY) {
    clone_instance(meta_service, from_instance_id, from_snapshot_id, to_instance_id, clone_type);
    update_snapshot_properties(meta_service, to_instance_id, true, 0, 3660);
    std::string cloud_unique_id = fmt::format("1:{}:0", to_instance_id);
    get_instance(meta_service, cloud_unique_id, to_instance);
    resource_manager->refresh_instance(to_instance_id, to_instance);
}

void begin_and_commit_snapshot(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                               SnapshotContext& ctx, const std::string& snapshot_label = "") {
    begin_snapshot(meta_service, cloud_unique_id,
                   snapshot_label.empty() ? "test_label" : snapshot_label, &ctx, false);
    commit_snapshot(meta_service, cloud_unique_id, ctx.snapshot_id, ctx.image_url, 1000);
}

void begin_and_abort_snapshot(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                              SnapshotContext& ctx, const std::string& snapshot_label = "") {
    begin_snapshot(meta_service, cloud_unique_id,
                   snapshot_label.empty() ? "test_label" : snapshot_label, &ctx, false);
    // Abort the snapshot
    brpc::Controller cntl;
    AbortSnapshotRequest req;
    req.set_cloud_unique_id(cloud_unique_id);
    req.set_snapshot_id(ctx.snapshot_id);
    req.set_reason("Test abort snapshot");
    AbortSnapshotResponse res;
    meta_service->abort_snapshot(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
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
        ASSERT_TRUE(res.image_url().find("snapshot/") != std::string::npos);
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
        ASSERT_TRUE(res.image_url().find("snapshot/") != std::string::npos);
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
        ASSERT_EQ(res.status().code(), MetaServiceCode::TXN_ID_NOT_FOUND) << res.ShortDebugString();
        ASSERT_TRUE(res.status().msg().find("snapshot not found") != std::string::npos);
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

    // Create and commit a snapshot
    std::string referenced_snapshot_id;
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_auto_snapshot(true);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_drop_referenced");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        referenced_snapshot_id = res.snapshot_id();

        // Commit it
        CommitSnapshotRequest commit_req;
        commit_req.set_cloud_unique_id(cloud_unique_id);
        commit_req.set_snapshot_id(referenced_snapshot_id);
        commit_req.set_image_url(res.image_url());
        commit_req.set_last_journal_id(12345);
        CommitSnapshotResponse commit_res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &commit_req, &commit_res, nullptr);
        ASSERT_EQ(commit_res.status().code(), MetaServiceCode::OK);
    }
    // Clone it
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_clone_type(CloneInstanceRequest::READ_ONLY);
        req.set_from_instance_id("test_instance");
        req.set_from_snapshot_id(referenced_snapshot_id);
        req.set_new_instance_id("readonly_clone");

        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
    // Test drop snapshot which is referenced (should fail)
    {
        brpc::Controller cntl;
        DropSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(referenced_snapshot_id);
        DropSnapshotResponse res;
        meta_service->drop_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find(
                            "cannot drop snapshot that is referenced by other instance") !=
                    std::string::npos);
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
        instance_info.set_multi_version_status(MULTI_VERSION_READ_WRITE);
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
        (*alter_req.mutable_properties())["ENABLE_SNAPSHOT"] = "true";

        meta_service->alter_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &alter_req, &alter_res, nullptr);
        ASSERT_EQ(alter_res.status().code(), MetaServiceCode::OK);
    }

    {
        brpc::Controller cntl;
        AlterInstanceRequest alter_req;
        AlterInstanceResponse alter_res;
        alter_req.set_op(AlterInstanceRequest::SET_SNAPSHOT_PROPERTY);
        alter_req.set_instance_id("test_instance");
        (*alter_req.mutable_properties())["MAX_RESERVED_SNAPSHOTS"] = "0"; // Disable auto snapshot

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
        (*alter_req.mutable_properties())["MAX_RESERVED_SNAPSHOTS"] = "5";

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

TEST(MetaServiceSnapshotTest, CloneInstanceReadOnlyTest) {
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

    DORIS_CLOUD_DEFER {
        sp->disable_processing();
        sp->clear_all_call_backs();
    };

    // Create source instance
    {
        CreateInstanceRequest req;
        CreateInstanceResponse res;
        req.set_instance_id("test_instance");
        req.set_name("test_instance");
        req.set_user_id("test_user");

        auto obj_info = req.mutable_obj_info();
        obj_info->set_ak("source_ak");
        obj_info->set_sk("source_ak");
        obj_info->set_bucket("source_bucket");
        obj_info->set_prefix("source_prefix");
        obj_info->set_endpoint("source_endpoint");
        obj_info->set_region("test_region");
        obj_info->set_external_endpoint("test_external_endpoint");
        obj_info->set_provider(ObjectStoreInfoPB::OSS);
        obj_info->set_id("1");

        brpc::Controller cntl;
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

    // Create and commit a snapshot
    std::string snapshot_id;
    std::string image_url;
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(1800);
        req.set_ttl_seconds(14400);
        req.set_snapshot_label("test_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        snapshot_id = res.snapshot_id();

        // Commit the snapshot
        CommitSnapshotRequest commit_req;
        commit_req.set_cloud_unique_id(cloud_unique_id);
        commit_req.set_snapshot_id(snapshot_id);
        image_url = "/snapshot/" + snapshot_id + "/";
        commit_req.set_image_url(image_url);
        commit_req.set_last_journal_id(100);
        CommitSnapshotResponse commit_res;
        brpc::Controller commit_cntl;
        meta_service->commit_snapshot(
                reinterpret_cast<::google::protobuf::RpcController*>(&commit_cntl), &commit_req,
                &commit_res, nullptr);
        ASSERT_EQ(commit_res.status().code(), MetaServiceCode::OK);
    }

    // Test missing clone_type
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_from_instance_id("test_instance");
        req.set_from_snapshot_id(snapshot_id);
        req.set_new_instance_id("readonly_clone");

        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find("clone_type") != std::string::npos);
    }

    // Test missing from_instance_id
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_clone_type(CloneInstanceRequest::READ_ONLY);
        req.set_from_snapshot_id(snapshot_id);
        req.set_new_instance_id("readonly_clone");

        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find("from_instance_id") != std::string::npos);
    }

    // Test missing from_snapshot_id
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_clone_type(CloneInstanceRequest::READ_ONLY);
        req.set_from_instance_id("test_instance");
        req.set_new_instance_id("readonly_clone");

        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find("from_snapshot_id") != std::string::npos);
    }

    // Test missing new_instance_id
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_clone_type(CloneInstanceRequest::READ_ONLY);
        req.set_from_instance_id("test_instance");
        req.set_from_snapshot_id(snapshot_id);

        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find("new_instance_id") != std::string::npos);
    }

    // Test non-existent source instance
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_clone_type(CloneInstanceRequest::READ_ONLY);
        req.set_from_instance_id("non_existent_instance");
        req.set_from_snapshot_id(snapshot_id);
        req.set_new_instance_id("readonly_clone");

        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        std::cout << "clone: " << res.DebugString() << std::endl;
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // Test non-existent snapshot (invalid format)
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_clone_type(CloneInstanceRequest::READ_ONLY);
        req.set_from_instance_id("test_instance");
        req.set_from_snapshot_id("invalid_snapshot");
        req.set_new_instance_id("readonly_clone");

        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find("failed to parse") != std::string::npos);
    }

    // Test non-existent snapshot (valid format but not exists)
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_clone_type(CloneInstanceRequest::READ_ONLY);
        req.set_from_instance_id("test_instance");
        req.set_from_snapshot_id("12345678901234567890");
        req.set_new_instance_id("readonly_clone");

        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find("snapshot not found") != std::string::npos);
    }

    // Test successful READ_ONLY clone
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_clone_type(CloneInstanceRequest::READ_ONLY);
        req.set_from_instance_id("test_instance");
        req.set_from_snapshot_id(snapshot_id);
        req.set_new_instance_id("readonly_clone");

        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_FALSE(res.image_url().empty());
        ASSERT_EQ(res.image_url(), image_url);
        ASSERT_TRUE(res.has_obj_info());
        ASSERT_EQ(res.obj_info().id(), "1");
        ASSERT_EQ(res.obj_info().ak(), "source_ak");
        ASSERT_EQ(res.obj_info().bucket(), "source_bucket");
        ASSERT_EQ(res.obj_info().endpoint(), "source_endpoint");
    }

    // Verify cloned instance exists and has correct properties
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string instance_key_str = instance_key("readonly_clone");
        std::string instance_value;
        ASSERT_EQ(txn->get(instance_key_str, &instance_value), TxnErrorCode::TXN_OK);

        InstanceInfoPB instance_info;
        ASSERT_TRUE(instance_info.ParseFromString(instance_value));

        // Verify read-only flag
        ASSERT_TRUE(instance_info.ready_only());

        // Verify source relationship
        ASSERT_EQ(instance_info.source_instance_id(), "test_instance");
        ASSERT_EQ(instance_info.source_snapshot_id(), snapshot_id);

        // Verify storage configuration is inherited from source
        ASSERT_EQ(instance_info.obj_info_size(), 1);
        ASSERT_EQ(instance_info.obj_info(0).id(), "1");
        ASSERT_EQ(instance_info.obj_info(0).ak(), "source_ak");
        ASSERT_EQ(instance_info.obj_info(0).bucket(), "source_bucket");

        // Verify instance metadata
        ASSERT_EQ(instance_info.instance_id(), "readonly_clone");
        ASSERT_EQ(instance_info.user_id(), "test_user");
        ASSERT_EQ(instance_info.status(), InstanceInfoPB::NORMAL);
        ASSERT_TRUE(instance_info.name().find("clone_read_only_") != std::string::npos);

        // Verify snapshot configuration (read-only instance should disable snapshot creation)
        ASSERT_EQ(instance_info.max_reserved_snapshot(), 0);
    }

    // Test idempotent READ_ONLY clone.
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_clone_type(CloneInstanceRequest::READ_ONLY);
        req.set_from_instance_id("test_instance");
        req.set_from_snapshot_id(snapshot_id);
        req.set_new_instance_id("readonly_clone");

        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Test clone to same instance_id but with different snapshot should fail
    {
        // Create another snapshot
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(1800);
        req.set_ttl_seconds(14400);
        req.set_snapshot_label("test_snapshot2");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        std::string snapshot_id2 = res.snapshot_id();

        // Commit the second snapshot
        CommitSnapshotRequest commit_req;
        commit_req.set_cloud_unique_id(cloud_unique_id);
        commit_req.set_snapshot_id(snapshot_id2);
        commit_req.set_image_url("/snapshot/" + snapshot_id2 + "/");
        commit_req.set_last_journal_id(200);
        CommitSnapshotResponse commit_res;
        brpc::Controller commit_cntl;
        meta_service->commit_snapshot(
                reinterpret_cast<::google::protobuf::RpcController*>(&commit_cntl), &commit_req,
                &commit_res, nullptr);
        ASSERT_EQ(commit_res.status().code(), MetaServiceCode::OK);

        // Try to clone with different snapshot to existing instance
        brpc::Controller clone_cntl;
        CloneInstanceRequest clone_req;
        clone_req.set_clone_type(CloneInstanceRequest::READ_ONLY);
        clone_req.set_from_instance_id("test_instance");
        clone_req.set_from_snapshot_id(snapshot_id2);
        clone_req.set_new_instance_id("readonly_clone");

        CloneInstanceResponse clone_res;
        meta_service->clone_instance(
                reinterpret_cast<::google::protobuf::RpcController*>(&clone_cntl), &clone_req,
                &clone_res, nullptr);
        ASSERT_EQ(clone_res.status().code(), MetaServiceCode::ALREADY_EXISTED);
    }
}

TEST(MetaServiceSnapshotTest, CloneInstanceWritableTest) {
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

    DORIS_CLOUD_DEFER {
        sp->disable_processing();
        sp->clear_all_call_backs();
    };

    // Create source instance
    {
        CreateInstanceRequest req;
        CreateInstanceResponse res;
        req.set_instance_id("test_instance");
        req.set_name("test_instance");
        req.set_user_id("test_user");

        auto obj_info = req.mutable_obj_info();
        obj_info->set_ak("source_ak2");
        obj_info->set_sk("source_ak2");
        obj_info->set_bucket("source_bucket2");
        obj_info->set_prefix("source_prefix2");
        obj_info->set_endpoint("source_endpoint2");
        obj_info->set_region("test_region2");
        obj_info->set_external_endpoint("test_external_endpoint");
        obj_info->set_provider(ObjectStoreInfoPB::OSS);
        obj_info->set_id("1");

        brpc::Controller cntl;
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

    // Create and commit a snapshot
    std::string snapshot_id;
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(1800);
        req.set_ttl_seconds(14400);
        req.set_snapshot_label("test_snapshot2");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        snapshot_id = res.snapshot_id();

        // Commit the snapshot
        CommitSnapshotRequest commit_req;
        commit_req.set_cloud_unique_id(cloud_unique_id);
        commit_req.set_snapshot_id(snapshot_id);
        commit_req.set_image_url("/snapshot/" + snapshot_id + "/");
        commit_req.set_last_journal_id(200);
        CommitSnapshotResponse commit_res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &commit_req, &commit_res, nullptr);
        ASSERT_EQ(commit_res.status().code(), MetaServiceCode::OK);
    }

    // Test WRITABLE clone
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_clone_type(CloneInstanceRequest::WRITABLE);
        req.set_from_instance_id("test_instance");
        req.set_from_snapshot_id(snapshot_id);
        req.set_new_instance_id("writable_clone");

        // Set new storage configuration for writable clone
        ObjectStoreInfoPB* obj_info = req.mutable_obj_info();
        obj_info->set_ak("writable_ak");
        obj_info->set_sk("writable_sk");
        obj_info->set_bucket("writable_bucket");
        obj_info->set_prefix("writable_prefix");
        obj_info->set_endpoint("writable_endpoint");
        obj_info->set_region("writable_region");
        obj_info->set_provider(ObjectStoreInfoPB::OSS);

        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_FALSE(res.image_url().empty());
        ASSERT_TRUE(res.has_obj_info());
    }

    // Verify cloned instance exists and has correct properties
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string instance_key_str = instance_key("writable_clone");
        std::string instance_value;
        ASSERT_EQ(txn->get(instance_key_str, &instance_value), TxnErrorCode::TXN_OK);

        InstanceInfoPB instance_info;
        ASSERT_TRUE(instance_info.ParseFromString(instance_value));
        ASSERT_FALSE(instance_info.ready_only());
        ASSERT_EQ(instance_info.source_instance_id(), "test_instance");
        ASSERT_EQ(instance_info.source_snapshot_id(), snapshot_id);
        // Should have inherited obj_info plus new storage configuration
        ASSERT_EQ(instance_info.obj_info_size(), 2);
        ASSERT_EQ(instance_info.resource_ids_size(), 2) << instance_info.ShortDebugString();
    }
}

TEST(MetaServiceSnapshotTest, CloneInstanceRollbackTest) {
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

    DORIS_CLOUD_DEFER {
        sp->disable_processing();
        sp->clear_all_call_backs();
    };

    // Create source instance
    {
        CreateInstanceRequest req;
        CreateInstanceResponse res;
        req.set_instance_id("test_instance");
        req.set_name("test_instance");
        req.set_user_id("test_user");

        auto obj_info = req.mutable_obj_info();
        obj_info->set_ak("source_ak");
        obj_info->set_sk("source_ak");
        obj_info->set_bucket("source_bucket");
        obj_info->set_prefix("source_prefix");
        obj_info->set_endpoint("source_endpoint");
        obj_info->set_region("test_region");
        obj_info->set_external_endpoint("test_external_endpoint");
        obj_info->set_provider(ObjectStoreInfoPB::OSS);
        obj_info->set_id("1");

        brpc::Controller cntl;
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

    // Create and commit a snapshot
    std::string snapshot_id;
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(1800);
        req.set_ttl_seconds(14400);
        req.set_snapshot_label("test_instance");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        snapshot_id = res.snapshot_id();

        // Commit the snapshot
        CommitSnapshotRequest commit_req;
        commit_req.set_cloud_unique_id(cloud_unique_id);
        commit_req.set_snapshot_id(snapshot_id);
        commit_req.set_image_url("/snapshot/" + snapshot_id + "/");
        commit_req.set_last_journal_id(300);
        CommitSnapshotResponse commit_res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &commit_req, &commit_res, nullptr);
        ASSERT_EQ(commit_res.status().code(), MetaServiceCode::OK);
    }

    // Test ROLLBACK clone
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_clone_type(CloneInstanceRequest::ROLLBACK);
        req.set_from_instance_id("test_instance");
        req.set_from_snapshot_id(snapshot_id);
        req.set_new_instance_id("test_instance_rollback"); // Different from source for rollback

        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_FALSE(res.image_url().empty());
        ASSERT_TRUE(res.has_obj_info());
    }

    // Verify rollback created new instance correctly
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string instance_key_str = instance_key("test_instance_rollback");
        std::string instance_value;
        ASSERT_EQ(txn->get(instance_key_str, &instance_value), TxnErrorCode::TXN_OK);

        InstanceInfoPB instance_info;
        ASSERT_TRUE(instance_info.ParseFromString(instance_value));
        ASSERT_EQ(instance_info.source_snapshot_id(), snapshot_id);
        ASSERT_EQ(instance_info.source_instance_id(), "test_instance");
        ASSERT_EQ(instance_info.original_instance_id(), "test_instance");
    }
}

TEST(MetaServiceSnapshotTest, CloneInstanceParameterValidationTest) {
    auto meta_service = get_meta_service(true);

    // Test missing clone_type
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_from_instance_id("source");
        req.set_from_snapshot_id("1234567890abcdef1234");
        req.set_new_instance_id("target");

        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find("clone_type not specified") != std::string::npos);
    }

    // Test missing from_instance_id
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_clone_type(CloneInstanceRequest::READ_ONLY);
        req.set_from_snapshot_id("1234567890abcdef1234");
        req.set_new_instance_id("target");

        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find("from_instance_id not specified") != std::string::npos);
    }

    // Test missing from_snapshot_id
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_clone_type(CloneInstanceRequest::READ_ONLY);
        req.set_from_instance_id("source");
        req.set_new_instance_id("target");

        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find("from_snapshot_id not specified") != std::string::npos);
    }

    // Test missing new_instance_id
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_clone_type(CloneInstanceRequest::READ_ONLY);
        req.set_from_instance_id("source");
        req.set_from_snapshot_id("1234567890abcdef1234");

        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find("new_instance_id not specified") != std::string::npos);
    }

    // Test invalid snapshot ID format (wrong length)
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_clone_type(CloneInstanceRequest::READ_ONLY);
        req.set_from_instance_id("source");
        req.set_from_snapshot_id("invalid_length");
        req.set_new_instance_id("target");

        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find("failed to parse") != std::string::npos);
    }

    // Test invalid snapshot ID format (non-hex characters)
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_clone_type(CloneInstanceRequest::READ_ONLY);
        req.set_from_instance_id("source");
        req.set_from_snapshot_id("123456789gabcdef1234"); // 'g' is not hex
        req.set_new_instance_id("target");

        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find("failed to parse") != std::string::npos);
    }

    // Test WRITABLE without obj_info
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_clone_type(CloneInstanceRequest::WRITABLE);
        req.set_from_instance_id("source");
        req.set_from_snapshot_id("1234567890abcdef1234");
        req.set_new_instance_id("target");

        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find("WRITABLE clone requires obj_info") !=
                    std::string::npos);
    }

    // Test clone with same from_instance_id and new_instance_id
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_clone_type(CloneInstanceRequest::READ_ONLY);
        req.set_from_instance_id("same_instance");
        req.set_from_snapshot_id("1234567890abcdef1234");
        req.set_new_instance_id("same_instance"); // Same as source - should fail

        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(
                res.status().msg().find("from_instance_id and new_instance_id must be different") !=
                std::string::npos);
    }
}

TEST(MetaServiceSnapshotTest, CloneInstanceSnapshotNotFoundTest) {
    auto meta_service = get_meta_service(true);

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

    DORIS_CLOUD_DEFER {
        sp->disable_processing();
        sp->clear_all_call_backs();
    };

    // Create source instance
    {
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

    // Test clone with non-existent snapshot
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_clone_type(CloneInstanceRequest::READ_ONLY);
        req.set_from_instance_id("test_instance");
        req.set_from_snapshot_id("1234567890abcdef1234"); // Non-existent snapshot
        req.set_new_instance_id("target_notfound");

        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(res.status().msg().find("snapshot not found") != std::string::npos);
    }
}

TEST(MetaServiceSnapshotTest, CloneInstanceExistingTargetTest) {
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

    DORIS_CLOUD_DEFER {
        sp->disable_processing();
        sp->clear_all_call_backs();
    };

    // Create source instance and another existing instance
    {
        brpc::Controller cntl;
        CreateInstanceRequest req;
        req.set_instance_id("test_instance");
        req.set_user_id("test_user");
        req.set_name("test_instance");
        auto obj_info = req.mutable_obj_info();
        obj_info->set_ak("ak_test");
        obj_info->set_sk("sk_test");
        obj_info->set_bucket("test_bucket");
        obj_info->set_prefix("test_prefix");
        obj_info->set_endpoint("test_endpoint");
        obj_info->set_region("test_region");
        obj_info->set_external_endpoint("test_external_endpoint");
        obj_info->set_provider(ObjectStoreInfoPB::OSS);
        obj_info->set_id("1");

        CreateInstanceResponse res;
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);

        // Create another instance that will conflict
        req.set_instance_id("existing_target");
        req.set_name("existing_target");
        res.Clear();
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

    // Create and commit a snapshot
    std::string snapshot_id;
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(1800);
        req.set_ttl_seconds(14400);
        req.set_snapshot_label("existing_test_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        snapshot_id = res.snapshot_id();

        // Commit the snapshot
        CommitSnapshotRequest commit_req;
        commit_req.set_cloud_unique_id(cloud_unique_id);
        commit_req.set_snapshot_id(snapshot_id);
        commit_req.set_image_url("/snapshot/" + snapshot_id + "/");
        commit_req.set_last_journal_id(400);
        CommitSnapshotResponse commit_res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &commit_req, &commit_res, nullptr);
        ASSERT_EQ(commit_res.status().code(), MetaServiceCode::OK);
    }

    // Test clone to existing target instance (should fail)
    {
        brpc::Controller cntl;
        CloneInstanceRequest req;
        req.set_clone_type(CloneInstanceRequest::READ_ONLY);
        req.set_from_instance_id("test_instance");
        req.set_from_snapshot_id(snapshot_id);
        req.set_new_instance_id("existing_target"); // This instance already exists

        CloneInstanceResponse res;
        meta_service->clone_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::ALREADY_EXISTED);
        ASSERT_TRUE(res.status().msg().find("already exists") != std::string::npos);
    }
}

TEST(MetaServiceSnapshotTest, GetInstanceWithPredecessorSuccessorTest) {
    auto meta_service = get_meta_service(true);

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

    DORIS_CLOUD_DEFER {
        sp->disable_processing();
        sp->clear_all_call_backs();
    };

    // Create test instance with predecessor and successor
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);

        // Create instance with predecessor and successor
        InstanceInfoPB instance;
        instance.set_instance_id("test_instance");
        instance.set_name("test_instance");
        instance.set_user_id("test_user");
        instance.set_source_instance_id("parent_instance");       // predecessor
        instance.set_successor_instance_id("successor_instance"); // successor

        // Add basic obj_info
        auto* obj = instance.add_obj_info();
        obj->set_ak("test_ak");
        obj->set_sk("test_sk");
        obj->set_bucket("test_bucket");
        obj->set_endpoint("test_endpoint");
        obj->set_provider(ObjectStoreInfoPB::OSS);

        std::string key = instance_key("test_instance");
        std::string value = instance.SerializeAsString();
        txn->put(key, value);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Test get_instance returns predecessor and successor IDs
    {
        std::string instance_id = "test_instance";
        std::string cloud_unique_id = fmt::format("1:{}:0", instance_id);
        brpc::Controller cntl;
        GetInstanceRequest req;
        GetInstanceResponse res;
        req.set_cloud_unique_id(cloud_unique_id);

        meta_service->get_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                   &req, &res, nullptr);

        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_TRUE(res.has_instance());
        EXPECT_EQ(res.instance().instance_id(), "test_instance");

        // Verify predecessor and successor are returned in instance
        ASSERT_TRUE(res.instance().has_source_instance_id());
        EXPECT_EQ(res.instance().source_instance_id(), "parent_instance");

        ASSERT_TRUE(res.instance().has_successor_instance_id());
        EXPECT_EQ(res.instance().successor_instance_id(), "successor_instance");
    }
}

TEST(MetaServiceHttpTest, DropInstanceTest) {
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

    std::string instance_id = "test_instance";
    // Create test instance first
    {
        brpc::Controller cntl;
        CreateInstanceRequest req;
        req.set_instance_id(instance_id);
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
        std::string instance_key_str = instance_key(instance_id);
        std::string instance_value;
        ASSERT_EQ(txn->get(instance_key_str, &instance_value), TxnErrorCode::TXN_OK);
        InstanceInfoPB instance_info;
        ASSERT_TRUE(instance_info.ParseFromString(instance_value));
        instance_info.set_snapshot_switch_status(SnapshotSwitchStatus::SNAPSHOT_SWITCH_ON);
        txn->put(instance_key_str, instance_info.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Begin snapshot
    std::string snapshot_id = "";
    {
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_auto_snapshot(false);
        req.set_timeout_seconds(12);
        req.set_ttl_seconds(3600);
        req.set_request_ip("127.0.0.1");
        req.set_snapshot_label("snapshot_label");

        brpc::Controller cnt;
        BeginSnapshotResponse resp;
        meta_service->begin_snapshot(&cnt, &req, &resp, brpc::DoNothing());
        ASSERT_FALSE(cnt.Failed()) << cnt.ErrorText();
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK) << resp.ShortDebugString();
        snapshot_id = resp.snapshot_id();
    }

    // Commit snapshot
    {
        CommitSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        req.set_image_url("/test/snapshot/url");
        req.set_last_journal_id(100);
        req.set_request_ip("127.0.0.1");

        brpc::Controller cntl;
        CommitSnapshotResponse res;
        meta_service->commit_snapshot(&cntl, &req, &res, nullptr);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.ShortDebugString();
    }

    // Cannot drop instance because it has snapshots
    {
        brpc::Controller cntl;
        AlterInstanceRequest req;
        AlterInstanceResponse res;
        req.set_instance_id(instance_id);
        req.set_op(AlterInstanceRequest::DROP);
        meta_service->alter_instance(&cntl, &req, &res, nullptr);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.ShortDebugString();
        ASSERT_TRUE(res.status().msg().find("instance has snapshots") != std::string::npos);
    }

    // Drop snapshot
    {
        brpc::Controller cntl;
        DropSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_snapshot_id(snapshot_id);
        DropSnapshotResponse res;
        meta_service->drop_snapshot(&cntl, &req, &res, nullptr);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Drop instance
    {
        brpc::Controller cntl;
        AlterInstanceRequest req;
        AlterInstanceResponse res;
        req.set_instance_id(instance_id);
        req.set_op(AlterInstanceRequest::DROP);
        meta_service->alter_instance(&cntl, &req, &res, nullptr);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.ShortDebugString();
    }
}

TEST(MetaServiceHttpTest, RollbackListSnapshotTest) {
    auto meta_service = get_meta_service(false);
    auto resource_mgr = meta_service->resource_mgr();

    // Step1: create instance1
    std::string instance_id1 = "rollback_list_snapshot_test_instance1";
    std::string cloud_unique_id1 = fmt::format("1:{}:0", instance_id1);
    create_and_refresh_instance(meta_service.get(), instance_id1);
    InstanceInfoPB instance_info1;
    get_instance(meta_service.get(), cloud_unique_id1, instance_info1);

    // create partition/index/tablet
    int64_t db_id = 1, table_id = 2, index_id = 3, partition_id = 4, tablet_id = 5;
    prepare_and_commit_index(meta_service.get(), cloud_unique_id1, db_id, table_id, index_id);
    prepare_and_commit_partition(meta_service.get(), cloud_unique_id1, db_id, table_id,
                                 partition_id, index_id);
    create_tablet(meta_service.get(), cloud_unique_id1, db_id, table_id, index_id, partition_id,
                  tablet_id);

    // insert 5 rowsets (version 2-6); create snapshot_1_1 for instance1
    for (int i = 2; i <= 6; i++) {
        insert_rowset(meta_service.get(), cloud_unique_id1, db_id, fmt::format("label_{}", i),
                      table_id, partition_id, tablet_id, nullptr);
    }
    SnapshotContext snapshot_1_1;
    begin_and_commit_snapshot(meta_service.get(), cloud_unique_id1, snapshot_1_1, "snapshot_1_1");

    // insert 5 rowsets (version 7-11); abort snapshot_1_2 for instance1
    for (int i = 7; i <= 11; i++) {
        insert_rowset(meta_service.get(), cloud_unique_id1, db_id, fmt::format("label_{}", i),
                      table_id, partition_id, tablet_id, nullptr);
    }
    SnapshotContext snapshot_1_2;
    begin_and_abort_snapshot(meta_service.get(), cloud_unique_id1, snapshot_1_2, "snapshot_1_2");

    // insert 2 rowsets (version 12-13); create snapshot_1_3 for instance1
    for (int i = 12; i <= 13; i++) {
        insert_rowset(meta_service.get(), cloud_unique_id1, db_id, fmt::format("label_{}", i),
                      table_id, partition_id, tablet_id, nullptr);
    }
    SnapshotContext snapshot_1_3;
    begin_and_commit_snapshot(meta_service.get(), cloud_unique_id1, snapshot_1_3, "snapshot_1_3");

    // insert 4 rowsets (version 14-17); create snapshot_1_4 for instance1
    for (int i = 14; i <= 17; i++) {
        insert_rowset(meta_service.get(), cloud_unique_id1, db_id, fmt::format("label_{}", i),
                      table_id, partition_id, tablet_id, nullptr);
    }
    SnapshotContext snapshot_1_4;
    begin_and_commit_snapshot(meta_service.get(), cloud_unique_id1, snapshot_1_4, "snapshot_1_4");
    {
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id1, tablet_id, 0, 17, rowsets);
        ASSERT_EQ(rowsets.size(), 17);
    }

    // Step2: instance1 rollback to instance2
    std::string instance_id2 = "rollback_list_snapshot_test_instance2";
    std::string cloud_unique_id2 = fmt::format("1:{}:0", instance_id2);
    InstanceInfoPB instance_info2;
    clone_and_refresh_instance(meta_service.get(), resource_mgr.get(), instance_id1,
                               snapshot_1_1.snapshot_id, instance_id2, instance_info2,
                               CloneInstanceRequest_CloneType_ROLLBACK);
    get_instance(meta_service.get(), cloud_unique_id1, instance_info1);
    {
        ASSERT_EQ(instance_info1.successor_instance_id(), instance_id2);
        ASSERT_EQ(instance_info1.original_instance_id(), "");
        ASSERT_EQ(instance_info1.source_instance_id(), "");

        ASSERT_EQ(instance_info2.successor_instance_id(), "");
        ASSERT_EQ(instance_info2.original_instance_id(), instance_id1);
        ASSERT_EQ(instance_info2.source_instance_id(), instance_id1);
    }
    // get rowsets for instance2
    {
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id2, tablet_id, 0, 6, rowsets);
        ASSERT_EQ(rowsets.size(), 6);
    }

    // list snapshot
    for (auto& cloud_unique_id : {cloud_unique_id1, cloud_unique_id2}) {
        list_snapshot(meta_service.get(), cloud_unique_id, "", 3);
        list_snapshot(meta_service.get(), cloud_unique_id, "", 4, true);
        list_snapshot(meta_service.get(), cloud_unique_id, snapshot_1_1.snapshot_id, 1);
        list_snapshot(meta_service.get(), cloud_unique_id, snapshot_1_2.snapshot_id, 0);
        list_snapshot(meta_service.get(), cloud_unique_id, snapshot_1_2.snapshot_id, 1, true);
    }

    // instance2 drop snapshot_1_4
    drop_snapshot(meta_service.get(), cloud_unique_id2, snapshot_1_4.snapshot_id);
    for (auto& cloud_unique_id : {cloud_unique_id1, cloud_unique_id2}) {
        list_snapshot(meta_service.get(), cloud_unique_id, "", 2);
        list_snapshot(meta_service.get(), cloud_unique_id, "", 3, true);
    }

    // insert 2 rowsets (version 7-8); create snapshot_2_1 for instance2
    for (int i = 7; i <= 8; i++) {
        insert_rowset(meta_service.get(), cloud_unique_id2, db_id, fmt::format("label_{}", i),
                      table_id, partition_id, tablet_id, nullptr);
    }
    SnapshotContext snapshot_2_1;
    begin_and_commit_snapshot(meta_service.get(), cloud_unique_id2, snapshot_2_1, "snapshot_2_1");

    // insert 3 rowsets (version 9-11); create snapshot_2_2 for instance2
    for (int i = 9; i <= 11; i++) {
        insert_rowset(meta_service.get(), cloud_unique_id2, db_id, fmt::format("label_{}", i),
                      table_id, partition_id, tablet_id, nullptr);
    }
    SnapshotContext snapshot_2_2;
    begin_and_abort_snapshot(meta_service.get(), cloud_unique_id2, snapshot_2_2);

    // get rowsets for instance2
    {
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id2, tablet_id, 0, 11, rowsets);
        ASSERT_EQ(rowsets.size(), 11);
    }

    // list snapshot
    // all
    list_snapshot(meta_service.get(), cloud_unique_id1, "", 2);
    list_snapshot(meta_service.get(), cloud_unique_id2, "", 3);
    list_snapshot(meta_service.get(), cloud_unique_id2, "", 5, true);
    // normal
    list_snapshot(meta_service.get(), cloud_unique_id2, snapshot_1_1.snapshot_id, 1);
    list_snapshot(meta_service.get(), cloud_unique_id2, snapshot_2_1.snapshot_id, 1);
    // dropped
    list_snapshot(meta_service.get(), cloud_unique_id2, snapshot_1_4.snapshot_id, 0, true);
    // aborted
    list_snapshot(meta_service.get(), cloud_unique_id2, snapshot_1_2.snapshot_id, 0);
    list_snapshot(meta_service.get(), cloud_unique_id2, snapshot_2_2.snapshot_id, 0);
    list_snapshot(meta_service.get(), cloud_unique_id2, snapshot_1_2.snapshot_id, 1, true);
    list_snapshot(meta_service.get(), cloud_unique_id2, snapshot_2_2.snapshot_id, 1, true);
    // invalid
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id2);
        req.set_required_snapshot_id("test");
        ListSnapshotResponse res;
        meta_service->list_snapshot(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // Step3: rollback instance2 to instance3: using snapshot_1_3 from instance1
    std::string instance_id3 = "rollback_list_snapshot_test_instance3";
    std::string cloud_unique_id3 = fmt::format("1:{}:0", instance_id3);
    InstanceInfoPB instance_info3;
    clone_and_refresh_instance(meta_service.get(), resource_mgr.get(), instance_id2,
                               snapshot_1_3.snapshot_id, instance_id3, instance_info3,
                               CloneInstanceRequest_CloneType_ROLLBACK);
    get_instance(meta_service.get(), cloud_unique_id1, instance_info1);
    get_instance(meta_service.get(), cloud_unique_id2, instance_info2);
    {
        ASSERT_EQ(instance_info1.successor_instance_id(), instance_id2);
        ASSERT_EQ(instance_info1.original_instance_id(), "");
        ASSERT_EQ(instance_info1.source_instance_id(), "");

        ASSERT_EQ(instance_info2.successor_instance_id(), instance_id3);
        ASSERT_EQ(instance_info2.original_instance_id(), instance_id1);
        ASSERT_EQ(instance_info2.source_instance_id(), instance_id1);

        ASSERT_EQ(instance_info3.successor_instance_id(), "");
        ASSERT_EQ(instance_info3.original_instance_id(), instance_id1);
        // NOTE:
        ASSERT_EQ(instance_info3.source_instance_id(), instance_id1);
    }
    // get rowsets for instance3
    {
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id3, tablet_id, 0, 13, rowsets);
        ASSERT_EQ(rowsets.size(), 13);
    }

    // list snapshot
    // all
    std::vector<SnapshotContext> snapshots;
    list_snapshot(meta_service.get(), cloud_unique_id3, &snapshots, "", 3);
    list_snapshot(meta_service.get(), cloud_unique_id3, "", 5, true);
    // normal
    list_snapshot(meta_service.get(), cloud_unique_id3, snapshot_1_1.snapshot_id, 1);
    list_snapshot(meta_service.get(), cloud_unique_id3, snapshot_2_1.snapshot_id, 1);
    // dropped
    list_snapshot(meta_service.get(), cloud_unique_id3, snapshot_1_4.snapshot_id, 0, true);
    // aborted
    list_snapshot(meta_service.get(), cloud_unique_id3, snapshot_1_2.snapshot_id, 0);
    list_snapshot(meta_service.get(), cloud_unique_id3, snapshot_2_2.snapshot_id, 0);
    list_snapshot(meta_service.get(), cloud_unique_id3, snapshot_1_2.snapshot_id, 1, true);
    list_snapshot(meta_service.get(), cloud_unique_id3, snapshot_2_2.snapshot_id, 1, true);
    // check derived instance size
    for (auto& snapshot : snapshots) {
        if (snapshot.snapshot_id == snapshot_1_1.snapshot_id ||
            snapshot.snapshot_id == snapshot_1_3.snapshot_id) {
            ASSERT_EQ(1, snapshot.derived_instance_size);
        } else {
            ASSERT_EQ(0, snapshot.derived_instance_size) << ", label=" << snapshot.label;
        }
    }

    // Step4: clone instance2 to instance4: using snapshot_1_3 from instance1
    std::string instance_id4 = "snapshot_chain_compactor_test_instance4";
    std::string cloud_unique_id4 = fmt::format("1:{}:0", instance_id4);
    InstanceInfoPB instance_info4;
    clone_and_refresh_instance(meta_service.get(), resource_mgr.get(), instance_id2,
                               snapshot_1_3.snapshot_id, instance_id4, instance_info4,
                               CloneInstanceRequest_CloneType_READ_ONLY);
    get_instance(meta_service.get(), cloud_unique_id2, instance_info2);
    {
        ASSERT_EQ(instance_info2.successor_instance_id(), instance_id3);
        ASSERT_EQ(instance_info2.original_instance_id(), instance_id1);
        ASSERT_EQ(instance_info2.source_instance_id(), instance_id1);

        ASSERT_EQ(instance_info4.successor_instance_id(), "");
        ASSERT_EQ(instance_info4.original_instance_id(), "");
        // NOTE:
        ASSERT_EQ(instance_info4.source_instance_id(), instance_id1);
    }
    {
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id4, tablet_id, 0, 13, rowsets);
        ASSERT_EQ(rowsets.size(), 13);
    }

    // Step5: clone instance2 to instance5: using snapshot_2_1 from instance2
    std::string instance_id5 = "snapshot_chain_compactor_test_instance5";
    std::string cloud_unique_id5 = fmt::format("1:{}:0", instance_id5);
    InstanceInfoPB instance_info5;
    clone_and_refresh_instance(meta_service.get(), resource_mgr.get(), instance_id2,
                               snapshot_2_1.snapshot_id, instance_id5, instance_info5,
                               CloneInstanceRequest_CloneType_WRITABLE);
    get_instance(meta_service.get(), cloud_unique_id2, instance_info2);
    {
        ASSERT_EQ(instance_info2.successor_instance_id(), instance_id3);
        ASSERT_EQ(instance_info2.original_instance_id(), instance_id1);
        ASSERT_EQ(instance_info2.source_instance_id(), instance_id1);

        ASSERT_EQ(instance_info5.successor_instance_id(), "");
        ASSERT_EQ(instance_info5.original_instance_id(), "");
        ASSERT_EQ(instance_info5.source_instance_id(), instance_id2);
    }
    {
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id5, tablet_id, 0, 8, rowsets);
        ASSERT_EQ(rowsets.size(), 8);
    }
}

} // namespace doris::cloud
