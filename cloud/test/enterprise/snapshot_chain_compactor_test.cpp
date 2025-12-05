#include "recycler/snapshot_chain_compactor.h"

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <butil/strings/string_split.h>
#include <fmt/core.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>
#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <ranges>
#include <string>
#include <thread>

#include "common/config.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "enterprise/snapshot/snapshot_helper.h"
#include "enterprise/snapshot/snapshot_manager.h"
#include "meta-service/meta_service.h"
#include "meta-service/meta_service_http.h"
#include "meta-store/blob_message.h"
#include "meta-store/clone_chain_reader.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/meta_reader.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
#include "mock_accessor.h"
#include "rate-limiter/rate_limiter.h"
#include "recycler/checker.h"
#include "recycler/recycler.h"
#include "recycler/storage_vault_accessor.h"
#include "recycler/util.h"

using namespace doris;
using namespace doris::cloud;

doris::cloud::RecyclerThreadPoolGroup thread_group;

int main(int argc, char** argv) {
    auto conf_file = "doris_cloud.conf";
    if (!cloud::config::init(conf_file, true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }
    if (!cloud::init_glog("enterprise_compact_snapshot_keys_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }

    using namespace std::chrono;
    config::recycler_sleep_before_scheduling_seconds = 0; // we dont have to wait in UT
    config::enable_multi_version_status = false;  // Disable default multi-version status in UT
    config::enable_split_tablet_schema_pb = true; // Enable split tablet schema in UT
    config::enable_split_rowset_meta_pb = true;   // Enable split rowset meta in UT
    config::split_rowset_meta_pb_size = 1;
    config::split_tablet_schema_pb_size = 1;

    ::testing::InitGoogleTest(&argc, argv);
    auto s3_producer_pool = std::make_shared<SimpleThreadPool>(config::recycle_pool_parallelism);
    s3_producer_pool->start();
    auto recycle_tablet_pool = std::make_shared<SimpleThreadPool>(config::recycle_pool_parallelism);
    recycle_tablet_pool->start();
    auto group_recycle_function_pool =
            std::make_shared<SimpleThreadPool>(config::recycle_pool_parallelism);
    group_recycle_function_pool->start();
    thread_group =
            RecyclerThreadPoolGroup(std::move(s3_producer_pool), std::move(recycle_tablet_pool),
                                    std::move(group_recycle_function_pool));
    return RUN_ALL_TESTS();
}

constexpr int DATA_DISK_SIZE_CONST = 100;
constexpr int INDEX_DISK_SIZE_CONST = 10;
constexpr int DISK_SIZE_CONST = 110;
constexpr std::string_view RESOURCE_ID = "mock_resource_id";

std::unique_ptr<MetaServiceProxy> get_meta_service() {
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

    auto rs = std::make_shared<ResourceManager>(txn_kv);
    auto rl = std::make_shared<RateLimiter>();
    auto snapshot = std::make_shared<selectdb::SnapshotManager>(txn_kv);
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs, rl, snapshot);
    return std::make_unique<MetaServiceProxy>(std::move(meta_service));
}

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

void drop_index(MetaServiceProxy* service, const std::string& cloud_unique_id, int64_t db_id,
                int64_t table_id, int64_t index_id) {
    IndexRequest request;
    request.set_cloud_unique_id(cloud_unique_id);
    request.set_db_id(db_id);
    request.set_table_id(table_id);
    request.add_index_ids(index_id);

    IndexResponse response;
    brpc::Controller cntl;
    service->drop_index(&cntl, &request, &response, nullptr);
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

void drop_partition(MetaServiceProxy* service, const std::string& cloud_unique_id, int64_t db_id,
                    int64_t table_id, int64_t partition_id, int64_t index_id) {
    PartitionRequest request;
    request.set_cloud_unique_id(cloud_unique_id);
    request.set_db_id(db_id);
    request.set_table_id(table_id);
    request.add_partition_ids(partition_id);
    request.add_index_ids(index_id);

    PartitionResponse response;
    brpc::Controller cntl;
    service->drop_partition(&cntl, &request, &response, nullptr);
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

void get_tablet_meta(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                     int64_t tablet_id, TabletMetaCloudPB* tablet_meta) {
    brpc::Controller cntl;
    GetTabletRequest req;
    GetTabletResponse res;
    req.set_cloud_unique_id(cloud_unique_id);
    req.set_tablet_id(tablet_id);
    meta_service->get_tablet(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.ShortDebugString();
    ASSERT_TRUE(res.has_tablet_meta()) << tablet_id;
    tablet_meta->CopyFrom(res.tablet_meta());
}

void get_partition_version(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                           int64_t db_id, int64_t table_id, int64_t partition_id,
                           int64_t* version) {
    brpc::Controller cntl;
    GetVersionRequest req;
    GetVersionResponse res;
    req.set_cloud_unique_id(cloud_unique_id);
    req.set_db_id(db_id);
    req.set_table_id(table_id);
    req.set_partition_id(partition_id);
    meta_service->get_version(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << partition_id;
    *version = res.version();
}

void get_table_version(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                       int64_t db_id, int64_t table_id, int64_t* version) {
    brpc::Controller cntl;
    GetVersionRequest req;
    GetVersionResponse res;
    req.set_cloud_unique_id(cloud_unique_id);
    req.set_is_table_version(true);
    req.set_db_id(db_id);
    req.set_table_id(table_id);
    meta_service->get_version(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.ShortDebugString();
    *version = res.version();
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

void get_delete_bitmap_lock(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                            int64_t table_id, int64_t partition_id, int64_t lock_id,
                            int64_t initiator) {
    brpc::Controller cntl;
    GetDeleteBitmapUpdateLockRequest req;
    GetDeleteBitmapUpdateLockResponse res;
    req.set_cloud_unique_id(cloud_unique_id);
    req.set_table_id(table_id);
    req.add_partition_ids(partition_id);
    req.set_expiration(10);
    req.set_lock_id(lock_id);
    req.set_initiator(initiator);
    meta_service->get_delete_bitmap_update_lock(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
}

void remove_delete_bitmap_lock(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                               int64_t table_id, int64_t lock_id, int64_t initiator) {
    brpc::Controller cntl;
    RemoveDeleteBitmapUpdateLockRequest req;
    RemoveDeleteBitmapUpdateLockResponse res;
    req.set_cloud_unique_id(cloud_unique_id);
    req.set_table_id(table_id);
    req.set_lock_id(lock_id);
    req.set_initiator(initiator);
    meta_service->remove_delete_bitmap_update_lock(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
}

void update_delete_bitmap(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                          int64_t table_id, int64_t partition_id, int64_t tablet_id,
                          int64_t lock_id, int64_t initiator, const std::string& rowset_id) {
    brpc::Controller cntl;
    UpdateDeleteBitmapRequest req;
    UpdateDeleteBitmapResponse res;
    req.set_cloud_unique_id(cloud_unique_id);
    req.set_table_id(table_id);
    req.set_partition_id(partition_id);
    req.set_lock_id(lock_id);
    req.set_initiator(initiator);
    req.set_tablet_id(tablet_id);
    req.set_store_version(2);

    DeleteBitmapPB delete_bitmap_pb;
    auto num = 10;
    for (int i = 0; i < num; i++) {
        delete_bitmap_pb.add_rowset_ids(rowset_id);
        delete_bitmap_pb.add_segment_ids(0);
        delete_bitmap_pb.add_versions(i);
        delete_bitmap_pb.add_segment_delete_bitmaps("1");
    }
    DeleteBitmapStoragePB delete_bitmap_storage_pb;
    delete_bitmap_storage_pb.set_store_in_fdb(true);
    *(delete_bitmap_storage_pb.mutable_delete_bitmap()) = std::move(delete_bitmap_pb);
    *(req.add_delete_bitmap_storages()) = std::move(delete_bitmap_storage_pb);
    req.add_delta_rowset_ids(rowset_id);
    meta_service->update_delete_bitmap(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
}

void update_delete_bitmap(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                          int64_t table_id, int64_t partition_id, int64_t tablet_id,
                          const std::string& rowset_id) {
    int64_t lock_id = -1;
    int64_t initiator = 1234;
    ASSERT_NO_FATAL_FAILURE(get_delete_bitmap_lock(meta_service, cloud_unique_id, table_id,
                                                   partition_id, lock_id, initiator));
    ASSERT_NO_FATAL_FAILURE(update_delete_bitmap(meta_service, cloud_unique_id, table_id,
                                                 partition_id, tablet_id, lock_id, initiator,
                                                 rowset_id));
    ASSERT_NO_FATAL_FAILURE(
            remove_delete_bitmap_lock(meta_service, cloud_unique_id, table_id, lock_id, initiator));
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
                   int64_t tablet_id, std::string* rowset_id = nullptr,
                   StorageVaultAccessor* accessor = nullptr) {
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
    if (accessor) {
        for (int i = 0; i < 1; ++i) {
            auto path = doris::cloud::segment_path(rowset.tablet_id(), rowset.rowset_id_v2(), i);
            accessor->put_file(path, "");
        }
    }
}

void insert_rowsets(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                    int64_t db_id, const std::string& label, int64_t table_id, int64_t partition_id,
                    std::vector<int64_t> tablet_ids) {
    int64_t txn_id = 0;
    ASSERT_NO_FATAL_FAILURE(
            begin_txn(meta_service, cloud_unique_id, db_id, label, table_id, txn_id));
    for (auto tablet_id : tablet_ids) {
        auto rowset = create_rowset(txn_id, tablet_id, partition_id);
        ASSERT_NO_FATAL_FAILURE(prepare_rowset(meta_service, cloud_unique_id, rowset));
        ASSERT_NO_FATAL_FAILURE(commit_rowset(meta_service, cloud_unique_id, rowset));
    }
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

void start_compaction_job(MetaService* meta_service, const std::string& cloud_unique_id,
                          int64_t tablet_id, const std::string& job_id,
                          const std::string& initiator, int base_compaction_cnt,
                          int cumu_compaction_cnt, TabletCompactionJobPB::CompactionType type,
                          std::pair<int64_t, int64_t> input_version = {0, 0}) {
    brpc::Controller cntl;
    StartTabletJobRequest req;
    StartTabletJobResponse res;
    req.set_cloud_unique_id(cloud_unique_id);
    req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
    auto compaction = req.mutable_job()->add_compaction();
    compaction->set_id(job_id);
    compaction->set_initiator(initiator);
    compaction->set_base_compaction_cnt(base_compaction_cnt);
    compaction->set_cumulative_compaction_cnt(cumu_compaction_cnt);
    compaction->set_type(type);
    long now = time(nullptr);
    compaction->set_expiration(now + 12);
    compaction->set_lease(now + 3);
    if (input_version.second > 0) {
        compaction->add_input_versions(input_version.first);
        compaction->add_input_versions(input_version.second);
        compaction->set_check_input_versions_range(true);
    }
    meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.ShortDebugString();
}

void finish_compaction_job(MetaService* meta_service, const std::string& cloud_unique_id,
                           int64_t tablet_id, const std::string& job_id,
                           const std::string& initiator, int base_compaction_cnt,
                           int cumu_compaction_cnt, TabletCompactionJobPB::CompactionType type,
                           const std::string& output_rowset_id,
                           std::pair<int64_t, int64_t> input_version, int64_t txn_id) {
    brpc::Controller cntl;
    FinishTabletJobRequest req;
    FinishTabletJobResponse res;
    req.set_cloud_unique_id(cloud_unique_id);
    req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
    auto compaction = req.mutable_job()->add_compaction();
    compaction->set_id(job_id);
    compaction->set_initiator(initiator);
    compaction->set_base_compaction_cnt(base_compaction_cnt);
    compaction->set_cumulative_compaction_cnt(cumu_compaction_cnt);
    compaction->set_type(type);
    long now = time(nullptr);
    compaction->set_expiration(now + 12);
    compaction->set_lease(now + 3);
    compaction->add_input_versions(input_version.first);
    compaction->add_input_versions(input_version.second);
    compaction->add_output_versions(input_version.second); // [first, second]
    compaction->set_check_input_versions_range(true);
    compaction->add_output_rowset_ids(output_rowset_id);
    compaction->add_txn_id(txn_id);
    req.set_action(FinishTabletJobRequest::COMMIT);
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.ShortDebugString();
}

void compact_rowsets_cumulative(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                                int64_t db_id, const std::string& label, int64_t table_id,
                                int64_t partition_id, int64_t tablet_id, int64_t start_version,
                                int64_t end_version, int num_rows) {
    TabletStatsPB stats;
    ASSERT_NO_FATAL_FAILURE(get_tablet_stats(meta_service, cloud_unique_id, tablet_id, stats));
    int base_compaction_cnt = stats.base_compaction_cnt();
    int cumu_compaction_cnt = stats.cumulative_compaction_cnt();
    std::string job_id = fmt::format("compaction_{}_{}_{}", tablet_id, start_version, end_version);
    ASSERT_NO_FATAL_FAILURE(start_compaction_job(
            meta_service, cloud_unique_id, tablet_id, job_id, "test_case", base_compaction_cnt,
            cumu_compaction_cnt, TabletCompactionJobPB::CUMULATIVE, {start_version, end_version}));
    int64_t txn_id = 123321;
    doris::RowsetMetaCloudPB compact_rowset =
            create_rowset(txn_id, tablet_id, partition_id, start_version, num_rows);
    std::string output_rowset_id = compact_rowset.rowset_id_v2();
    compact_rowset.set_end_version(end_version);
    ASSERT_NO_FATAL_FAILURE(prepare_rowset(meta_service, cloud_unique_id, compact_rowset));
    ASSERT_NO_FATAL_FAILURE(commit_rowset(meta_service, cloud_unique_id, compact_rowset));
    ASSERT_NO_FATAL_FAILURE(finish_compaction_job(
            meta_service, cloud_unique_id, tablet_id, job_id, "test_case", base_compaction_cnt,
            cumu_compaction_cnt, TabletCompactionJobPB::CUMULATIVE, output_rowset_id,
            {start_version, end_version}, txn_id));
}

void start_schema_change_job(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                             int64_t table_id, int64_t index_id, int64_t partition_id,
                             int64_t tablet_id, int64_t new_tablet_id, const std::string& job_id,
                             const std::string& initiator, int64_t alter_version = -1) {
    brpc::Controller cntl;
    StartTabletJobRequest req;
    StartTabletJobResponse res;
    req.set_cloud_unique_id(cloud_unique_id);
    req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
    auto sc = req.mutable_job()->mutable_schema_change();
    sc->set_id(job_id);
    sc->set_initiator(initiator);
    sc->mutable_new_tablet_idx()->set_tablet_id(new_tablet_id);
    if (alter_version != -1) {
        sc->set_alter_version(alter_version);
    }
    long now = time(nullptr);
    sc->set_expiration(now + 12);
    meta_service->start_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK)
            << job_id << ' ' << initiator << ' ' << res.status().msg();
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK)
            << job_id << ' ' << initiator;
};

void finish_schema_change_job(MetaService* meta_service, const std::string& cloud_unique_id,
                              int64_t tablet_id, int64_t new_tablet_id, const std::string& job_id,
                              const std::string& initiator,
                              const std::vector<doris::RowsetMetaCloudPB>& output_rowsets,
                              FinishTabletJobRequest_Action action = FinishTabletJobRequest::COMMIT,
                              int64_t delete_bitmap_lock_initiator = 12345) {
    brpc::Controller cntl;
    FinishTabletJobRequest req;
    FinishTabletJobResponse res;
    req.set_cloud_unique_id(cloud_unique_id);
    req.set_action(action);
    req.mutable_job()->mutable_idx()->set_tablet_id(tablet_id);
    auto sc = req.mutable_job()->mutable_schema_change();
    sc->mutable_new_tablet_idx()->set_tablet_id(new_tablet_id);
    if (output_rowsets.empty()) {
        sc->set_alter_version(0);
    } else {
        sc->set_alter_version(output_rowsets.back().end_version());
        for (auto& rowset : output_rowsets) {
            sc->add_txn_ids(rowset.txn_id());
            sc->add_output_versions(rowset.end_version());
            sc->set_num_output_rows(sc->num_output_rows() + rowset.num_rows());
            sc->set_num_output_segments(sc->num_output_segments() + rowset.num_segments());
            sc->set_size_output_rowsets(sc->size_output_rowsets() + rowset.total_disk_size());
            sc->set_index_size_output_rowsets(sc->index_size_output_rowsets() +
                                              rowset.index_disk_size());
            sc->set_segment_size_output_rowsets(sc->segment_size_output_rowsets() +
                                                rowset.data_disk_size());
        }
        sc->set_num_output_rowsets(output_rowsets.size());
    }
    sc->set_id(job_id);
    sc->set_initiator(initiator);
    sc->set_delete_bitmap_lock_initiator(delete_bitmap_lock_initiator);
    meta_service->finish_tablet_job(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.ShortDebugString();
}

struct SnapshotContext {
    std::string snapshot_id;
    std::string image_url;
    ObjectStoreInfoPB store_info;
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
    req.set_image_file_size(100);
    req.set_snapshot_data_size(1000);

    brpc::Controller cntl;
    CommitSnapshotResponse res;
    meta_service->commit_snapshot(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.ShortDebugString();
}

void list_snapshot(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                   std::vector<SnapshotContext>* snapshots) {
    ListSnapshotRequest req;
    req.set_cloud_unique_id(cloud_unique_id);
    // req.set_required_snapshot_id(snapshot_ids[1]); // Query specific aborted snapshot

    brpc::Controller cntl;
    ListSnapshotResponse res;
    meta_service->list_snapshot(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.ShortDebugString();
    for (auto snapshot : res.snapshots()) {
        SnapshotContext ctx;
        ctx.snapshot_id = snapshot.snapshot_id();
        ctx.image_url = snapshot.image_url();
        snapshots->emplace_back(ctx);
    }
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

void clone_instance(MetaServiceProxy* meta_service, const std::string& from_instance_id,
                    const std::string& snapshot_id, const std::string& clone_instance_id) {
    CloneInstanceRequest req;
    req.set_clone_type(CloneInstanceRequest::READ_ONLY);
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

void drop_instance(MetaServiceProxy* meta_service, const std::string& instance_id) {
    brpc::Controller cntl;
    AlterInstanceRequest req;
    AlterInstanceResponse res;
    req.set_instance_id(instance_id);
    req.set_op(AlterInstanceRequest::DROP);
    meta_service->alter_instance(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.ShortDebugString();
}

void clone_and_refresh_instance(MetaServiceProxy* meta_service, ResourceManager* resource_manager,
                                const std::string& from_instance_id,
                                const std::string& from_snapshot_id,
                                const std::string& to_instance_id, InstanceInfoPB& to_instance) {
    clone_instance(meta_service, from_instance_id, from_snapshot_id, to_instance_id);
    update_snapshot_properties(meta_service, to_instance_id, true, 0, 3660);
    std::string cloud_unique_id = fmt::format("1:{}:0", to_instance_id);
    get_instance(meta_service, cloud_unique_id, to_instance);
    resource_manager->refresh_instance(to_instance_id, to_instance);
}

void begin_and_commit_snapshot(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                               SnapshotContext& ctx) {
    begin_snapshot(meta_service, cloud_unique_id, "test_label", &ctx, false);
    commit_snapshot(meta_service, cloud_unique_id, ctx.snapshot_id, ctx.image_url, 1000);
}

std::unique_ptr<InstanceRecycler> get_instance_recycler(
        MetaServiceProxy* meta_service, const InstanceInfoPB& instance_info,
        std::shared_ptr<StorageVaultAccessor> accessor) {
    auto txn_lazy_committer = std::make_shared<TxnLazyCommitter>(meta_service->txn_kv());
    auto recycler = std::make_unique<InstanceRecycler>(meta_service->txn_kv(), instance_info,
                                                       thread_group, txn_lazy_committer);
    recycler->TEST_add_accessor(RESOURCE_ID, accessor);
    return recycler;
}

std::unique_ptr<InstanceRecycler> get_instance_recycler(MetaServiceProxy* meta_service,
                                                        const InstanceInfoPB& instance_info) {
    std::shared_ptr<StorageVaultAccessor> accessor = std::make_shared<MockAccessor>();
    return get_instance_recycler(meta_service, instance_info, accessor);
}

std::unique_ptr<InstanceChecker> get_instance_checker(
        MetaServiceProxy* meta_service, const std::string& instance_id,
        std::shared_ptr<StorageVaultAccessor> accessor) {
    auto checker = std::make_unique<InstanceChecker>(meta_service->txn_kv(), instance_id);
    checker->TEST_add_accessor(RESOURCE_ID, accessor);
    return checker;
}

std::unique_ptr<InstanceChecker> get_instance_checker(MetaServiceProxy* meta_service,
                                                      const std::string& instance_id) {
    std::shared_ptr<StorageVaultAccessor> accessor = std::make_shared<MockAccessor>();
    return get_instance_checker(meta_service, instance_id, accessor);
}

void issue_http_request(MetaServiceProxy* meta_service, brpc::HttpMethod method,
                        const std::string& path,
                        const std::unordered_map<std::string, std::string>& params,
                        rapidjson::Document* document) {
    std::string uri = fmt::format("/{}?", path);
    for (auto&& [k, v] : params) {
        uri += fmt::format("{}={}&", k, v);
    }

    brpc::Controller cntl;
    auto& http_request = cntl.http_request();
    http_request.set_method(method);
    http_request.set_content_type("application/json");
    http_request.uri() = uri;
    const_cast<std::string&>(http_request.unresolved_path()) = path;

    MetaServiceHttpRequest req;
    MetaServiceHttpResponse resp;
    meta_service->http(&cntl, &req, &resp, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    auto& http_response = cntl.http_response();
    ASSERT_EQ(http_response.status_code(), 200)
            << http_response.status_code() << " " << cntl.response_attachment();

    document->Parse(cntl.response_attachment().to_string().c_str());
    ASSERT_FALSE(document->HasParseError());
    ASSERT_TRUE(document->IsObject());
    ASSERT_TRUE(document->HasMember("code"));
    ASSERT_EQ(document->FindMember("code")->value.GetString(), std::string("OK"))
            << cntl.response_attachment();
}

struct SnapshotProperty {
    SnapshotSwitchStatus status;
    int64_t max_reserved_snapshots;
    int64_t snapshot_interval_seconds;
};

std::string dump_rapidjson(const rapidjson::Document& document) {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    document.Accept(writer);
    return buffer.GetString();
}

// Convert a string to a hex-escaped string.
// A non-displayed character is represented as \xHH where HH is the hexadecimal value of the character.
// A displayed character is represented as itself.
std::string escape_hex(std::string_view data) {
    std::string result;
    for (char c : data) {
        if (isprint(c)) {
            result += c;
        } else {
            result += fmt::format("\\x{:02x}", static_cast<unsigned char>(c));
        }
    }
    return result;
}

size_t count_range(TxnKv* txn_kv, std::string_view begin = "", std::string_view end = "\xFF") {
    std::unique_ptr<Transaction> txn;
    EXPECT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    if (!txn) {
        return 0; // Failed to create transaction
    }

    FullRangeGetOptions opts;
    opts.txn = txn.get();
    auto iter = txn_kv->full_range_get(std::string(begin), std::string(end), std::move(opts));
    size_t total = 0;
    for (auto&& kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
        total += 1;
    }

    EXPECT_TRUE(iter->is_valid()); // The iterator should still be valid after the next call.
    return total;
}

std::string dump_range(TxnKv* txn_kv, std::string_view begin = "", std::string_view end = "\xFF") {
    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        return "Failed to create dump range transaction";
    }
    FullRangeGetOptions opts;
    opts.txn = txn.get();
    auto iter = txn_kv->full_range_get(std::string(begin), std::string(end), std::move(opts));
    std::string buffer;
    for (auto&& kv = iter->next(); kv.has_value(); kv = iter->next()) {
        buffer += fmt::format("Key: {}, Value: {}, KeyHex: {}\n", escape_hex(kv->first),
                              escape_hex(kv->second), hex(kv->first));
    }
    EXPECT_TRUE(iter->is_valid()); // The iterator should still be valid after the next call.
    return buffer;
}

struct TableKeys {
    Versionstamp table_version;

    void equals(TableKeys& other) { ASSERT_EQ(table_version, other.table_version); }
};

void get_table_keys(TxnKv* txn_kv, std::string& instance_id, int64_t table_id,
                    TableKeys& table_keys) {
    MetaReader reader(instance_id, txn_kv);
    // table_version_key
    ASSERT_EQ(reader.get_table_version(table_id, &table_keys.table_version), TxnErrorCode::TXN_OK);
}

void get_table_keys(ResourceManager* resource_mgr, TxnKv* txn_kv, std::string& instance_id,
                    int64_t table_id, TableKeys& table_keys) {
    CloneChainReader reader(instance_id, txn_kv, resource_mgr);
    // table_version_key
    ASSERT_EQ(reader.get_table_version(table_id, &table_keys.table_version), TxnErrorCode::TXN_OK);
}

struct PartitionKeys {
    Versionstamp partition_version;
    VersionPB version;
    Versionstamp partition_meta_version;
    PartitionIndexPB partition_index;

    void equals(PartitionKeys& other) {
        ASSERT_EQ(partition_version, other.partition_version);
        ASSERT_EQ(version.SerializeAsString(), other.version.SerializeAsString());
        ASSERT_EQ(partition_meta_version, other.partition_meta_version);
        ASSERT_EQ(partition_index.SerializeAsString(), other.partition_index.SerializeAsString());
    }
};

void get_partition_keys(TxnKv* txn_kv, std::string& instance_id, int64_t partition_id,
                        PartitionKeys& partition_keys, bool not_found = false) {
    MetaReader reader(instance_id, txn_kv);
    // partition_version_key
    TxnErrorCode err = reader.get_partition_version(partition_id, &partition_keys.version,
                                                    &partition_keys.partition_version);
    ASSERT_TRUE(err == TxnErrorCode::TXN_OK || err == TxnErrorCode::TXN_KEY_NOT_FOUND) << err;
    TxnErrorCode code = not_found ? TxnErrorCode::TXN_KEY_NOT_FOUND : TxnErrorCode::TXN_OK;
    // meta_partition_key
    ASSERT_EQ(reader.is_partition_exists(partition_id), code);
    // partition_index_key
    ASSERT_EQ(reader.get_partition_index(partition_id, &partition_keys.partition_index), code);
    // partition_inverted_index_key
    auto key = versioned::partition_inverted_index_key(
            {instance_id, partition_keys.partition_index.db_id(),
             partition_keys.partition_index.table_id(), partition_id});
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string val;
    ASSERT_EQ(txn->get(key, &val), code);
    ASSERT_EQ(val, "");
}

void get_partition_keys(ResourceManager* resource_mgr, TxnKv* txn_kv, std::string& instance_id,
                        int64_t partition_id, PartitionKeys& partition_keys) {
    CloneChainReader reader(instance_id, txn_kv, resource_mgr);
    // partition_version_key
    TxnErrorCode err = reader.get_partition_version(partition_id, &partition_keys.version,
                                                    &partition_keys.partition_version);
    ASSERT_TRUE(err == TxnErrorCode::TXN_OK || err == TxnErrorCode::TXN_KEY_NOT_FOUND) << err;
    // meta_partition_key
    ASSERT_EQ(reader.is_partition_exists(partition_id), TxnErrorCode::TXN_OK);
    // partition_index_key
    ASSERT_EQ(reader.get_partition_index(partition_id, &partition_keys.partition_index),
              TxnErrorCode::TXN_OK);
    // partition_inverted_index_key
}

struct IndexKeys {
    Versionstamp index_meta_version;
    IndexIndexPB index_index;
    TabletSchemaCloudPB tablet_schema;

    void equals(IndexKeys& other) {
        ASSERT_EQ(index_meta_version, other.index_meta_version);
        ASSERT_EQ(index_index.SerializeAsString(), other.index_index.SerializeAsString());
        ASSERT_EQ(tablet_schema.SerializeAsString(), other.tablet_schema.SerializeAsString());
    }
};

void get_index_keys(TxnKv* txn_kv, std::string& instance_id, int64_t index_id,
                    IndexKeys& index_keys) {
    MetaReader reader(instance_id, txn_kv);
    // meta_index_key
    ASSERT_EQ(reader.is_index_exists(index_id), TxnErrorCode::TXN_OK);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string key = versioned::meta_index_key({instance_id, index_id});
    std::string val;
    ASSERT_EQ(versioned_get(txn.get(), key, &index_keys.index_meta_version, &val),
              TxnErrorCode::TXN_OK);
    ASSERT_EQ(val, "");
    // index_index_key
    ASSERT_EQ(reader.get_index_index(index_id, &index_keys.index_index), TxnErrorCode::TXN_OK);
    // index_inverted_key
    key = versioned::index_inverted_key({instance_id, index_keys.index_index.db_id(),
                                         index_keys.index_index.table_id(), index_id});
    ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
    ASSERT_EQ(val, "");
    // meta_schema_key
    int64_t schema_version = 0;
    ASSERT_EQ(reader.get_tablet_schema(index_id, schema_version, &index_keys.tablet_schema),
              TxnErrorCode::TXN_OK)
            << "instance_id=" << instance_id << ", index_id=" << index_id
            << ", schema_version=" << schema_version;
}

void get_index_keys(ResourceManager* resource_mgr, TxnKv* txn_kv, std::string& instance_id,
                    int64_t index_id, IndexKeys& index_keys) {
    CloneChainReader reader(instance_id, txn_kv, resource_mgr);
    // meta_index_key
    ASSERT_EQ(reader.is_index_exists(index_id), TxnErrorCode::TXN_OK);
    // index_index_key
    ASSERT_EQ(reader.get_index_index(index_id, &index_keys.index_index), TxnErrorCode::TXN_OK);
    // index_inverted_key
    // meta_schema_key
    int64_t schema_version = 0;
    ASSERT_EQ(reader.get_tablet_schema(index_id, schema_version, &index_keys.tablet_schema),
              TxnErrorCode::TXN_OK);
}

struct RowsetKeys {
    std::vector<RowsetMetaCloudPB> rowset_metas;
    std::vector<std::pair<std::string, DeleteBitmapStoragePB>> delete_bitmaps;

    void equals(RowsetKeys& other) {
        ASSERT_EQ(rowset_metas.size(), other.rowset_metas.size());
        for (size_t i = 0; i < rowset_metas.size(); ++i) {
            auto& rowset_meta = rowset_metas[i];
            auto& other_rowset_meta = other.rowset_metas[i];
            ASSERT_EQ(rowset_meta.start_version(), other_rowset_meta.start_version());
            ASSERT_EQ(rowset_meta.end_version(), other_rowset_meta.end_version());
            ASSERT_EQ(rowset_meta.rowset_id_v2(), other_rowset_meta.rowset_id_v2());
            // ASSERT_NE(rowset_meta.SerializeAsString(), other_rowset_meta.SerializeAsString());
        }

        ASSERT_EQ(delete_bitmaps.size(), other.delete_bitmaps.size());
        for (size_t i = 0; i < delete_bitmaps.size(); ++i) {
            auto& [rowset_id, delete_bitmap] = delete_bitmaps[i];
            auto& [other_rowset_id, other_delete_bitmap] = other.delete_bitmaps[i];
            ASSERT_EQ(rowset_id, other_rowset_id);
            ASSERT_EQ(delete_bitmap.SerializeAsString(), other_delete_bitmap.SerializeAsString());
        }
    }
};

void get_rowset_keys(TxnKv* txn_kv, std::string& instance_id, int64_t tablet_id,
                     RowsetKeys& rowset_keys) {
    MetaReader reader(instance_id, txn_kv);
    ASSERT_EQ(reader.get_rowset_metas(tablet_id, 0, INT64_MAX, &rowset_keys.rowset_metas),
              TxnErrorCode::TXN_OK);
    // meta_delete_bitmap_key
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    for (auto& rowset_meta : rowset_keys.rowset_metas) {
        auto& rowset_id = rowset_meta.rowset_id_v2();
        auto key = versioned::meta_delete_bitmap_key({instance_id, tablet_id, rowset_id});
        ValueBuf val_buf;
        auto err = cloud::blob_get(txn.get(), key, &val_buf);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            continue;
        }
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        DeleteBitmapStoragePB delete_bitmap;
        ASSERT_TRUE(val_buf.to_pb(&delete_bitmap));
        rowset_keys.delete_bitmaps.push_back(std::make_pair(rowset_id, delete_bitmap));
    }
}

void get_rowset_keys(ResourceManager* resource_mgr, TxnKv* txn_kv, std::string& instance_id,
                     int64_t tablet_id, RowsetKeys& rowset_keys) {
    CloneChainReader reader(instance_id, txn_kv, resource_mgr);
    ASSERT_EQ(reader.get_rowset_metas(tablet_id, 0, INT64_MAX, &rowset_keys.rowset_metas),
              TxnErrorCode::TXN_OK);
    // meta_delete_bitmap_key
    for (auto& rowset_meta : rowset_keys.rowset_metas) {
        auto& rowset_id = rowset_meta.rowset_id_v2();
        DeleteBitmapStoragePB delete_bitmap;
        auto err = reader.get_delete_bitmap_v2(tablet_id, rowset_id, &delete_bitmap);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            continue;
        }
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        rowset_keys.delete_bitmaps.push_back(std::make_pair(rowset_id, delete_bitmap));
    }
}

struct TabletKeys {
    int64_t tablet_id;
    TabletKeys(int64_t tablet_id) { this->tablet_id = tablet_id; }
    Versionstamp tablet_meta_version;
    TabletMetaCloudPB tablet_meta;
    doris::cloud::TabletIndexPB tablet_index;
    Versionstamp load_stats_version;
    TabletStatsPB load_stats;
    Versionstamp compact_stats_version;
    TabletStatsPB compact_stats;
    RowsetKeys rowset_keys;

    void equals(TabletKeys& other) {
        ASSERT_EQ(tablet_meta_version, other.tablet_meta_version);
        ASSERT_EQ(tablet_meta.SerializeAsString(), other.tablet_meta.SerializeAsString());
        ASSERT_EQ(tablet_index.SerializeAsString(), other.tablet_index.SerializeAsString());
        ASSERT_EQ(load_stats_version, other.load_stats_version);
        ASSERT_EQ(load_stats.SerializeAsString(), other.load_stats.SerializeAsString());
        ASSERT_EQ(compact_stats_version, other.compact_stats_version);
        ASSERT_EQ(compact_stats.SerializeAsString(), other.compact_stats.SerializeAsString());
        rowset_keys.equals(other.rowset_keys);
    }
};

void get_tablet_keys(TxnKv* txn_kv, std::string& instance_id, TabletKeys& tablet_keys) {
    int64_t tablet_id = tablet_keys.tablet_id;
    MetaReader reader(instance_id, txn_kv);
    // meta_tablet_key
    ASSERT_EQ(reader.get_tablet_meta(tablet_id, &tablet_keys.tablet_meta,
                                     &tablet_keys.tablet_meta_version),
              TxnErrorCode::TXN_OK);
    // tablet_index_key
    ASSERT_EQ(reader.get_tablet_index(tablet_id, &tablet_keys.tablet_index), TxnErrorCode::TXN_OK);
    // tablet_inverted_index_key
    std::string key = versioned::tablet_inverted_index_key(
            {instance_id, tablet_keys.tablet_index.db_id(), tablet_keys.tablet_index.table_id(),
             tablet_keys.tablet_index.index_id(), tablet_keys.tablet_index.partition_id(),
             tablet_id});
    std::string val;
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
    ASSERT_EQ(val, "");
    // tablet_load_stats_key
    ASSERT_EQ(reader.get_tablet_load_stats(tablet_id, &tablet_keys.load_stats,
                                           &tablet_keys.load_stats_version),
              TxnErrorCode::TXN_OK);
    // tablet_compact_stats_key
    ASSERT_EQ(reader.get_tablet_compact_stats(tablet_id, &tablet_keys.compact_stats,
                                              &tablet_keys.compact_stats_version),
              TxnErrorCode::TXN_OK);
    get_rowset_keys(txn_kv, instance_id, tablet_id, tablet_keys.rowset_keys);
}

void get_tablet_keys(ResourceManager* resource_mgr, TxnKv* txn_kv, std::string& instance_id,
                     TabletKeys& tablet_keys) {
    int64_t tablet_id = tablet_keys.tablet_id;
    CloneChainReader reader(instance_id, txn_kv, resource_mgr);
    // meta_tablet_key
    ASSERT_EQ(reader.get_tablet_meta(tablet_id, &tablet_keys.tablet_meta,
                                     &tablet_keys.tablet_meta_version),
              TxnErrorCode::TXN_OK);
    // tablet_index_key
    ASSERT_EQ(reader.get_tablet_index(tablet_id, &tablet_keys.tablet_index), TxnErrorCode::TXN_OK);
    // tablet_inverted_index_key
    // tablet_load_stats_key
    ASSERT_EQ(reader.get_tablet_load_stats(tablet_id, &tablet_keys.load_stats,
                                           &tablet_keys.load_stats_version),
              TxnErrorCode::TXN_OK);
    // tablet_compact_stats_key
    ASSERT_EQ(reader.get_tablet_compact_stats(tablet_id, &tablet_keys.compact_stats,
                                              &tablet_keys.compact_stats_version),
              TxnErrorCode::TXN_OK);
    get_rowset_keys(resource_mgr, txn_kv, instance_id, tablet_id, tablet_keys.rowset_keys);
}

TEST(SnapshotChainCompactorTest, IsSnapshotChainNeedCompact) {
    // instance1 -> instance2 -> instance3 -> instance5
    // instance1 -> instance4
    auto meta_service = get_meta_service();
    auto resource_mgr = meta_service->resource_mgr();
    auto txn_kv = meta_service->txn_kv();

    // create instance1 and snapshot
    std::string instance_id1 = "snapshot_chain_compactor_test_instance1";
    SnapshotContext ctx1;
    InstanceInfoPB instance_info1;
    {
        std::string cloud_unique_id1 = fmt::format("1:{}:0", instance_id1);
        create_and_refresh_instance(meta_service.get(), instance_id1);
        get_instance(meta_service.get(), cloud_unique_id1, instance_info1);
        begin_and_commit_snapshot(meta_service.get(), cloud_unique_id1, ctx1);
    }

    // clone instance2 from instance1 and snapshot
    std::string instance_id2 = "snapshot_chain_compactor_test_instance2";
    std::string cloud_unique_id2 = fmt::format("1:{}:0", instance_id2);
    SnapshotContext ctx2;
    InstanceInfoPB instance_info2;
    {
        clone_and_refresh_instance(meta_service.get(), resource_mgr.get(), instance_id1,
                                   ctx1.snapshot_id, instance_id2, instance_info2);
        begin_and_commit_snapshot(meta_service.get(), cloud_unique_id2, ctx2);
    }

    // clone instance3 from instance2 and snapshot
    std::string instance_id3 = "snapshot_chain_compactor_test_instance3";
    SnapshotContext ctx3;
    InstanceInfoPB instance_info3;
    {
        std::string cloud_unique_id3 = fmt::format("1:{}:0", instance_id3);
        clone_and_refresh_instance(meta_service.get(), resource_mgr.get(), instance_id2,
                                   ctx2.snapshot_id, instance_id3, instance_info3);
        begin_and_commit_snapshot(meta_service.get(), cloud_unique_id3, ctx3);
    }

    // clone instance4 from instance1
    std::string instance_id4 = "snapshot_chain_compactor_test_instance4";
    InstanceInfoPB instance_info4;
    clone_and_refresh_instance(meta_service.get(), resource_mgr.get(), instance_id1,
                               ctx1.snapshot_id, instance_id4, instance_info4);

    // clone instance4 from instance3
    std::string instance_id5 = "snapshot_chain_compactor_test_instance5";
    InstanceInfoPB instance_info5;
    clone_and_refresh_instance(meta_service.get(), resource_mgr.get(), instance_id3,
                               ctx3.snapshot_id, instance_id5, instance_info5);

    // check is_snapshot_chain_need_compact
    SnapshotChainCompactor compactor(txn_kv);
    ASSERT_FALSE(compactor.is_snapshot_chain_need_compact(instance_info1));
    ASSERT_TRUE(compactor.is_snapshot_chain_need_compact(instance_info2));
    ASSERT_FALSE(compactor.is_snapshot_chain_need_compact(instance_info3));
    ASSERT_FALSE(compactor.is_snapshot_chain_need_compact(instance_info4));
    ASSERT_FALSE(compactor.is_snapshot_chain_need_compact(instance_info5));

    // compact instance2 and complete
    InstanceChainCompactor instance_chain_compactor(txn_kv, instance_info2);
    instance_chain_compactor.handle_compaction_completion();
    get_instance(meta_service.get(), cloud_unique_id2, instance_info2);

    // check is_snapshot_chain_need_compact
    ASSERT_FALSE(compactor.is_snapshot_chain_need_compact(instance_info1));
    ASSERT_FALSE(compactor.is_snapshot_chain_need_compact(instance_info2));
    ASSERT_TRUE(compactor.is_snapshot_chain_need_compact(instance_info3));
    ASSERT_FALSE(compactor.is_snapshot_chain_need_compact(instance_info4));
    ASSERT_FALSE(compactor.is_snapshot_chain_need_compact(instance_info5));
}

TEST(SnapshotChainCompactorTest, Basic) {
    auto meta_service = get_meta_service();
    auto resource_mgr = meta_service->resource_mgr();
    auto txn_kv = meta_service->txn_kv();
    std::string instance_id = "snapshot_chain_compactor_test_instance";
    std::string cloud_unique_id = fmt::format("1:{}:0", instance_id);
    create_and_refresh_instance(meta_service.get(), instance_id);
    int64_t db_id = 1, table_id = 2, index_id = 3, partition_id = 4, tablet_id = 5;
    // create partition/index/tablet
    prepare_and_commit_index(meta_service.get(), cloud_unique_id, db_id, table_id, index_id);
    prepare_and_commit_partition(meta_service.get(), cloud_unique_id, db_id, table_id, partition_id,
                                 index_id);
    create_tablet(meta_service.get(), cloud_unique_id, db_id, table_id, index_id, partition_id,
                  tablet_id, true);
    insert_rowset(meta_service.get(), cloud_unique_id, db_id, "label_1", table_id, partition_id,
                  tablet_id);
    // create partition2
    int64_t partition_id2 = 6;
    prepare_and_commit_partition(meta_service.get(), cloud_unique_id, db_id, table_id,
                                 partition_id2, index_id);
    // create partition3/tablet3
    int64_t partition_id3 = 7;
    int64_t tablet_id3 = 8;
    prepare_and_commit_partition(meta_service.get(), cloud_unique_id, db_id, table_id,
                                 partition_id3, index_id);
    create_tablet(meta_service.get(), cloud_unique_id, db_id, table_id, index_id, partition_id3,
                  tablet_id3, true);
    // insert some rowsets
    for (int i = 0; i < 10; i++) {
        std::string rowset_id;
        insert_rowset(meta_service.get(), cloud_unique_id, db_id, fmt::format("label3_{}", i),
                      table_id, partition_id3, tablet_id3, &rowset_id);
        update_delete_bitmap(meta_service.get(), cloud_unique_id, table_id, partition_id3,
                             tablet_id3, rowset_id);
    }

    TableKeys source_table_keys;
    get_table_keys(txn_kv.get(), instance_id, table_id, source_table_keys);
    PartitionKeys source_partition_keys;
    get_partition_keys(txn_kv.get(), instance_id, partition_id, source_partition_keys);
    PartitionKeys source_partition2_keys;
    get_partition_keys(txn_kv.get(), instance_id, partition_id2, source_partition2_keys);
    IndexKeys source_index_keys;
    get_index_keys(txn_kv.get(), instance_id, index_id, source_index_keys);
    TabletKeys source_tablet_keys(tablet_id);
    get_tablet_keys(txn_kv.get(), instance_id, source_tablet_keys);
    TabletKeys source_tablet3_keys(tablet_id3);
    get_tablet_keys(txn_kv.get(), instance_id, source_tablet3_keys);

    // snapshot
    std::vector<SnapshotContext> snapshots;
    list_snapshot(meta_service.get(), cloud_unique_id, &snapshots);
    ASSERT_EQ(snapshots.size(), 0);
    SnapshotContext ctx;
    begin_snapshot(meta_service.get(), cloud_unique_id, "snapshot_1", &ctx, false);
    commit_snapshot(meta_service.get(), cloud_unique_id, ctx.snapshot_id, ctx.image_url, 1000);
    list_snapshot(meta_service.get(), cloud_unique_id, &snapshots);
    ASSERT_EQ(snapshots.size(), 1);

    // create partition4/tablet4
    int64_t partition_id4 = 9;
    int64_t tablet_id4 = 10;
    prepare_and_commit_partition(meta_service.get(), cloud_unique_id, db_id, table_id,
                                 partition_id4, index_id);
    create_tablet(meta_service.get(), cloud_unique_id, db_id, table_id, index_id, partition_id4,
                  tablet_id4, true);

    // clone instance from snapshot
    std::string clone_instance_id = "clone_snapshot_chain_compactor_test_instance";
    std::string clone_cloud_unique_id = fmt::format("1:{}:0", clone_instance_id);
    clone_instance(meta_service.get(), instance_id, ctx.snapshot_id, clone_instance_id);
    InstanceInfoPB clone_instance_info;
    get_instance(meta_service.get(), clone_cloud_unique_id, clone_instance_info);
    resource_mgr->refresh_instance(clone_instance_id, clone_instance_info);

    // get keys by clone chain reader
    {
        TableKeys clone_table_keys;
        get_table_keys(resource_mgr.get(), txn_kv.get(), clone_instance_id, table_id,
                       clone_table_keys);
        clone_table_keys.equals(source_table_keys);

        PartitionKeys clone_partition_keys;
        get_partition_keys(resource_mgr.get(), txn_kv.get(), clone_instance_id, partition_id,
                           clone_partition_keys);
        clone_partition_keys.equals(source_partition_keys);

        PartitionKeys clone_partition2_keys;
        get_partition_keys(resource_mgr.get(), txn_kv.get(), clone_instance_id, partition_id2,
                           clone_partition2_keys);
        clone_partition2_keys.equals(source_partition2_keys);

        IndexKeys clone_index_keys;
        get_index_keys(resource_mgr.get(), txn_kv.get(), clone_instance_id, index_id,
                       clone_index_keys);
        clone_index_keys.index_meta_version = source_index_keys.index_meta_version;
        clone_index_keys.equals(source_index_keys);

        TabletKeys clone_tablet_keys(tablet_id);
        get_tablet_keys(resource_mgr.get(), txn_kv.get(), clone_instance_id, clone_tablet_keys);
        clone_tablet_keys.equals(source_tablet_keys);

        TabletKeys clone_tablet3_keys(tablet_id3);
        get_tablet_keys(resource_mgr.get(), txn_kv.get(), clone_instance_id, clone_tablet3_keys);
        clone_tablet3_keys.equals(source_tablet3_keys);
    }

    // compact snapshot keys
    InstanceChainCompactor compactor(txn_kv, clone_instance_info);
    ASSERT_EQ(compactor.do_compact(), 0);

    {
        TableKeys table_keys;
        get_table_keys(txn_kv.get(), clone_instance_id, table_id, table_keys);
        table_keys.equals(source_table_keys);

        PartitionKeys partition_keys;
        get_partition_keys(txn_kv.get(), clone_instance_id, partition_id, partition_keys);
        partition_keys.equals(source_partition_keys);

        PartitionKeys partition2_keys;
        get_partition_keys(txn_kv.get(), clone_instance_id, partition_id2, partition2_keys);
        partition2_keys.equals(source_partition2_keys);

        IndexKeys index_keys;
        get_index_keys(txn_kv.get(), clone_instance_id, index_id, index_keys);
        index_keys.equals(source_index_keys);

        TabletKeys tablet_keys(tablet_id);
        get_tablet_keys(txn_kv.get(), clone_instance_id, tablet_keys);
        tablet_keys.equals(source_tablet_keys);

        TabletKeys tablet3_keys(tablet_id3);
        get_tablet_keys(txn_kv.get(), clone_instance_id, tablet3_keys);
        tablet3_keys.equals(source_tablet3_keys);

        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), clone_cloud_unique_id, tablet_id3, 0, INT64_MAX, rowsets);
        ASSERT_EQ(rowsets.size(), 11);

        PartitionKeys partition4_keys;
        get_partition_keys(txn_kv.get(), clone_instance_id, partition_id4, partition4_keys, true);
    }
}

TEST(SnapshotChainCompactorTest, Insert) {
    auto meta_service = get_meta_service();
    auto resource_mgr = meta_service->resource_mgr();
    auto txn_kv = meta_service->txn_kv();
    std::string instance_id = "snapshot_chain_compactor_test_instance";
    std::string cloud_unique_id = fmt::format("1:{}:0", instance_id);
    create_and_refresh_instance(meta_service.get(), instance_id);
    int64_t db_id = 1, table_id = 2, index_id = 3, partition_id = 4, tablet_id = 5;

    // create partition/index/tablet
    prepare_and_commit_index(meta_service.get(), cloud_unique_id, db_id, table_id, index_id);
    prepare_and_commit_partition(meta_service.get(), cloud_unique_id, db_id, table_id, partition_id,
                                 index_id);
    create_tablet(meta_service.get(), cloud_unique_id, db_id, table_id, index_id, partition_id,
                  tablet_id);

    // Phase 1: insert 5 rowsets (version 2-6)
    for (int i = 0; i < 5; i++) {
        insert_rowset(meta_service.get(), cloud_unique_id, db_id, fmt::format("label_{}", i),
                      table_id, partition_id, tablet_id);
    }

    // Phase 1: seg compact version 2-2
    {
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id, tablet_id, 0, 6, rowsets);
        ASSERT_EQ(rowsets.size(), 6);

        compact_rowsets_cumulative(meta_service.get(), cloud_unique_id, db_id, "compaction_label_1",
                                   table_id, partition_id, tablet_id, 2, 2, 300);

        // Verify: should have 6 rowsets: [0-1], [2-2], [3-3], [4-4], [5-5], [6-6]
        rowsets.clear();
        get_rowsets(meta_service.get(), cloud_unique_id, tablet_id, 0, 6, rowsets);
        ASSERT_EQ(rowsets.size(), 6);
    }

    // Phase 1: compact version 2-4
    {
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id, tablet_id, 0, 6, rowsets);
        ASSERT_EQ(rowsets.size(), 6);

        compact_rowsets_cumulative(meta_service.get(), cloud_unique_id, db_id, "compaction_label_1",
                                   table_id, partition_id, tablet_id, 2, 4, 300);

        // Verify: should have 4 rowsets: [0-1], [2-4], [5-5], [6-6]
        rowsets.clear();
        get_rowsets(meta_service.get(), cloud_unique_id, tablet_id, 0, 6, rowsets);
        ASSERT_EQ(rowsets.size(), 4);
    }

    // Phase 2: insert 3 more rowsets (version 7-9)
    for (int i = 5; i < 8; i++) {
        insert_rowset(meta_service.get(), cloud_unique_id, db_id, fmt::format("label_{}", i),
                      table_id, partition_id, tablet_id);
    }

    // Phase 2: compact version 5-7
    {
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id, tablet_id, 0, 9, rowsets);
        // [0-1], [2-4], [5-5], [6-6], [7-7], [8-8], [9-9] - 3 + 3 = 7, but [5-7] will be compacted
        ASSERT_EQ(rowsets.size(), 7);

        compact_rowsets_cumulative(meta_service.get(), cloud_unique_id, db_id, "compaction_label_2",
                                   table_id, partition_id, tablet_id, 5, 7, 300);

        // Verify rowsets
        rowsets.clear();
        get_rowsets(meta_service.get(), cloud_unique_id, tablet_id, 0, 9, rowsets);
        ASSERT_EQ(rowsets.size(), 5); // [0-1], [2-4], [5-7], [8-8], [9-9]
    }

    TableKeys source_table_keys;
    get_table_keys(txn_kv.get(), instance_id, table_id, source_table_keys);
    PartitionKeys source_partition_keys;
    get_partition_keys(txn_kv.get(), instance_id, partition_id, source_partition_keys);
    IndexKeys source_index_keys;
    get_index_keys(txn_kv.get(), instance_id, index_id, source_index_keys);
    TabletKeys source_tablet_keys(tablet_id);
    get_tablet_keys(txn_kv.get(), instance_id, source_tablet_keys);

    // Phase 3: snapshot
    std::vector<SnapshotContext> snapshots;
    list_snapshot(meta_service.get(), cloud_unique_id, &snapshots);
    ASSERT_EQ(snapshots.size(), 0);
    SnapshotContext ctx;
    begin_snapshot(meta_service.get(), cloud_unique_id, "snapshot_1", &ctx, false);
    commit_snapshot(meta_service.get(), cloud_unique_id, ctx.snapshot_id, ctx.image_url, 1000);
    list_snapshot(meta_service.get(), cloud_unique_id, &snapshots);
    ASSERT_EQ(snapshots.size(), 1);

    // Phase 4: insert 2 more rowsets (version 10-11)
    for (int i = 8; i < 10; i++) {
        insert_rowset(meta_service.get(), cloud_unique_id, db_id, fmt::format("label_{}", i),
                      table_id, partition_id, tablet_id);
    }

    // Phase 4: compact version 8-10
    {
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id, tablet_id, 0, 11, rowsets);
        ASSERT_EQ(rowsets.size(), 7); // [0-1], [2-4], [5-7], [8-8], [9-9], [10-10], [11-11]

        compact_rowsets_cumulative(meta_service.get(), cloud_unique_id, db_id, "compaction_label_3",
                                   table_id, partition_id, tablet_id, 8, 10, 300);

        // Verify rowsets
        rowsets.clear();
        get_rowsets(meta_service.get(), cloud_unique_id, tablet_id, 0, 11, rowsets);
        ASSERT_EQ(rowsets.size(), 5); // [0-1], [2-4], [5-7], [8-10], [11-11]
    }

    // clone instance from snapshot
    std::string clone_instance_id = "clone_snapshot_chain_compactor_test_instance";
    std::string clone_cloud_unique_id = fmt::format("1:{}:0", clone_instance_id);
    clone_instance(meta_service.get(), instance_id, ctx.snapshot_id, clone_instance_id);
    InstanceInfoPB clone_instance_info;
    get_instance(meta_service.get(), clone_cloud_unique_id, clone_instance_info);
    resource_mgr->refresh_instance(clone_instance_id, clone_instance_info);

    // get keys by clone chain reader
    {
        TableKeys clone_table_keys;
        get_table_keys(resource_mgr.get(), txn_kv.get(), clone_instance_id, table_id,
                       clone_table_keys);
        clone_table_keys.equals(source_table_keys);

        PartitionKeys clone_partition_keys;
        get_partition_keys(resource_mgr.get(), txn_kv.get(), clone_instance_id, partition_id,
                           clone_partition_keys);
        clone_partition_keys.equals(source_partition_keys);

        IndexKeys clone_index_keys;
        get_index_keys(resource_mgr.get(), txn_kv.get(), clone_instance_id, index_id,
                       clone_index_keys);
        clone_index_keys.index_meta_version = source_index_keys.index_meta_version;
        clone_index_keys.equals(source_index_keys);

        TabletKeys clone_tablet_keys(tablet_id);
        get_tablet_keys(resource_mgr.get(), txn_kv.get(), clone_instance_id, clone_tablet_keys);
        clone_tablet_keys.equals(source_tablet_keys);
    }

    // compact snapshot keys
    InstanceChainCompactor compactor(txn_kv, clone_instance_info);
    ASSERT_EQ(compactor.do_compact(), 0);

    {
        TableKeys table_keys;
        get_table_keys(txn_kv.get(), clone_instance_id, table_id, table_keys);
        table_keys.equals(source_table_keys);

        PartitionKeys partition_keys;
        get_partition_keys(txn_kv.get(), clone_instance_id, partition_id, partition_keys);
        partition_keys.equals(source_partition_keys);

        IndexKeys index_keys;
        get_index_keys(txn_kv.get(), clone_instance_id, index_id, index_keys);
        index_keys.equals(source_index_keys);

        TabletKeys tablet_keys(tablet_id);
        get_tablet_keys(txn_kv.get(), clone_instance_id, tablet_keys);
        tablet_keys.equals(source_tablet_keys);

        {
            std::vector<doris::RowsetMetaCloudPB> rowsets;
            get_rowsets(meta_service.get(), clone_cloud_unique_id, tablet_id, 0, INT64_MAX,
                        rowsets);
            ASSERT_EQ(rowsets.size(), 5); // [0-1], [2-4], [5-7], [8-8], [9-9]
            std::vector<std::pair<int64_t, int64_t>> expected_versions = {
                    {0, 1}, {2, 4}, {5, 7}, {8, 8}, {9, 9}};
            for (auto i = 0; i < rowsets.size(); ++i) {
                ASSERT_EQ(rowsets[i].start_version(), expected_versions[i].first) << i;
                ASSERT_EQ(rowsets[i].end_version(), expected_versions[i].second) << i;
            }
        }
    }
}

TEST(SnapshotChainCompactorTest, SchemaChange) {
    auto meta_service = get_meta_service();
    auto resource_mgr = meta_service->resource_mgr();
    auto txn_kv = meta_service->txn_kv();
    std::string instance_id = "snapshot_chain_compactor_test_instance";
    std::string cloud_unique_id = fmt::format("1:{}:0", instance_id);
    create_and_refresh_instance(meta_service.get(), instance_id);
    int64_t db_id = 1, table_id = 2, index_id = 3, partition_id = 4;
    int64_t old_tablet_id = 5, new_tablet_id_1 = 6, new_tablet_id_2 = 7;

    // create partition/index/tablet
    prepare_and_commit_index(meta_service.get(), cloud_unique_id, db_id, table_id, index_id);
    prepare_and_commit_partition(meta_service.get(), cloud_unique_id, db_id, table_id, partition_id,
                                 index_id);
    create_tablet(meta_service.get(), cloud_unique_id, db_id, table_id, index_id, partition_id,
                  old_tablet_id);

    // Phase 1: Single version mode - insert 5 rowsets to old_tablet (version 2-6)
    for (int i = 0; i < 5; i++) {
        insert_rowset(meta_service.get(), cloud_unique_id, db_id, fmt::format("label_{}", i),
                      table_id, partition_id, old_tablet_id);
    }

    // Phase 1: Single version mode - schema change from old_tablet to new_tablet_1
    {
        std::vector<doris::RowsetMetaCloudPB> output_rowsets;
        std::string job_id = fmt::format("schema_change_{}_{}", old_tablet_id, new_tablet_id_1);
        int64_t alter_version = 6;

        // Create new tablet
        create_tablet(meta_service.get(), cloud_unique_id, db_id, table_id, index_id, partition_id,
                      new_tablet_id_1, false, TabletStatePB::PB_NOTREADY);

        // Start schema change job
        start_schema_change_job(meta_service.get(), cloud_unique_id, table_id, index_id,
                                partition_id, old_tablet_id, new_tablet_id_1, job_id, "test_case",
                                alter_version);

        // Create output rowsets for new_tablet_1 (version 0-1, 2-6)
        // First create the initial rowset [0-1]
        {
            int64_t txn_id = 100000;
            auto output_rowset = create_rowset(txn_id, new_tablet_id_1, partition_id, 0, 100);
            output_rowset.set_end_version(1);
            output_rowsets.push_back(output_rowset);
            prepare_rowset(meta_service.get(), cloud_unique_id, output_rowset);
            commit_rowset(meta_service.get(), cloud_unique_id, output_rowset);
        }
        // Then create rowsets for version 2-6
        for (int version = 2; version <= 6; version++) {
            int64_t txn_id = 100000 + version;
            auto output_rowset = create_rowset(txn_id, new_tablet_id_1, partition_id, version, 100);
            output_rowsets.push_back(output_rowset);
            prepare_rowset(meta_service.get(), cloud_unique_id, output_rowset);
            commit_rowset(meta_service.get(), cloud_unique_id, output_rowset);
        }

        // Finish schema change
        finish_schema_change_job(meta_service.get(), cloud_unique_id, old_tablet_id,
                                 new_tablet_id_1, job_id, "test_case", output_rowsets);

        // Verify new_tablet_1 has all rowsets
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id, new_tablet_id_1, 0, 6, rowsets);
        ASSERT_EQ(rowsets.size(), 6); // [0-1], [2-2], [3-3], [4-4], [5-5], [6-6]
    }

    // Phase 2: insert 3 more rowsets to new_tablet_1 (version 7-9)
    for (int i = 5; i < 8; i++) {
        insert_rowset(meta_service.get(), cloud_unique_id, db_id, fmt::format("label_{}", i),
                      table_id, partition_id, new_tablet_id_1);
    }

    // Phase 2: schema change from new_tablet_1 to new_tablet_2
    {
        std::vector<doris::RowsetMetaCloudPB> output_rowsets;
        std::string job_id = fmt::format("schema_change_{}_{}", new_tablet_id_1, new_tablet_id_2);
        int64_t alter_version = 9;

        // Create new tablet
        create_tablet(meta_service.get(), cloud_unique_id, db_id, table_id, index_id, partition_id,
                      new_tablet_id_2, false, TabletStatePB::PB_NOTREADY);

        // Start schema change job
        start_schema_change_job(meta_service.get(), cloud_unique_id, table_id, index_id,
                                partition_id, new_tablet_id_1, new_tablet_id_2, job_id, "test_case",
                                alter_version);

        // Create output rowsets for new_tablet_2 (version 0-1, 2-9)
        // First create the initial rowset [0-1]
        {
            int64_t txn_id = 200000;
            auto output_rowset = create_rowset(txn_id, new_tablet_id_2, partition_id, 0, 100);
            output_rowset.set_end_version(1);
            output_rowsets.push_back(output_rowset);
            prepare_rowset(meta_service.get(), cloud_unique_id, output_rowset);
            commit_rowset(meta_service.get(), cloud_unique_id, output_rowset);
        }
        // Then create rowsets for version 2-9
        for (int version = 2; version <= 9; version++) {
            int64_t txn_id = 200000 + version;
            auto output_rowset = create_rowset(txn_id, new_tablet_id_2, partition_id, version, 100);
            output_rowsets.push_back(output_rowset);
            prepare_rowset(meta_service.get(), cloud_unique_id, output_rowset);
            commit_rowset(meta_service.get(), cloud_unique_id, output_rowset);
        }

        // Finish schema change
        finish_schema_change_job(meta_service.get(), cloud_unique_id, new_tablet_id_1,
                                 new_tablet_id_2, job_id, "test_case", output_rowsets);

        // Verify new_tablet_2 has all rowsets
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id, new_tablet_id_2, 0, 9, rowsets);
        ASSERT_EQ(rowsets.size(), 9); // [0-1], [2-2], ..., [9-9]
    }

    TableKeys source_table_keys;
    get_table_keys(txn_kv.get(), instance_id, table_id, source_table_keys);
    PartitionKeys source_partition_keys;
    get_partition_keys(txn_kv.get(), instance_id, partition_id, source_partition_keys);
    IndexKeys source_index_keys;
    get_index_keys(txn_kv.get(), instance_id, index_id, source_index_keys);
    TabletKeys source_tablet_keys(old_tablet_id);
    get_tablet_keys(txn_kv.get(), instance_id, source_tablet_keys);
    TabletKeys source_new_tablet_keys(new_tablet_id_1);
    get_tablet_keys(txn_kv.get(), instance_id, source_new_tablet_keys);
    TabletKeys source_new_tablet2_keys(new_tablet_id_2);
    get_tablet_keys(txn_kv.get(), instance_id, source_new_tablet2_keys);

    // snapshot
    std::vector<SnapshotContext> snapshots;
    list_snapshot(meta_service.get(), cloud_unique_id, &snapshots);
    ASSERT_EQ(snapshots.size(), 0);
    SnapshotContext ctx;
    begin_snapshot(meta_service.get(), cloud_unique_id, "snapshot_1", &ctx, false);
    commit_snapshot(meta_service.get(), cloud_unique_id, ctx.snapshot_id, ctx.image_url, 1000);
    list_snapshot(meta_service.get(), cloud_unique_id, &snapshots);
    ASSERT_EQ(snapshots.size(), 1);

    // clone instance from snapshot
    std::string clone_instance_id = "clone_snapshot_chain_compactor_test_instance";
    std::string clone_cloud_unique_id = fmt::format("1:{}:0", clone_instance_id);
    clone_instance(meta_service.get(), instance_id, ctx.snapshot_id, clone_instance_id);
    InstanceInfoPB clone_instance_info;
    get_instance(meta_service.get(), clone_cloud_unique_id, clone_instance_info);
    resource_mgr->refresh_instance(clone_instance_id, clone_instance_info);

    // get keys by clone chain reader
    {
        TableKeys clone_table_keys;
        get_table_keys(resource_mgr.get(), txn_kv.get(), clone_instance_id, table_id,
                       clone_table_keys);
        clone_table_keys.equals(source_table_keys);

        PartitionKeys clone_partition_keys;
        get_partition_keys(resource_mgr.get(), txn_kv.get(), clone_instance_id, partition_id,
                           clone_partition_keys);
        clone_partition_keys.equals(source_partition_keys);

        IndexKeys clone_index_keys;
        get_index_keys(resource_mgr.get(), txn_kv.get(), clone_instance_id, index_id,
                       clone_index_keys);
        clone_index_keys.index_meta_version = source_index_keys.index_meta_version;
        clone_index_keys.equals(source_index_keys);

        TabletKeys clone_tablet_keys(old_tablet_id);
        get_tablet_keys(resource_mgr.get(), txn_kv.get(), clone_instance_id, clone_tablet_keys);
        clone_tablet_keys.equals(source_tablet_keys);

        TabletKeys clone_new_tablet_keys(new_tablet_id_1);
        get_tablet_keys(resource_mgr.get(), txn_kv.get(), clone_instance_id, clone_new_tablet_keys);
        clone_new_tablet_keys.equals(source_new_tablet_keys);

        TabletKeys clone_new_tablet2_keys(new_tablet_id_2);
        get_tablet_keys(resource_mgr.get(), txn_kv.get(), clone_instance_id,
                        clone_new_tablet2_keys);
        clone_new_tablet2_keys.equals(source_new_tablet2_keys);
    }

    // compact snapshot keys
    InstanceChainCompactor compactor(txn_kv, clone_instance_info);
    ASSERT_EQ(compactor.do_compact(), 0);

    {
        TableKeys table_keys;
        get_table_keys(txn_kv.get(), clone_instance_id, table_id, table_keys);
        table_keys.equals(source_table_keys);

        PartitionKeys partition_keys;
        get_partition_keys(txn_kv.get(), clone_instance_id, partition_id, partition_keys);
        partition_keys.equals(source_partition_keys);

        IndexKeys index_keys;
        get_index_keys(txn_kv.get(), clone_instance_id, index_id, index_keys);
        index_keys.equals(source_index_keys);

        TabletKeys tablet_keys(old_tablet_id);
        get_tablet_keys(txn_kv.get(), clone_instance_id, tablet_keys);
        tablet_keys.equals(source_tablet_keys);

        TabletKeys new_tablet_keys(new_tablet_id_1);
        get_tablet_keys(txn_kv.get(), instance_id, new_tablet_keys);
        new_tablet_keys.equals(source_new_tablet_keys);
        TabletKeys new_tablet2_keys(new_tablet_id_2);
        get_tablet_keys(txn_kv.get(), instance_id, new_tablet2_keys);
        new_tablet2_keys.equals(source_new_tablet2_keys);

        {
            std::vector<doris::RowsetMetaCloudPB> rowsets;
            get_rowsets(meta_service.get(), clone_cloud_unique_id, old_tablet_id, 0, INT64_MAX,
                        rowsets);
            ASSERT_EQ(rowsets.size(), 6); // [0-1], [2-2], [3-3], [4-4], [5-5], [6-6]
            std::vector<std::pair<int64_t, int64_t>> expected_versions = {{0, 1}, {2, 2}, {3, 3},
                                                                          {4, 4}, {5, 5}, {6, 6}};
            for (auto i = 0; i < rowsets.size(); ++i) {
                ASSERT_EQ(rowsets[i].start_version(), expected_versions[i].first) << i;
                ASSERT_EQ(rowsets[i].end_version(), expected_versions[i].second) << i;
            }
        }

        {
            std::vector<doris::RowsetMetaCloudPB> rowsets;
            get_rowsets(meta_service.get(), clone_cloud_unique_id, new_tablet_id_2, 0, INT64_MAX,
                        rowsets);
            ASSERT_EQ(rowsets.size(), 9); // [0-1], [2-2], [3-3], ..., [9-9]
            std::vector<std::pair<int64_t, int64_t>> expected_versions = {
                    {0, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}, {7, 7}, {8, 8}, {9, 9}};
            for (auto i = 0; i < rowsets.size(); ++i) {
                ASSERT_EQ(rowsets[i].start_version(), expected_versions[i].first) << i;
                ASSERT_EQ(rowsets[i].end_version(), expected_versions[i].second) << i;
            }
        }
    }
}

class HttpContext {
public:
    HttpContext(MetaServiceProxy* meta_service) : meta_service_(meta_service) {
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
        server.AddService(meta_service_, brpc::ServiceOwnership::SERVER_DOESNT_OWN_SERVICE);
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
            EXPECT_TRUE(s.ok()) << __PRETTY_FUNCTION__ << " Parse JSON: " << s.ToString()
                                << ", body: " << response_body;
            return {status_code, std::move(resp)};
        } else if constexpr (std::is_same_v<std::string, Response>) {
            return {status_code, std::move(response_body)};
        } else {
            return {status_code, {}};
        }
    }

private:
    MetaServiceProxy* meta_service_;
    brpc::Server server;
};

TEST(SnapshotChainCompactorTest, CompactMultiChain) {
    config::force_immediate_recycle = true;
    auto meta_service = get_meta_service();
    auto resource_mgr = meta_service->resource_mgr();
    auto txn_kv = meta_service->txn_kv();

    // Phase 1: create instance1
    std::string instance_id = "snapshot_chain_compactor_test_instance1";
    std::string cloud_unique_id = fmt::format("1:{}:0", instance_id);
    create_and_refresh_instance(meta_service.get(), instance_id);
    InstanceInfoPB instance_info1;
    get_instance(meta_service.get(), cloud_unique_id, instance_info1);

    int64_t db_id = 1, table_id = 2, index_id = 3, partition_id = 4, tablet_id = 5;
    // create partition/index/tablet
    prepare_and_commit_index(meta_service.get(), cloud_unique_id, db_id, table_id, index_id);
    prepare_and_commit_partition(meta_service.get(), cloud_unique_id, db_id, table_id, partition_id,
                                 index_id);
    create_tablet(meta_service.get(), cloud_unique_id, db_id, table_id, index_id, partition_id,
                  tablet_id);

    std::shared_ptr<StorageVaultAccessor> accessor = nullptr;
    {
        InstanceRecycler recycler(txn_kv, instance_info1, thread_group,
                                  std::make_shared<TxnLazyCommitter>(txn_kv));
        ASSERT_EQ(recycler.init(), 0);
        ASSERT_EQ(recycler.accessor_map_.size(), 1);
        accessor = recycler.accessor_map_.begin()->second;
        ASSERT_TRUE(accessor != nullptr);
    }
    auto compact_rowsets = [&](const std::string& cloud_unique_id, int64_t max_version,
                               int64_t start_version, int64_t end_version, int64_t before_rowsets,
                               int64_t after_rowsets) {
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id, tablet_id, 0, max_version, rowsets);
        ASSERT_EQ(rowsets.size(), before_rowsets);

        compact_rowsets_cumulative(meta_service.get(), cloud_unique_id, db_id, "compaction_label_1",
                                   table_id, partition_id, tablet_id, start_version, end_version,
                                   300);

        rowsets.clear();
        get_rowsets(meta_service.get(), cloud_unique_id, tablet_id, 0, max_version, rowsets);
        ASSERT_EQ(rowsets.size(), after_rowsets);
    };

    // Phase 1.1: insert 5 rowsets (version 2-6)
    for (int i = 0; i < 5; i++) {
        insert_rowset(meta_service.get(), cloud_unique_id, db_id, fmt::format("label_{}", i),
                      table_id, partition_id, tablet_id, nullptr, accessor.get());
    }

    // Phase 1.2: snapshot1
    SnapshotContext ctx1;
    begin_and_commit_snapshot(meta_service.get(), cloud_unique_id, ctx1);
    std::vector<SnapshotContext> snapshots;
    list_snapshot(meta_service.get(), cloud_unique_id, &snapshots);
    ASSERT_EQ(snapshots.size(), 1);

    // Phase 1.3: compact version 2-6
    compact_rowsets(cloud_unique_id, 6, 2, 6, 6, 2);

    // Phase 2: clone instance2 from snapshot1
    std::string instance_id2 = "snapshot_chain_compactor_test_instance2";
    std::string cloud_unique_id2 = fmt::format("1:{}:0", instance_id2);
    SnapshotContext ctx2;
    InstanceInfoPB instance_info2;
    {
        clone_and_refresh_instance(meta_service.get(), resource_mgr.get(), instance_id,
                                   ctx1.snapshot_id, instance_id2, instance_info2);
        // Phase 2.1: snapshot2
        begin_and_commit_snapshot(meta_service.get(), cloud_unique_id2, ctx2);
        // Phase 2.2: compact version 2-6
        compact_rowsets(cloud_unique_id2, 6, 2, 6, 6, 2);
    }

    // Phase 3: clone instance3 from snapshot2
    std::string instance_id3 = "snapshot_chain_compactor_test_instance3";
    std::string cloud_unique_id3 = fmt::format("1:{}:0", instance_id3);
    SnapshotContext ctx3;
    InstanceInfoPB instance_info3;
    {
        clone_and_refresh_instance(meta_service.get(), resource_mgr.get(), instance_id2,
                                   ctx2.snapshot_id, instance_id3, instance_info3);

        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id3, tablet_id, 0, 6, rowsets);
        ASSERT_EQ(rowsets.size(), 6);
        // Phase 3.1: snapshot3
        begin_and_commit_snapshot(meta_service.get(), cloud_unique_id3, ctx3);
        // Phase 3.2: compact version 2-6
        compact_rowsets(cloud_unique_id3, 6, 2, 6, 6, 2);
    }

    // Phase 4: clone instance4 from snapshot3
    std::string instance_id4 = "snapshot_chain_compactor_test_instance4";
    std::string cloud_unique_id4 = fmt::format("1:{}:0", instance_id4);
    SnapshotContext ctx4;
    InstanceInfoPB instance_info4;
    {
        clone_and_refresh_instance(meta_service.get(), resource_mgr.get(), instance_id3,
                                   ctx3.snapshot_id, instance_id4, instance_info4);

        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id4, tablet_id, 0, 6, rowsets);
        ASSERT_EQ(rowsets.size(), 6);
        for (size_t i = 1; i < rowsets.size(); ++i) {
            std::unique_ptr<ListIterator> list_iter;
            ASSERT_EQ(0, accessor->list_directory(
                                 rowset_path_prefix(tablet_id, rowsets[i].rowset_id_v2()),
                                 &list_iter));
            EXPECT_TRUE(list_iter->has_next());
        }
    }

    // Phase 5: compact snapshot chain for instance2/instance3
    {
        SnapshotChainCompactor snapshot_chain_compactor(txn_kv);
        ASSERT_TRUE(snapshot_chain_compactor.is_snapshot_chain_need_compact(instance_info2));
        InstanceChainCompactor compactor(txn_kv, instance_info2);
        ASSERT_EQ(compactor.do_compact(), 0);
        get_instance(meta_service.get(), cloud_unique_id2, instance_info2);
        resource_mgr->refresh_instance(instance_id2, instance_info2);
        drop_snapshot(meta_service.get(), cloud_unique_id, ctx1.snapshot_id);
    }
    {
        SnapshotChainCompactor snapshot_chain_compactor(txn_kv);
        ASSERT_TRUE(snapshot_chain_compactor.is_snapshot_chain_need_compact(instance_info3));
        InstanceChainCompactor compactor(txn_kv, instance_info3);
        ASSERT_EQ(compactor.do_compact(), 0);
        get_instance(meta_service.get(), cloud_unique_id3, instance_info3);
        resource_mgr->refresh_instance(instance_id3, instance_info3);
        drop_snapshot(meta_service.get(), cloud_unique_id2, ctx2.snapshot_id);
    }

    // Phase 6: recycle instance3/instance2/instance1
    auto recycle_instance = [&](const InstanceInfoPB& instance_info) {
        {
            auto recycler = get_instance_recycler(meta_service.get(), instance_info, accessor);
            ASSERT_EQ(recycler->init(), 0);
            ASSERT_EQ(recycler->recycle_cluster_snapshots(), 0);
            // ASSERT_EQ(recycler->do_recycle(), 0);
        }
        {
            auto recycler = get_instance_recycler(meta_service.get(), instance_info, accessor);
            ASSERT_EQ(recycler->init(), 0);
            // ASSERT_EQ(recycler->do_recycle(), 0);
            ASSERT_EQ(recycler->recycle_operation_logs(), 0);
            ASSERT_EQ(recycler->recycle_rowsets(), 0);
        }
    };
    recycle_instance(instance_info3);
    recycle_instance(instance_info2);
    recycle_instance(instance_info1);

    // Phase 7: check rowsets
    {
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id3, tablet_id, 0, 6, rowsets);
        ASSERT_EQ(rowsets.size(), 2);
    }
    {
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id4, tablet_id, 0, 6, rowsets);
        ASSERT_EQ(rowsets.size(), 6);
        for (size_t i = 1; i < rowsets.size(); ++i) {
            std::unique_ptr<ListIterator> list_iter;
            ASSERT_EQ(0, accessor->list_directory(
                                 rowset_path_prefix(tablet_id, rowsets[i].rowset_id_v2()),
                                 &list_iter));
            ASSERT_TRUE(list_iter->has_next());
        }
    }

    // ======= later case check that the rowsets can be recycled =======

    // Phase 8: instance1 set MULTI_VERSION_DISABLED and recycle
    update_snapshot_properties(meta_service.get(), instance_id, false, 0, 3660);
    HttpContext ctx(meta_service.get());
    {
        auto [http_code, response] = ctx.query<MetaServiceResponseStatus>(
                "set_multi_version_status",
                fmt::format("instance_id={}&multi_version_status=MULTI_VERSION_WRITE_ONLY",
                            instance_id));
        ASSERT_EQ(http_code, 200);
        ASSERT_EQ(response.code(), MetaServiceCode::OK);
    }
    {
        auto [http_code, response] = ctx.query<MetaServiceResponseStatus>(
                "set_multi_version_status",
                fmt::format("instance_id={}&multi_version_status=MULTI_VERSION_DISABLED",
                            instance_id));
        ASSERT_EQ(http_code, 200);
        ASSERT_EQ(response.code(), MetaServiceCode::OK);
    }
    recycle_instance(instance_info1);

    // Phase 8.1: check rowsets
    std::vector<doris::RowsetMetaCloudPB> pre_rowsets;
    {
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id4, tablet_id, 0, 6, rowsets);
        ASSERT_EQ(rowsets.size(), 6);
        for (size_t i = 1; i < rowsets.size(); ++i) {
            std::unique_ptr<ListIterator> list_iter;
            ASSERT_EQ(0, accessor->list_directory(
                                 rowset_path_prefix(tablet_id, rowsets[i].rowset_id_v2()),
                                 &list_iter));
            ASSERT_TRUE(list_iter->has_next());
        }
        pre_rowsets = std::move(rowsets);
    }

    // Phase 9: instance4 compact and recycle
    compact_rowsets(cloud_unique_id4, 6, 2, 6, 6, 2);
    {
        SnapshotChainCompactor snapshot_chain_compactor(txn_kv);
        ASSERT_FALSE(snapshot_chain_compactor.is_snapshot_chain_need_compact(instance_info4));
        InstanceChainCompactor compactor(txn_kv, instance_info4);
        ASSERT_EQ(compactor.do_compact(), 0);
        get_instance(meta_service.get(), cloud_unique_id4, instance_info4);
        resource_mgr->refresh_instance(instance_id4, instance_info4);
    }
    recycle_instance(instance_info4);

    // Phase 10: instance3 drop snapshot and recycle
    {
        drop_snapshot(meta_service.get(), cloud_unique_id3, ctx3.snapshot_id);
        recycle_instance(instance_info3);
    }

    // Phase 11: check rowsets are recycled
    {
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id4, tablet_id, 0, 6, rowsets);
        ASSERT_EQ(rowsets.size(), 2);
        for (size_t i = 1; i < pre_rowsets.size(); ++i) {
            std::unique_ptr<ListIterator> list_iter;
            ASSERT_EQ(0, accessor->list_directory(
                                 rowset_path_prefix(tablet_id, pre_rowsets[i].rowset_id_v2()),
                                 &list_iter));
            ASSERT_FALSE(list_iter->has_next());
        }
    }
}
