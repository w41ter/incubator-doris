#include <brpc/channel.h>
#include <butil/strings/string_split.h>
#include <fmt/core.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <ranges>
#include <string>
#include <thread>

#include "common/config.h"
#include "common/defer.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "enterprise/snapshot/snapshot_helper.h"
#include "enterprise/snapshot/snapshot_manager.h"
#include "meta-service/meta_service.h"
#include "meta-service/meta_service_http.h"
#include "meta-store/codec.h"
#include "meta-store/document_message.h"
#include "meta-store/document_message_get_range.h"
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
#include "recycler/snapshot_data_migrator.h"
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
    if (!cloud::init_glog("enterprise_migrate_snapshot_keys_test")) {
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
// This instance is MULTI_VERSION_DISABLED by default.
void create_and_refresh_instance(MetaServiceProxy* service, std::string instance_id) {
    InstanceInfoPB instance_info;
    instance_info.set_instance_id(instance_id);
    instance_info.mutable_resource_ids()->Add(std::string(RESOURCE_ID));
    auto* obj_info = instance_info.mutable_obj_info()->Add();
    obj_info->set_id(std::string(RESOURCE_ID));
    obj_info->set_ak("mock_ak");
    obj_info->set_sk("mock_sk");

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(instance_key(instance_id), instance_info.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    service->resource_mgr()->refresh_instance(instance_id);
    ASSERT_FALSE(service->resource_mgr()->is_version_write_enabled(instance_id));
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
                int64_t tablet_id, TabletStatePB state = TabletStatePB::PB_RUNNING,
                bool mow = false) {
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
                   int64_t tablet_id, TabletStatePB state = TabletStatePB::PB_RUNNING,
                   bool mow = false) {
    brpc::Controller cntl;
    CreateTabletsRequest req;
    CreateTabletsResponse res;
    req.set_db_id(db_id);
    req.set_cloud_unique_id(cloud_unique_id);
    add_tablet(req, table_id, index_id, partition_id, tablet_id, state, mow);
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
    req.set_snapshot_meta_image_size(100);
    req.set_snapshot_logical_data_size(1000);

    brpc::Controller cntl;
    CommitSnapshotResponse res;
    meta_service->commit_snapshot(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.ShortDebugString();
}

void abort_snapshot(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                    const std::string& snapshot_id, const std::string& reason) {
    AbortSnapshotRequest req;
    req.set_cloud_unique_id(cloud_unique_id);
    req.set_snapshot_id(snapshot_id);
    req.set_reason(reason);
    req.set_request_ip("127.0.0.1");

    brpc::Controller cntl;
    AbortSnapshotResponse res;
    meta_service->abort_snapshot(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.ShortDebugString();
}

void get_snapshots(MetaServiceProxy* meta_service, const std::string& instance_id,
                   std::vector<SnapshotInfoPB>& snapshots) {
    snapshots.clear();

    auto txn_kv = meta_service->txn_kv();
    MetaReader reader(instance_id, txn_kv.get());

    std::vector<std::pair<SnapshotPB, Versionstamp>> snapshot_and_versionstamps;
    ASSERT_EQ(reader.get_snapshots(&snapshot_and_versionstamps), TxnErrorCode::TXN_OK);

    for (auto& [snapshot, v] : snapshot_and_versionstamps) {
        SnapshotInfoPB info;
        info.set_snapshot_id(v.to_string());
        if (snapshot.has_snapshot_ancestor()) {
            info.set_ancestor_id(snapshot.snapshot_ancestor());
        }
        info.set_create_at(snapshot.create_at());
        info.set_finish_at(snapshot.finish_at());
        info.set_image_url(snapshot.image_url());
        info.set_journal_id(snapshot.last_journal_id());
        info.set_status(snapshot.status());
        info.set_type(snapshot.type());
        info.set_instance_id(instance_id);
        info.set_auto_snapshot(snapshot.auto_());
        if (snapshot.has_ttl_seconds()) {
            info.set_ttl_seconds(snapshot.ttl_seconds());
        }
        info.set_timeout_seconds(snapshot.timeout_seconds());
        if (snapshot.has_label()) {
            info.set_snapshot_label(snapshot.label());
        }
        if (snapshot.has_reason()) {
            info.set_reason(snapshot.reason());
        }
        snapshots.push_back(info);
    }
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
                          int64_t lock_id, int64_t initiator, const std::string& rowset_id,
                          DeleteBitmapPB& delete_bitmap_pb, int64_t version) {
    brpc::Controller cntl;
    UpdateDeleteBitmapRequest req;
    UpdateDeleteBitmapResponse res;
    req.set_cloud_unique_id(cloud_unique_id);
    req.set_table_id(table_id);
    req.set_partition_id(partition_id);
    req.set_lock_id(lock_id);
    req.set_initiator(initiator);
    req.set_tablet_id(tablet_id);
    req.set_store_version(version);

    if (version == 1 || version == 3) {
        for (size_t i = 0; i < delete_bitmap_pb.rowset_ids_size(); ++i) {
            req.add_rowset_ids(delete_bitmap_pb.rowset_ids(i));
            req.add_segment_ids(delete_bitmap_pb.segment_ids(i));
            req.add_versions(delete_bitmap_pb.versions(i));
            req.add_segment_delete_bitmaps(delete_bitmap_pb.segment_delete_bitmaps(i));
        }
    }
    if (version == 2 || version == 3) {
        DeleteBitmapStoragePB delete_bitmap_storage_pb;
        delete_bitmap_storage_pb.set_store_in_fdb(true);
        *(delete_bitmap_storage_pb.mutable_delete_bitmap()) = std::move(delete_bitmap_pb);
        *(req.add_delete_bitmap_storages()) = std::move(delete_bitmap_storage_pb);
        req.add_delta_rowset_ids(rowset_id);
    }
    meta_service->update_delete_bitmap(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
}

void get_delete_bitmap(MetaServiceProxy* meta_service, const std::string& cloud_unique_id,
                       int64_t tablet_id, const std::string& rowset_id, int64_t version,
                       DeleteBitmapPB& delete_bitmap_pb) {
    brpc::Controller cntl;
    GetDeleteBitmapRequest req;
    GetDeleteBitmapResponse res;
    req.set_cloud_unique_id(cloud_unique_id);
    req.set_tablet_id(tablet_id);
    req.set_store_version(version);
    req.add_rowset_ids(rowset_id);
    req.add_begin_versions(0);
    req.add_end_versions(INT64_MAX);
    meta_service->get_delete_bitmap(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    if (version == 1 || version == 3) {
        for (int i = 0; i < res.rowset_ids_size(); ++i) {
            delete_bitmap_pb.add_rowset_ids(res.rowset_ids(i));
            delete_bitmap_pb.add_segment_ids(res.segment_ids(i));
            delete_bitmap_pb.add_versions(res.versions(i));
            delete_bitmap_pb.add_segment_delete_bitmaps(res.segment_delete_bitmaps(i));
        }
    }
    if (version == 2 || version == 3) {
        if (res.delete_bitmap_storages_size() > 0) {
            ASSERT_EQ(res.delete_bitmap_storages_size(), 1);
            ASSERT_EQ(res.delete_bitmap_storages(0).store_in_fdb(), true);
            delete_bitmap_pb = res.delete_bitmap_storages(0).delete_bitmap();
        }
    }
}

std::string generate_random_string(int length) {
    std::string char_set = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_int_distribution<int> distribution(0, char_set.length() - 1);

    std::string randomString;
    for (int i = 0; i < length; ++i) {
        randomString += char_set[distribution(generator)];
    }
    return randomString;
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

void enable_instance_multi_version_write_only(MetaServiceProxy* meta_service,
                                              const std::string& instance_id) {
    std::string path = "set_multi_version_status";
    std::unordered_map<std::string, std::string> params = {
            {"token", config::http_token},
            {"instance_id", instance_id},
            {"multi_version_status", "MULTI_VERSION_WRITE_ONLY"},
    };

    rapidjson::Document document;
    issue_http_request(meta_service, brpc::HTTP_METHOD_POST, path, params, &document);
    meta_service->resource_mgr()->refresh_instance(instance_id);
    ASSERT_TRUE(meta_service->resource_mgr()->is_version_write_enabled(instance_id));
}

void enable_instance_multi_version_read_write(MetaServiceProxy* meta_service,
                                              const std::string& instance_id) {
    std::string path = "set_multi_version_status";
    std::unordered_map<std::string, std::string> params = {
            {"token", config::http_token},
            {"instance_id", instance_id},
            {"multi_version_status", "MULTI_VERSION_READ_WRITE"},
    };

    rapidjson::Document document;
    issue_http_request(meta_service, brpc::HTTP_METHOD_POST, path, params, &document);
    meta_service->resource_mgr()->refresh_instance(instance_id);
    ASSERT_TRUE(meta_service->resource_mgr()->is_version_read_enabled(instance_id));
}

void enable_instance_multi_version_disabled(MetaServiceProxy* meta_service,
                                            const std::string& instance_id) {
    std::string path = "set_multi_version_status";
    std::unordered_map<std::string, std::string> params = {
            {"token", config::http_token},
            {"instance_id", instance_id},
            {"multi_version_status", "MULTI_VERSION_DISABLED"},
    };

    rapidjson::Document document;
    issue_http_request(meta_service, brpc::HTTP_METHOD_POST, path, params, &document);
    meta_service->resource_mgr()->refresh_instance(instance_id);
    ASSERT_FALSE(meta_service->resource_mgr()->is_version_read_enabled(instance_id));
    ASSERT_FALSE(meta_service->resource_mgr()->is_version_write_enabled(instance_id));
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

void get_instance_snapshot_properties(MetaServiceProxy* meta_service,
                                      const std::string& instance_id, SnapshotProperty* property) {
    std::string path = "get_snapshot_property";
    std::unordered_map<std::string, std::string> params = {
            {"token", config::http_token},
            {"instance_id", instance_id},
    };

    rapidjson::Document document;
    issue_http_request(meta_service, brpc::HTTP_METHOD_GET, path, params, &document);
    ASSERT_TRUE(document.HasMember("result")) << dump_rapidjson(document);
    auto& data = document["result"];
    ASSERT_TRUE(data.IsObject());
    ASSERT_TRUE(data.HasMember("status"));
    std::string status_str = data["status"].GetString();
    if (status_str == "UNSUPPORTED") {
        property->status = SnapshotSwitchStatus::SNAPSHOT_SWITCH_DISABLED;
    } else if (status_str == "ENABLED") {
        property->status = SnapshotSwitchStatus::SNAPSHOT_SWITCH_ON;
    } else if (status_str == "DISABLED") {
        property->status = SnapshotSwitchStatus::SNAPSHOT_SWITCH_OFF;
    } else {
        ASSERT_TRUE(false) << "Unknown snapshot status: " << status_str;
    }

    if (data.HasMember("max_reserved_snapshots")) {
        property->max_reserved_snapshots = data["max_reserved_snapshots"].GetInt64();
    }
    if (data.HasMember("snapshot_interval_seconds")) {
        property->snapshot_interval_seconds = data["snapshot_interval_seconds"].GetInt64();
    }
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

void tablet_stats_must_equals(const TabletStatsPB& old_stats, const TabletStatsPB& new_stats) {
    ASSERT_EQ(new_stats.base_compaction_cnt(), old_stats.base_compaction_cnt());
    ASSERT_EQ(new_stats.cumulative_compaction_cnt(), old_stats.cumulative_compaction_cnt());
    ASSERT_EQ(new_stats.full_compaction_cnt(), old_stats.full_compaction_cnt());
    ASSERT_EQ(new_stats.cumulative_point(), old_stats.cumulative_point());
    ASSERT_EQ(new_stats.num_rows(), old_stats.num_rows());
    ASSERT_EQ(new_stats.num_segments(), old_stats.num_segments());
    ASSERT_EQ(new_stats.num_rowsets(), old_stats.num_rowsets());
    ASSERT_EQ(new_stats.data_size(), old_stats.data_size());
    ASSERT_EQ(new_stats.index_size(), old_stats.index_size());
    ASSERT_EQ(new_stats.segment_size(), old_stats.segment_size());
}

TEST(MigrateSnapshotKeysTest, Basic) {
    auto meta_service = get_meta_service();
    auto txn_kv = meta_service->txn_kv();
    std::string instance_id = "migrate_snapshot_keys_test_instance";
    std::string cloud_unique_id = fmt::format("1:{}:0", instance_id);
    create_and_refresh_instance(meta_service.get(), instance_id);
    int64_t db_id = 1, table_id = 2, index_id = 3, partition_id = 4, tablet_id = 5;
    // create partition/index/tablet
    prepare_and_commit_index(meta_service.get(), cloud_unique_id, db_id, table_id, index_id);
    prepare_and_commit_partition(meta_service.get(), cloud_unique_id, db_id, table_id, partition_id,
                                 index_id);
    create_tablet(meta_service.get(), cloud_unique_id, db_id, table_id, index_id, partition_id,
                  tablet_id);

    insert_rowset(meta_service.get(), cloud_unique_id, db_id, "label_1", table_id, partition_id,
                  tablet_id);

    enable_instance_multi_version_write_only(meta_service.get(), instance_id);

    TabletStatsPB old_tablet_stats;
    get_tablet_stats(meta_service.get(), cloud_unique_id, tablet_id, old_tablet_stats);

    // migrate old keys and switch to multi-version read write
    {
        InstanceInfoPB instance_info;
        get_instance(meta_service.get(), cloud_unique_id, instance_info);
        InstanceDataMigrator migrator(txn_kv, instance_info);
        ASSERT_EQ(migrator.do_migrate(), 0);
        enable_instance_multi_version_read_write(meta_service.get(), instance_id);

        // Check the snapshot properties
        SnapshotProperty property;
        get_instance_snapshot_properties(meta_service.get(), instance_id, &property);
        ASSERT_EQ(property.status, SnapshotSwitchStatus::SNAPSHOT_SWITCH_OFF);
    }

    int64_t partition_version = -1, table_version = -1;
    get_partition_version(meta_service.get(), cloud_unique_id, db_id, table_id, partition_id,
                          &partition_version);
    get_table_version(meta_service.get(), cloud_unique_id, db_id, table_id, &table_version);
    ASSERT_EQ(partition_version, 2);
    ASSERT_GE(table_version, 1);

    TabletMetaCloudPB tablet_meta;
    get_tablet_meta(meta_service.get(), cloud_unique_id, tablet_id, &tablet_meta);
    ASSERT_EQ(tablet_meta.tablet_id(), tablet_id);

    std::vector<doris::RowsetMetaCloudPB> rowsets;
    get_rowsets(meta_service.get(), cloud_unique_id, tablet_id, 0, 2, rowsets);
    ASSERT_EQ(rowsets.size(), 2);
    ASSERT_EQ(rowsets[0].start_version(), 0);
    ASSERT_EQ(rowsets[0].end_version(), 1);
    ASSERT_EQ(rowsets[1].start_version(), 2);
    ASSERT_EQ(rowsets[1].end_version(), 2);

    TabletStatsPB new_tablet_stats;
    get_tablet_stats(meta_service.get(), cloud_unique_id, tablet_id, new_tablet_stats);
    tablet_stats_must_equals(new_tablet_stats, old_tablet_stats);

    // Switch back to multi-version disabled, test the tablet stats is compatible.
    enable_instance_multi_version_disabled(meta_service.get(), instance_id);
    get_tablet_stats(meta_service.get(), cloud_unique_id, tablet_id, old_tablet_stats);
    tablet_stats_must_equals(new_tablet_stats, old_tablet_stats);
}

TEST(MigrateSnapshotKeysTest, Insert) {
    auto meta_service = get_meta_service();
    auto txn_kv = meta_service->txn_kv();
    std::string instance_id = "migrate_snapshot_keys_test_instance";
    std::string cloud_unique_id = fmt::format("1:{}:0", instance_id);
    create_and_refresh_instance(meta_service.get(), instance_id);
    int64_t db_id = 1, table_id = 2, index_id = 3, partition_id = 4, tablet_id = 5;
    // create partition/index/tablet
    prepare_and_commit_index(meta_service.get(), cloud_unique_id, db_id, table_id, index_id);
    prepare_and_commit_partition(meta_service.get(), cloud_unique_id, db_id, table_id, partition_id,
                                 index_id);
    create_tablet(meta_service.get(), cloud_unique_id, db_id, table_id, index_id, partition_id,
                  tablet_id);

    // insert some rowsets
    for (int i = 0; i < 10; i++) {
        insert_rowset(meta_service.get(), cloud_unique_id, db_id, fmt::format("label_{}", i),
                      table_id, partition_id, tablet_id);
    }

    // switch to multi-version write only
    enable_instance_multi_version_write_only(meta_service.get(), instance_id);

    // check the rowset metas
    {
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id, tablet_id, 0, 10, rowsets);
        ASSERT_EQ(rowsets.size(), 10);
    }

    // insert more rowsets
    for (int i = 10; i < 20; i++) {
        insert_rowset(meta_service.get(), cloud_unique_id, db_id, fmt::format("label_{}", i),
                      table_id, partition_id, tablet_id);
    }

    // check the rowset metas
    {
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id, tablet_id, 0, 20, rowsets);
        ASSERT_EQ(rowsets.size(), 20);
    }

    TabletStatsPB old_tablet_stats;
    get_tablet_stats(meta_service.get(), cloud_unique_id, tablet_id, old_tablet_stats);

    // migrate old keys and switch to multi-version read write
    {
        InstanceInfoPB instance_info;
        get_instance(meta_service.get(), cloud_unique_id, instance_info);
        InstanceDataMigrator migrator(txn_kv, instance_info);
        ASSERT_EQ(migrator.do_migrate(), 0);
        enable_instance_multi_version_read_write(meta_service.get(), instance_id);

        // Check the snapshot properties
        SnapshotProperty property;
        get_instance_snapshot_properties(meta_service.get(), instance_id, &property);
        ASSERT_EQ(property.status, SnapshotSwitchStatus::SNAPSHOT_SWITCH_OFF);
    }

    // Get the rowset metas again
    std::vector<doris::RowsetMetaCloudPB> rowsets;
    get_rowsets(meta_service.get(), cloud_unique_id, tablet_id, 0, 20, rowsets);
    ASSERT_EQ(rowsets.size(), 20);

    int64_t partition_version = -1, table_version = -1;
    get_partition_version(meta_service.get(), cloud_unique_id, db_id, table_id, partition_id,
                          &partition_version);
    get_table_version(meta_service.get(), cloud_unique_id, db_id, table_id, &table_version);
    ASSERT_EQ(partition_version, 21);
    ASSERT_GE(table_version, 21);

    TabletMetaCloudPB tablet_meta;
    get_tablet_meta(meta_service.get(), cloud_unique_id, tablet_id, &tablet_meta);
    ASSERT_EQ(tablet_meta.tablet_id(), tablet_id);

    TabletStatsPB new_tablet_stats;
    get_tablet_stats(meta_service.get(), cloud_unique_id, tablet_id, new_tablet_stats);
    tablet_stats_must_equals(new_tablet_stats, old_tablet_stats);

    // Switch back to multi-version disabled, test the tablet stats is compatible.
    enable_instance_multi_version_disabled(meta_service.get(), instance_id);
    get_tablet_stats(meta_service.get(), cloud_unique_id, tablet_id, old_tablet_stats);
    tablet_stats_must_equals(new_tablet_stats, old_tablet_stats);
}

TEST(MigrateSnapshotKeysTest, Compaction) {
    auto meta_service = get_meta_service();
    auto txn_kv = meta_service->txn_kv();
    std::string instance_id = "migrate_snapshot_keys_compaction_test_instance";
    std::string cloud_unique_id = fmt::format("1:{}:0", instance_id);
    create_and_refresh_instance(meta_service.get(), instance_id);
    int64_t db_id = 1, table_id = 2, index_id = 3, partition_id = 4, tablet_id = 5;

    // create partition/index/tablet
    prepare_and_commit_index(meta_service.get(), cloud_unique_id, db_id, table_id, index_id);
    prepare_and_commit_partition(meta_service.get(), cloud_unique_id, db_id, table_id, partition_id,
                                 index_id);
    create_tablet(meta_service.get(), cloud_unique_id, db_id, table_id, index_id, partition_id,
                  tablet_id);

    // Phase 1: Single version mode - insert 5 rowsets (version 2-6)
    for (int i = 0; i < 5; i++) {
        insert_rowset(meta_service.get(), cloud_unique_id, db_id, fmt::format("label_{}", i),
                      table_id, partition_id, tablet_id);
    }

    // Phase 1: Single version mode - compact version 2-4
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

    // Phase 2: Switch to multi-version write only
    enable_instance_multi_version_write_only(meta_service.get(), instance_id);

    // Phase 2: Dual write mode - insert 3 more rowsets (version 7-9)
    for (int i = 5; i < 8; i++) {
        insert_rowset(meta_service.get(), cloud_unique_id, db_id, fmt::format("label_{}", i),
                      table_id, partition_id, tablet_id);
    }

    // Phase 2: Dual write mode - compact version 5-7 (cross migration boundary)
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

    TabletStatsPB old_tablet_stats;
    get_tablet_stats(meta_service.get(), cloud_unique_id, tablet_id, old_tablet_stats);

    // Phase 3: Migrate old keys and switch to multi-version read write
    {
        InstanceInfoPB instance_info;
        get_instance(meta_service.get(), cloud_unique_id, instance_info);
        InstanceDataMigrator migrator(txn_kv, instance_info);
        ASSERT_EQ(migrator.do_migrate(), 0);
        enable_instance_multi_version_read_write(meta_service.get(), instance_id);

        // Check the snapshot properties
        SnapshotProperty property;
        get_instance_snapshot_properties(meta_service.get(), instance_id, &property);
        ASSERT_EQ(property.status, SnapshotSwitchStatus::SNAPSHOT_SWITCH_OFF);
    }

    TabletStatsPB new_tablet_stats;
    get_tablet_stats(meta_service.get(), cloud_unique_id, tablet_id, new_tablet_stats);
    tablet_stats_must_equals(new_tablet_stats, old_tablet_stats);

    // Phase 4: Multi-version mode - insert 2 more rowsets (version 10-11)
    for (int i = 8; i < 10; i++) {
        insert_rowset(meta_service.get(), cloud_unique_id, db_id, fmt::format("label_{}", i),
                      table_id, partition_id, tablet_id);
    }

    // Phase 4: Multi-version mode - compact version 8-10
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

    // Final check: Get all rowsets and verify versions
    {
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id, tablet_id, 0, 11, rowsets);
        ASSERT_EQ(rowsets.size(), 5); // [0-1], [2-4], [5-7], [8-10], [11-11]

        // Verify version continuity
        ASSERT_EQ(rowsets[0].start_version(), 0);
        ASSERT_EQ(rowsets[0].end_version(), 1);
        ASSERT_EQ(rowsets[1].start_version(), 2);
        ASSERT_EQ(rowsets[1].end_version(), 4);
        ASSERT_EQ(rowsets[2].start_version(), 5);
        ASSERT_EQ(rowsets[2].end_version(), 7);
        ASSERT_EQ(rowsets[3].start_version(), 8);
        ASSERT_EQ(rowsets[3].end_version(), 10);
        ASSERT_EQ(rowsets[4].start_version(), 11);
        ASSERT_EQ(rowsets[4].end_version(), 11);
    }

    int64_t partition_version = -1, table_version = -1;
    get_partition_version(meta_service.get(), cloud_unique_id, db_id, table_id, partition_id,
                          &partition_version);
    get_table_version(meta_service.get(), cloud_unique_id, db_id, table_id, &table_version);
    ASSERT_EQ(partition_version, 11);
    ASSERT_GE(table_version, 11);
    TabletMetaCloudPB tablet_meta;
    get_tablet_meta(meta_service.get(), cloud_unique_id, tablet_id, &tablet_meta);
    ASSERT_EQ(tablet_meta.tablet_id(), tablet_id);

    // Switch back to multi-version disabled, test the tablet stats is compatible.
    get_tablet_stats(meta_service.get(), cloud_unique_id, tablet_id, new_tablet_stats);
    enable_instance_multi_version_disabled(meta_service.get(), instance_id);
    get_tablet_stats(meta_service.get(), cloud_unique_id, tablet_id, old_tablet_stats);
    tablet_stats_must_equals(new_tablet_stats, old_tablet_stats);
}

TEST(MigrateSnapshotKeysTest, CompactionAfterMigrated) {
    auto meta_service = get_meta_service();
    auto txn_kv = meta_service->txn_kv();
    std::string instance_id = "migrate_snapshot_keys_compaction_after_migrated_test_instance";
    std::string cloud_unique_id = fmt::format("1:{}:0", instance_id);
    create_and_refresh_instance(meta_service.get(), instance_id);
    int64_t db_id = 1, table_id = 2, index_id = 3, partition_id = 4, tablet_id = 5;

    // create partition/index/tablet
    prepare_and_commit_index(meta_service.get(), cloud_unique_id, db_id, table_id, index_id);
    prepare_and_commit_partition(meta_service.get(), cloud_unique_id, db_id, table_id, partition_id,
                                 index_id);
    create_tablet(meta_service.get(), cloud_unique_id, db_id, table_id, index_id, partition_id,
                  tablet_id);

    for (int i = 0; i < 5; i++) {
        insert_rowset(meta_service.get(), cloud_unique_id, db_id, fmt::format("label_{}", i),
                      table_id, partition_id, tablet_id);
    }

    TabletStatsPB old_tablet_stats;
    get_tablet_stats(meta_service.get(), cloud_unique_id, tablet_id, old_tablet_stats);

    enable_instance_multi_version_write_only(meta_service.get(), instance_id);

    // Migrate old keys and switch to multi-version read write
    {
        InstanceInfoPB instance_info;
        get_instance(meta_service.get(), cloud_unique_id, instance_info);
        InstanceDataMigrator migrator(txn_kv, instance_info);
        ASSERT_EQ(migrator.do_migrate(), 0);
        enable_instance_multi_version_read_write(meta_service.get(), instance_id);

        // Check the snapshot properties
        SnapshotProperty property;
        get_instance_snapshot_properties(meta_service.get(), instance_id, &property);
        ASSERT_EQ(property.status, SnapshotSwitchStatus::SNAPSHOT_SWITCH_OFF);
    }

    // Check tablet stats after migration
    TabletStatsPB new_tablet_stats;
    get_tablet_stats(meta_service.get(), cloud_unique_id, tablet_id, new_tablet_stats);
    tablet_stats_must_equals(new_tablet_stats, old_tablet_stats);

    // Phase 1: Single version mode - compact version 2-4
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

    // Phase 2: Dual write mode - insert 3 more rowsets (version 7-9)
    for (int i = 5; i < 8; i++) {
        insert_rowset(meta_service.get(), cloud_unique_id, db_id, fmt::format("label_{}", i),
                      table_id, partition_id, tablet_id);
    }

    // Phase 2: Dual write mode - compact version 5-7 (cross migration boundary)
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

    // Phase 4: Multi-version mode - insert 2 more rowsets (version 10-11)
    for (int i = 8; i < 10; i++) {
        insert_rowset(meta_service.get(), cloud_unique_id, db_id, fmt::format("label_{}", i),
                      table_id, partition_id, tablet_id);
    }

    // Phase 4: Multi-version mode - compact version 8-10
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

    // Final check: Get all rowsets and verify versions
    {
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id, tablet_id, 0, 11, rowsets);
        ASSERT_EQ(rowsets.size(), 5); // [0-1], [2-4], [5-7], [8-10], [11-11]

        // Verify version continuity
        ASSERT_EQ(rowsets[0].start_version(), 0);
        ASSERT_EQ(rowsets[0].end_version(), 1);
        ASSERT_EQ(rowsets[1].start_version(), 2);
        ASSERT_EQ(rowsets[1].end_version(), 4);
        ASSERT_EQ(rowsets[2].start_version(), 5);
        ASSERT_EQ(rowsets[2].end_version(), 7);
        ASSERT_EQ(rowsets[3].start_version(), 8);
        ASSERT_EQ(rowsets[3].end_version(), 10);
        ASSERT_EQ(rowsets[4].start_version(), 11);
        ASSERT_EQ(rowsets[4].end_version(), 11);
    }

    int64_t partition_version = -1, table_version = -1;
    get_partition_version(meta_service.get(), cloud_unique_id, db_id, table_id, partition_id,
                          &partition_version);
    get_table_version(meta_service.get(), cloud_unique_id, db_id, table_id, &table_version);
    ASSERT_EQ(partition_version, 11);
    ASSERT_GE(table_version, 11);
    TabletMetaCloudPB tablet_meta;
    get_tablet_meta(meta_service.get(), cloud_unique_id, tablet_id, &tablet_meta);
    ASSERT_EQ(tablet_meta.tablet_id(), tablet_id);

    // Switch back to multi-version disabled, test the tablet stats is compatible.
    get_tablet_stats(meta_service.get(), cloud_unique_id, tablet_id, new_tablet_stats);
    enable_instance_multi_version_disabled(meta_service.get(), instance_id);
    get_tablet_stats(meta_service.get(), cloud_unique_id, tablet_id, old_tablet_stats);
    tablet_stats_must_equals(new_tablet_stats, old_tablet_stats);
}

TEST(MigrateSnapshotKeysTest, SchemaChange) {
    auto meta_service = get_meta_service();
    auto txn_kv = meta_service->txn_kv();
    std::string instance_id = "migrate_snapshot_keys_schema_change_test_instance";
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
                      new_tablet_id_1, TabletStatePB::PB_NOTREADY);

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

    // Phase 2: Switch to multi-version write only
    enable_instance_multi_version_write_only(meta_service.get(), instance_id);

    // Phase 2: Dual write mode - insert 3 more rowsets to new_tablet_1 (version 7-9)
    for (int i = 5; i < 8; i++) {
        insert_rowset(meta_service.get(), cloud_unique_id, db_id, fmt::format("label_{}", i),
                      table_id, partition_id, new_tablet_id_1);
    }

    // Phase 2: Dual write mode - schema change from new_tablet_1 to new_tablet_2
    {
        std::vector<doris::RowsetMetaCloudPB> output_rowsets;
        std::string job_id = fmt::format("schema_change_{}_{}", new_tablet_id_1, new_tablet_id_2);
        int64_t alter_version = 9;

        // Create new tablet
        create_tablet(meta_service.get(), cloud_unique_id, db_id, table_id, index_id, partition_id,
                      new_tablet_id_2, TabletStatePB::PB_NOTREADY);

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

    TabletStatsPB old_tablet_stats;
    get_tablet_stats(meta_service.get(), cloud_unique_id, new_tablet_id_2, old_tablet_stats);

    // Phase 3: Migrate old keys and switch to multi-version read write
    {
        InstanceInfoPB instance_info;
        get_instance(meta_service.get(), cloud_unique_id, instance_info);
        InstanceDataMigrator migrator(txn_kv, instance_info);
        ASSERT_EQ(migrator.do_migrate(), 0);
        enable_instance_multi_version_read_write(meta_service.get(), instance_id);

        // Check the snapshot properties
        SnapshotProperty property;
        get_instance_snapshot_properties(meta_service.get(), instance_id, &property);
        ASSERT_EQ(property.status, SnapshotSwitchStatus::SNAPSHOT_SWITCH_OFF);
    }

    TabletStatsPB new_tablet_stats;
    get_tablet_stats(meta_service.get(), cloud_unique_id, new_tablet_id_2, new_tablet_stats);
    tablet_stats_must_equals(new_tablet_stats, old_tablet_stats);

    // Phase 4: Multi-version mode - insert 2 more rowsets to new_tablet_2 (version 10-11)
    for (int i = 8; i < 10; i++) {
        insert_rowset(meta_service.get(), cloud_unique_id, db_id, fmt::format("label_{}", i),
                      table_id, partition_id, new_tablet_id_2);
    }

    // Final check: Get all rowsets and verify versions on new_tablet_2
    {
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id, new_tablet_id_2, 0, 11, rowsets);
        ASSERT_EQ(rowsets.size(), 11); // [0-1], [2-2], [3-3], ..., [11-11]

        // Verify version continuity
        ASSERT_EQ(rowsets[0].start_version(), 0);
        ASSERT_EQ(rowsets[0].end_version(), 1);
        for (int i = 1; i < 11; i++) {
            ASSERT_EQ(rowsets[i].start_version(), i + 1);
            ASSERT_EQ(rowsets[i].end_version(), i + 1);
        }
    }

    int64_t partition_version = -1, table_version = -1;
    get_partition_version(meta_service.get(), cloud_unique_id, db_id, table_id, partition_id,
                          &partition_version);
    get_table_version(meta_service.get(), cloud_unique_id, db_id, table_id, &table_version);
    ASSERT_EQ(partition_version, 11);
    ASSERT_GE(table_version, 11);
    TabletMetaCloudPB tablet_meta;
    get_tablet_meta(meta_service.get(), cloud_unique_id, new_tablet_id_2, &tablet_meta);
    ASSERT_EQ(tablet_meta.tablet_id(), new_tablet_id_2);

    // Switch back to multi-version disabled, test the tablet stats is compatible.
    get_tablet_stats(meta_service.get(), cloud_unique_id, new_tablet_id_2, new_tablet_stats);
    enable_instance_multi_version_disabled(meta_service.get(), instance_id);
    get_tablet_stats(meta_service.get(), cloud_unique_id, new_tablet_id_2, old_tablet_stats);
    tablet_stats_must_equals(new_tablet_stats, old_tablet_stats);
}

TEST(MigrateSnapshotKeysTest, SchemaChangeAfterMigrated) {
    auto meta_service = get_meta_service();
    auto txn_kv = meta_service->txn_kv();
    std::string instance_id = "migrate_snapshot_keys_schema_change_after_migrated_test_instance";
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

    TabletStatsPB old_tablet_stats;
    get_tablet_stats(meta_service.get(), cloud_unique_id, old_tablet_id, old_tablet_stats);

    enable_instance_multi_version_write_only(meta_service.get(), instance_id);

    {
        InstanceInfoPB instance_info;
        get_instance(meta_service.get(), cloud_unique_id, instance_info);
        InstanceDataMigrator migrator(txn_kv, instance_info);
        ASSERT_EQ(migrator.do_migrate(), 0);
        enable_instance_multi_version_read_write(meta_service.get(), instance_id);

        // Check the snapshot properties
        SnapshotProperty property;
        get_instance_snapshot_properties(meta_service.get(), instance_id, &property);
        ASSERT_EQ(property.status, SnapshotSwitchStatus::SNAPSHOT_SWITCH_OFF);
    }

    TabletStatsPB new_tablet_stats;
    get_tablet_stats(meta_service.get(), cloud_unique_id, old_tablet_id, new_tablet_stats);
    tablet_stats_must_equals(new_tablet_stats, old_tablet_stats);

    // Phase 1: Single version mode - schema change from old_tablet to new_tablet_1
    {
        std::vector<doris::RowsetMetaCloudPB> output_rowsets;
        std::string job_id = fmt::format("schema_change_{}_{}", old_tablet_id, new_tablet_id_1);
        int64_t alter_version = 6;

        // Create new tablet
        create_tablet(meta_service.get(), cloud_unique_id, db_id, table_id, index_id, partition_id,
                      new_tablet_id_1, TabletStatePB::PB_NOTREADY);

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

    // Phase 2: Dual write mode - insert 3 more rowsets to new_tablet_1 (version 7-9)
    for (int i = 5; i < 8; i++) {
        insert_rowset(meta_service.get(), cloud_unique_id, db_id, fmt::format("label_{}", i),
                      table_id, partition_id, new_tablet_id_1);
    }

    // Phase 2: Dual write mode - schema change from new_tablet_1 to new_tablet_2
    {
        std::vector<doris::RowsetMetaCloudPB> output_rowsets;
        std::string job_id = fmt::format("schema_change_{}_{}", new_tablet_id_1, new_tablet_id_2);
        int64_t alter_version = 9;

        // Create new tablet
        create_tablet(meta_service.get(), cloud_unique_id, db_id, table_id, index_id, partition_id,
                      new_tablet_id_2, TabletStatePB::PB_NOTREADY);

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

    // Phase 3: Multi-version mode - insert 2 more rowsets to new_tablet_2 (version 10-11)
    for (int i = 8; i < 10; i++) {
        insert_rowset(meta_service.get(), cloud_unique_id, db_id, fmt::format("label_{}", i),
                      table_id, partition_id, new_tablet_id_2);
    }

    // Final check: Get all rowsets and verify versions on new_tablet_2
    {
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id, new_tablet_id_2, 0, 11, rowsets);
        ASSERT_EQ(rowsets.size(), 11); // [0-1], [2-2], [3-3], ..., [11-11]

        // Verify version continuity
        ASSERT_EQ(rowsets[0].start_version(), 0);
        ASSERT_EQ(rowsets[0].end_version(), 1);
        for (int i = 1; i < 11; i++) {
            ASSERT_EQ(rowsets[i].start_version(), i + 1);
            ASSERT_EQ(rowsets[i].end_version(), i + 1);
        }
    }

    int64_t partition_version = -1, table_version = -1;
    get_partition_version(meta_service.get(), cloud_unique_id, db_id, table_id, partition_id,
                          &partition_version);
    get_table_version(meta_service.get(), cloud_unique_id, db_id, table_id, &table_version);
    ASSERT_EQ(partition_version, 11);
    ASSERT_GE(table_version, 11);
    TabletMetaCloudPB tablet_meta;
    get_tablet_meta(meta_service.get(), cloud_unique_id, new_tablet_id_2, &tablet_meta);
    ASSERT_EQ(tablet_meta.tablet_id(), new_tablet_id_2);

    // Switch back to multi-version disabled, test the tablet stats is compatible.
    get_tablet_stats(meta_service.get(), cloud_unique_id, new_tablet_id_2, new_tablet_stats);
    enable_instance_multi_version_disabled(meta_service.get(), instance_id);
    get_tablet_stats(meta_service.get(), cloud_unique_id, new_tablet_id_2, old_tablet_stats);
    tablet_stats_must_equals(new_tablet_stats, old_tablet_stats);
}

// Like the previous test SchemaChangeAfterMigrated, but do some compaction before schema change
TEST(MigrateSnapshotKeysTest, SchemaChangeAfterMigrated2) {
    auto meta_service = get_meta_service();
    auto txn_kv = meta_service->txn_kv();
    std::string instance_id = "migrate_snapshot_keys_schema_change_after_migrated_2_test_instance";
    std::string cloud_unique_id = fmt::format("1:{}:0", instance_id);
    create_and_refresh_instance(meta_service.get(), instance_id);
    int64_t db_id = 1, table_id = 2, index_id = 3, partition_id = 4;
    int64_t old_tablet_id = 5, new_tablet_id_1 = 6;

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

    // Compact the rowsets, to update the tablet stats key, so that we can verify the tablet stats
    // consistency after migration/schema change.
    {
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id, old_tablet_id, 0, 6, rowsets);
        ASSERT_EQ(rowsets.size(), 6);
        compact_rowsets_cumulative(meta_service.get(), cloud_unique_id, db_id,
                                   "compaction_label_before_migration", table_id, partition_id,
                                   old_tablet_id, 2, 6, 300);
        // Verify: should have 2 rowsets: [0-1], [2-6]
        rowsets.clear();
        get_rowsets(meta_service.get(), cloud_unique_id, old_tablet_id, 0, 6, rowsets);
        ASSERT_EQ(rowsets.size(), 2);
    }

    TabletStatsPB old_tablet_stats;
    get_tablet_stats(meta_service.get(), cloud_unique_id, old_tablet_id, old_tablet_stats);

    enable_instance_multi_version_write_only(meta_service.get(), instance_id);

    {
        InstanceInfoPB instance_info;
        get_instance(meta_service.get(), cloud_unique_id, instance_info);
        InstanceDataMigrator migrator(txn_kv, instance_info);
        ASSERT_EQ(migrator.do_migrate(), 0);
        enable_instance_multi_version_read_write(meta_service.get(), instance_id);

        // Check the snapshot properties
        SnapshotProperty property;
        get_instance_snapshot_properties(meta_service.get(), instance_id, &property);
        ASSERT_EQ(property.status, SnapshotSwitchStatus::SNAPSHOT_SWITCH_OFF);
    }

    TabletStatsPB new_tablet_stats;
    get_tablet_stats(meta_service.get(), cloud_unique_id, old_tablet_id, new_tablet_stats);
    tablet_stats_must_equals(new_tablet_stats, old_tablet_stats);

    // Phase 1: Single version mode - schema change from old_tablet to new_tablet_1
    {
        std::vector<doris::RowsetMetaCloudPB> output_rowsets;
        std::string job_id = fmt::format("schema_change_{}_{}", old_tablet_id, new_tablet_id_1);
        int64_t alter_version = 6;

        // Create new tablet
        create_tablet(meta_service.get(), cloud_unique_id, db_id, table_id, index_id, partition_id,
                      new_tablet_id_1, TabletStatePB::PB_NOTREADY);

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
        {
            int64_t version = 6;
            int64_t txn_id = 100000 + version;
            auto output_rowset = create_rowset(txn_id, new_tablet_id_1, partition_id, version, 100);
            output_rowset.set_start_version(2);
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
        ASSERT_EQ(rowsets.size(), 2); // [0-1], [2-6]
    }

    // Final check: Get all rowsets and verify versions on new_tablet_1
    {
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id, new_tablet_id_1, 0, 11, rowsets);
        ASSERT_EQ(rowsets.size(), 2); // [0-1], [2-6]

        // Verify version continuity
        ASSERT_EQ(rowsets[0].start_version(), 0);
        ASSERT_EQ(rowsets[0].end_version(), 1);
        ASSERT_EQ(rowsets[1].start_version(), 2);
        ASSERT_EQ(rowsets[1].end_version(), 6);
    }

    int64_t partition_version = -1, table_version = -1;
    get_partition_version(meta_service.get(), cloud_unique_id, db_id, table_id, partition_id,
                          &partition_version);
    get_table_version(meta_service.get(), cloud_unique_id, db_id, table_id, &table_version);
    ASSERT_EQ(partition_version, 6);
    ASSERT_GE(table_version, 6);
    TabletMetaCloudPB tablet_meta;
    get_tablet_meta(meta_service.get(), cloud_unique_id, new_tablet_id_1, &tablet_meta);
    ASSERT_EQ(tablet_meta.tablet_id(), new_tablet_id_1);

    // Switch back to multi-version disabled, test the tablet stats is compatible.
    get_tablet_stats(meta_service.get(), cloud_unique_id, new_tablet_id_1, new_tablet_stats);
    enable_instance_multi_version_disabled(meta_service.get(), instance_id);
    get_tablet_stats(meta_service.get(), cloud_unique_id, new_tablet_id_1, old_tablet_stats);
    tablet_stats_must_equals(new_tablet_stats, old_tablet_stats);
}

TEST(MigrateSnapshotKeysTest, DeleteBitmap) {
    auto meta_service = get_meta_service();
    auto txn_kv = meta_service->txn_kv();
    std::string instance_id = "migrate_snapshot_keys_delete_bitmap_test_instance";
    std::string cloud_unique_id = fmt::format("1:{}:0", instance_id);
    create_and_refresh_instance(meta_service.get(), instance_id);
    int64_t db_id = 1, table_id = 2, index_id = 3, partition_id = 4, tablet_id = 5;

    // create partition/index/tablet
    prepare_and_commit_index(meta_service.get(), cloud_unique_id, db_id, table_id, index_id);
    prepare_and_commit_partition(meta_service.get(), cloud_unique_id, db_id, table_id, partition_id,
                                 index_id);
    create_tablet(meta_service.get(), cloud_unique_id, db_id, table_id, index_id, partition_id,
                  tablet_id, TabletStatePB::PB_RUNNING, true);

    // Phase 1: Single version mode - insert 5 rowsets (version 2-6)
    for (int i = 0; i < 5; i++) {
        insert_rowset(meta_service.get(), cloud_unique_id, db_id, fmt::format("label_{}", i),
                      table_id, partition_id, tablet_id);
    }

    // update delete bitmap v1
    size_t large_dbm_size = 300 * 1000 * 3;
    {
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id, tablet_id, 0, 6, rowsets);

        int64_t lock_id = -1;
        int64_t initiator = 1234;
        get_delete_bitmap_lock(meta_service.get(), cloud_unique_id, table_id, partition_id, lock_id,
                               initiator);

        std::string large_value = generate_random_string(large_dbm_size);
        for (size_t i = 0; i < rowsets.size(); i++) {
            auto& rowset = rowsets[i];
            DeleteBitmapPB delete_bitmap_pb;
            for (size_t j = 1; j < i; j++) {
                delete_bitmap_pb.add_rowset_ids(rowsets[j].rowset_id_v2());
                delete_bitmap_pb.add_segment_ids(0);
                delete_bitmap_pb.add_versions(rowset.end_version());
                delete_bitmap_pb.add_segment_delete_bitmaps(
                        rowset.end_version() % 2 == 0 ? large_value : "bitmap_data");
            }
            update_delete_bitmap(meta_service.get(), cloud_unique_id, table_id, partition_id,
                                 tablet_id, lock_id, initiator, rowset.rowset_id_v2(),
                                 delete_bitmap_pb, 1);
        }

        remove_delete_bitmap_lock(meta_service.get(), cloud_unique_id, table_id, lock_id,
                                  initiator);

        for (size_t i = 1; i < rowsets.size(); i++) {
            auto& rowset = rowsets[i];
            {
                DeleteBitmapPB delete_bitmap_pb;
                get_delete_bitmap(meta_service.get(), cloud_unique_id, tablet_id,
                                  rowset.rowset_id_v2(), 1, delete_bitmap_pb);
                ASSERT_EQ(delete_bitmap_pb.rowset_ids_size(),
                          rowsets.size() - rowset.end_version());
            }
            {
                DeleteBitmapPB delete_bitmap_pb;
                get_delete_bitmap(meta_service.get(), cloud_unique_id, tablet_id,
                                  rowset.rowset_id_v2(), 2, delete_bitmap_pb);
                ASSERT_EQ(delete_bitmap_pb.rowset_ids_size(), 0);
            }
        }
    }

    // Phase 2: Switch to multi-version write only
    enable_instance_multi_version_write_only(meta_service.get(), instance_id);

    // Phase 2: Dual write mode - insert 3 more rowsets (version 7-9)
    for (int i = 5; i < 8; i++) {
        insert_rowset(meta_service.get(), cloud_unique_id, db_id, fmt::format("label_{}", i),
                      table_id, partition_id, tablet_id);
    }

    // update delete bitmap v2
    {
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id, tablet_id, 0, 9, rowsets);

        int64_t lock_id = -1;
        int64_t initiator = 1234;
        get_delete_bitmap_lock(meta_service.get(), cloud_unique_id, table_id, partition_id, lock_id,
                               initiator);

        for (size_t i = 6; i < rowsets.size(); i++) {
            auto& rowset = rowsets[i];
            DeleteBitmapPB delete_bitmap_pb;
            for (size_t j = 1; j < i; j++) {
                delete_bitmap_pb.add_rowset_ids(rowsets[j].rowset_id_v2());
                delete_bitmap_pb.add_segment_ids(0);
                delete_bitmap_pb.add_versions(rowset.end_version());
                delete_bitmap_pb.add_segment_delete_bitmaps("bitmap_data");
            }
            update_delete_bitmap(meta_service.get(), cloud_unique_id, table_id, partition_id,
                                 tablet_id, lock_id, initiator, rowset.rowset_id_v2(),
                                 delete_bitmap_pb, 2);
        }

        remove_delete_bitmap_lock(meta_service.get(), cloud_unique_id, table_id, lock_id,
                                  initiator);

        for (size_t i = 1; i < rowsets.size(); i++) {
            auto& rowset = rowsets[i];
            {
                DeleteBitmapPB delete_bitmap_pb;
                get_delete_bitmap(meta_service.get(), cloud_unique_id, tablet_id,
                                  rowset.rowset_id_v2(), 1, delete_bitmap_pb);
                if (i < 6) {
                    ASSERT_EQ(delete_bitmap_pb.rowset_ids_size(), 6 - rowset.end_version());
                } else {
                    ASSERT_EQ(delete_bitmap_pb.rowset_ids_size(), 0);
                }
            }
            {
                DeleteBitmapPB delete_bitmap_pb;
                get_delete_bitmap(meta_service.get(), cloud_unique_id, tablet_id,
                                  rowset.rowset_id_v2(), 2, delete_bitmap_pb);
                if (i < 6) {
                    ASSERT_EQ(delete_bitmap_pb.rowset_ids_size(), 0);
                } else {
                    ASSERT_EQ(delete_bitmap_pb.rowset_ids_size(), rowset.start_version() - 2);
                }
            }
        }
    }

    // Phase 3: Migrate old keys and switch to multi-version read write
    {
        InstanceInfoPB instance_info;
        get_instance(meta_service.get(), cloud_unique_id, instance_info);
        InstanceDataMigrator migrator(txn_kv, instance_info);
        ASSERT_EQ(migrator.do_migrate(), 0);
        enable_instance_multi_version_read_write(meta_service.get(), instance_id);

        // Check the snapshot properties
        SnapshotProperty property;
        get_instance_snapshot_properties(meta_service.get(), instance_id, &property);
        ASSERT_EQ(property.status, SnapshotSwitchStatus::SNAPSHOT_SWITCH_OFF);
    }

    // check delete bitmap
    {
        std::vector<doris::RowsetMetaCloudPB> rowsets;
        get_rowsets(meta_service.get(), cloud_unique_id, tablet_id, 0, 9, rowsets);
        // <rowset_id, <dbm_version, dbm_size>>
        std::map<std::string, std::vector<std::pair<int64_t, int64_t>>> rowset_map;

        for (size_t i = 1; i < rowsets.size(); i++) {
            auto& rowset = rowsets[i];
            DeleteBitmapPB delete_bitmap_pb;
            get_delete_bitmap(meta_service.get(), cloud_unique_id, tablet_id, rowset.rowset_id_v2(),
                              2, delete_bitmap_pb);
            ASSERT_EQ(delete_bitmap_pb.rowset_ids_size(), delete_bitmap_pb.versions_size());
            ASSERT_EQ(delete_bitmap_pb.rowset_ids_size(),
                      delete_bitmap_pb.segment_delete_bitmaps_size());
            ASSERT_EQ(delete_bitmap_pb.rowset_ids_size(), delete_bitmap_pb.segment_ids_size());
            if (i < 6) {
                ASSERT_EQ(delete_bitmap_pb.rowset_ids_size(), 6 - rowset.start_version());
            } else {
                ASSERT_EQ(delete_bitmap_pb.rowset_ids_size(), rowset.start_version() - 2);
            }

            for (size_t j = 0; j < delete_bitmap_pb.rowset_ids_size(); j++) {
                auto& rowset_id = delete_bitmap_pb.rowset_ids(j);
                if (rowset_map.find(rowset_id) == rowset_map.end()) {
                    rowset_map[rowset_id] = std::vector<std::pair<int64_t, int64_t>>();
                }
                rowset_map[rowset_id].emplace_back(
                        std::make_pair(delete_bitmap_pb.versions(j),
                                       delete_bitmap_pb.segment_delete_bitmaps(j).size()));
            }
        }

        for (size_t i = 1; i < rowsets.size() - 1; i++) {
            auto& rowset = rowsets[i];
            auto iter = rowset_map.find(rowset.rowset_id_v2());
            ASSERT_TRUE(iter != rowset_map.end());
            auto& vec = iter->second;
            ASSERT_EQ(vec.size(), 9 - rowset.start_version());
            for (size_t j = 0; j < vec.size(); j++) {
                auto version = vec[j].first;
                auto delete_bitmap_size = vec[j].second;
                if (rowset.end_version() <= 6 && version <= 6 && version % 2 == 0) {
                    ASSERT_EQ(delete_bitmap_size, large_dbm_size);
                } else {
                    ASSERT_EQ(delete_bitmap_size, std::string("bitmap_data").size());
                }
            }
        }
    }
}