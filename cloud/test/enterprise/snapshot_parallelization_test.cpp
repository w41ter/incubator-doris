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

#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>
#include <strings.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/simple_thread_pool.h"
#include "common/util.h"
#include "enterprise/snapshot/snapshot_helper.h"
#include "enterprise/snapshot/snapshot_manager.h"
#include "meta-store/document_message.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/meta_reader.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
#include "meta-store/versionstamp.h"
#include "recycler/snapshot_chain_compactor.h"
#include "recycler/snapshot_data_migrator.h"

using namespace doris;
using namespace doris::cloud;
using namespace std::chrono_literals;

static constexpr int64_t kDbId = 1;
static constexpr int64_t kTableId = 101;
static constexpr int64_t kPartitionId = 201;
static constexpr int64_t kIndexId = 301;
static constexpr int64_t kTabletId = 401;
static constexpr int64_t kSchemaVersion = 1;
static constexpr int64_t kStressObjectCount = 100;
static const int kStressSchemaVersion = 1;
static const std::vector<int> kStressConcurrencies = {1, 2, 3, 4, 5, 10, 20, 50};
static constexpr int kSeedBatchSize = 1000;

enum class KvBackend { MEMKV, FDBKV };

KvBackend stress_backend() {
    const char* backend = std::getenv("SNAPSHOT_KV_BACKEND");
    if (backend != nullptr && strcasecmp(backend, "fdb") == 0) {
        return KvBackend::FDBKV;
    }
    return KvBackend::MEMKV;
}

std::string fdb_cluster_file() {
    const char* env_path = std::getenv("FDB_CLUSTER_FILE");
    if (env_path != nullptr && env_path[0] != '\0') {
        return env_path;
    }
    // Default FDB cluster file path provided by the test environment.
    return "";
}

std::string_view backend_name(KvBackend backend) {
    return backend == KvBackend::FDBKV ? "fdbkv" : "memkv";
}

std::shared_ptr<TxnKv> make_kv(KvBackend backend) {
    std::shared_ptr<TxnKv> kv;
    if (backend == KvBackend::MEMKV) {
        kv = std::make_shared<MemTxnKv>();
    } else {
        cloud::config::fdb_cluster_file_path = fdb_cluster_file();
        static std::shared_ptr<TxnKv> cached_fdb; // keep alive to avoid multiple API selections
        if (!cached_fdb) {
            cached_fdb = std::make_shared<FdbTxnKv>();
            if (cached_fdb->init() != 0) {
                return nullptr;
            }
        }
        return cached_fdb;
    }
    if (kv->init() != 0) {
        return nullptr;
    }
    return kv;
}

std::string make_instance_id(std::string_view prefix, int concurrency) {
    static std::atomic<uint64_t> counter {0};
    return std::string(prefix) + "_c" + std::to_string(concurrency) + "_" +
           std::to_string(counter.fetch_add(1));
}

void seed_single_version_dataset(std::shared_ptr<TxnKv> txn_kv, const std::string& instance_id,
                                 int64_t count) {
    int batch = 0;
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

    for (int64_t i = 0; i < count; ++i) {
        int64_t db_id = 1;
        int64_t table_id = i + 1;
        int64_t partition_id = i + 1;
        int64_t index_id = i + 1;
        int64_t tablet_id = i + 1;

        // table version
        txn->put(table_version_key({instance_id, db_id, table_id}), "");

        // partition version
        VersionPB partition_version;
        partition_version.set_version(1);
        txn->put(partition_version_key({instance_id, db_id, table_id, partition_id}),
                 partition_version.SerializeAsString());

        // tablet index
        doris::cloud::TabletIndexPB tablet_index_pb;
        tablet_index_pb.set_db_id(db_id);
        tablet_index_pb.set_table_id(table_id);
        tablet_index_pb.set_index_id(index_id);
        tablet_index_pb.set_partition_id(partition_id);
        txn->put(meta_tablet_idx_key({instance_id, tablet_id}),
                 tablet_index_pb.SerializeAsString());

        // tablet schema
        TabletSchemaCloudPB schema;
        schema.set_schema_version(kStressSchemaVersion);
        document_put(txn.get(), meta_schema_key({instance_id, index_id, kStressSchemaVersion}),
                     std::move(schema));

        // tablet meta
        TabletMetaCloudPB tablet_meta;
        tablet_meta.set_tablet_id(tablet_id);
        tablet_meta.set_table_id(table_id);
        tablet_meta.set_partition_id(partition_id);
        tablet_meta.set_index_id(index_id);
        tablet_meta.set_schema_version(kStressSchemaVersion);
        document_put(txn.get(),
                     meta_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id}),
                     std::move(tablet_meta));

        // tablet stats
        TabletStatsPB stats;
        stats.set_num_rows(1);
        stats.set_num_rowsets(1);
        txn->put(stats_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id}),
                 stats.SerializeAsString());

        // rowset meta
        RowsetMetaCloudPB rowset_meta;
        rowset_meta.set_rowset_id(i);
        rowset_meta.set_rowset_id_v2("rs_" + std::to_string(i));
        rowset_meta.set_tablet_id(tablet_id);
        rowset_meta.set_start_version(1);
        rowset_meta.set_end_version(1);
        rowset_meta.set_num_rows(1);
        ASSERT_TRUE(document_put(
                txn.get(), meta_rowset_key({instance_id, tablet_id, rowset_meta.end_version()}),
                std::move(rowset_meta)));

        if (++batch >= kSeedBatchSize) {
            ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
            ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
            batch = 0;
        }
    }

    if (txn != nullptr) {
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }
}

void seed_versioned_dataset(std::shared_ptr<TxnKv> txn_kv, const std::string& source_instance,
                            int64_t count, Versionstamp data_version) {
    int batch = 0;
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

    for (int64_t i = 0; i < count; ++i) {
        int64_t db_id = 1;
        int64_t table_id = i + 1;
        int64_t partition_id = i + 1;
        int64_t index_id = i + 1;
        int64_t tablet_id = i + 1;

        // table version
        versioned_put(txn.get(), versioned::table_version_key({source_instance, table_id}),
                      data_version, "");

        // partition meta/index/version
        PartitionIndexPB partition_index;
        partition_index.set_db_id(db_id);
        partition_index.set_table_id(table_id);
        versioned_put(txn.get(), versioned::meta_partition_key({source_instance, partition_id}),
                      data_version, partition_index.SerializeAsString());
        txn->put(versioned::partition_index_key({source_instance, partition_id}),
                 partition_index.SerializeAsString());

        VersionPB partition_version;
        partition_version.set_version(1);
        versioned_put(txn.get(), versioned::partition_version_key({source_instance, partition_id}),
                      data_version, partition_version.SerializeAsString());

        // index meta/index/schema
        IndexIndexPB index_index;
        index_index.set_db_id(db_id);
        index_index.set_table_id(table_id);
        versioned_put(txn.get(), versioned::meta_index_key({source_instance, index_id}),
                      data_version, "index_meta_payload");
        TabletSchemaCloudPB schema;
        schema.set_schema_version(kStressSchemaVersion);
        ASSERT_TRUE(versioned::document_put(
                txn.get(),
                versioned::meta_schema_key({source_instance, index_id, kStressSchemaVersion}),
                data_version, std::move(schema)));
        TabletSchemaCloudPB schema_non_versioned;
        schema_non_versioned.set_schema_version(kStressSchemaVersion);
        ASSERT_TRUE(document_put(
                txn.get(),
                versioned::meta_schema_key({source_instance, index_id, kStressSchemaVersion}),
                std::move(schema_non_versioned)));
        txn->put(versioned::index_index_key({source_instance, index_id}),
                 index_index.SerializeAsString());

        // tablet meta/index/stats
        TabletMetaCloudPB tablet_meta;
        tablet_meta.set_tablet_id(tablet_id);
        tablet_meta.set_table_id(table_id);
        tablet_meta.set_partition_id(partition_id);
        tablet_meta.set_index_id(index_id);
        tablet_meta.set_schema_version(kStressSchemaVersion);
        versioned::document_put(txn.get(), versioned::meta_tablet_key({source_instance, tablet_id}),
                                data_version, std::move(tablet_meta));

        doris::cloud::TabletIndexPB tablet_index_pb;
        tablet_index_pb.set_db_id(db_id);
        tablet_index_pb.set_table_id(table_id);
        tablet_index_pb.set_index_id(index_id);
        tablet_index_pb.set_partition_id(partition_id);
        txn->put(versioned::tablet_index_key({source_instance, tablet_id}),
                 tablet_index_pb.SerializeAsString());

        TabletStatsPB stats;
        stats.set_num_rows(1);
        stats.set_num_rowsets(1);
        versioned::document_put(txn.get(),
                                versioned::tablet_load_stats_key({source_instance, tablet_id}),
                                data_version, std::move(stats));
        TabletStatsPB stats_compact;
        stats_compact.set_num_rows(1);
        stats_compact.set_num_rowsets(1);
        versioned::document_put(txn.get(),
                                versioned::tablet_compact_stats_key({source_instance, tablet_id}),
                                data_version, std::move(stats_compact));

        if (++batch >= kSeedBatchSize) {
            ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
            ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
            batch = 0;
        }
    }

    if (txn != nullptr) {
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }
}

void seed_versioned_schemas_only(std::shared_ptr<TxnKv> txn_kv, const std::string& instance_id,
                                 int64_t count) {
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    int batch = 0;
    for (int64_t i = 0; i < count; ++i) {
        int64_t index_id = i + 1;
        TabletSchemaCloudPB schema;
        schema.set_schema_version(kStressSchemaVersion);
        ASSERT_TRUE(versioned::document_put(
                txn.get(),
                versioned::meta_schema_key({instance_id, index_id, kStressSchemaVersion}),
                Versionstamp::max(), std::move(schema)));
        TabletSchemaCloudPB schema_non_versioned;
        schema_non_versioned.set_schema_version(kStressSchemaVersion);
        ASSERT_TRUE(document_put(
                txn.get(),
                versioned::meta_schema_key({instance_id, index_id, kStressSchemaVersion}),
                std::move(schema_non_versioned)));
        if (++batch >= kSeedBatchSize) {
            ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
            ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
            batch = 0;
        }
    }
    if (txn != nullptr) {
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }
}

void seed_versioned_tablet_meta_only(std::shared_ptr<TxnKv> txn_kv, const std::string& instance_id,
                                     int64_t count) {
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    int batch = 0;
    for (int64_t i = 0; i < count; ++i) {
        int64_t table_id = i + 1;
        int64_t partition_id = i + 1;
        int64_t index_id = i + 1;
        int64_t tablet_id = i + 1;

        TabletMetaCloudPB tablet_meta;
        tablet_meta.set_tablet_id(tablet_id);
        tablet_meta.set_table_id(table_id);
        tablet_meta.set_partition_id(partition_id);
        tablet_meta.set_index_id(index_id);
        tablet_meta.set_schema_version(kStressSchemaVersion);
        ASSERT_TRUE(versioned::document_put(txn.get(),
                                            versioned::meta_tablet_key({instance_id, tablet_id}),
                                            Versionstamp::min(), std::move(tablet_meta)));
        if (++batch >= kSeedBatchSize) {
            ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
            ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
            batch = 0;
        }
    }
    if (txn != nullptr) {
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }
}

void persist_instance_info(std::shared_ptr<TxnKv> txn_kv, const InstanceInfoPB& info) {
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string value;
    ASSERT_TRUE(info.SerializeToString(&value));
    txn->put(instance_key(info.instance_id()), value);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
}

namespace {

// Simple fixture that seeds both single-version and versioned key spaces so the
// real CompactExecutor and MigrateExecutor implementations run end-to-end.
class SnapshotParallelizationTest : public ::testing::Test {
protected:
    void SetUp() override {
        config::enable_multi_version_status = false;
        config::enable_split_tablet_schema_pb = true;
        config::enable_split_rowset_meta_pb = true;
        // Keep parallelism small and deterministic for the unit test while still exercising
        // concurrent workers. The fixture seeds exactly 1 db/table/partition/index/tablet worth of
        // metadata, so each thread has real work but no cross-key interference.
        config::snapshot_compact_parallelism = 4;
        config::snapshot_migrate_parallelism = 4;

        txn_kv = std::make_shared<MemTxnKv>();
        ASSERT_EQ(txn_kv->init(), 0);

        data_version = Versionstamp(100);
        snapshot_version = Versionstamp(200);
        snapshot_id = selectdb::serialize_snapshot_versionstamp(snapshot_version);

        persist_instance_info(make_migrate_instance());
        persist_instance_info(make_compact_instance());
        seed_versioned_source();
        seed_target_schema();
        seed_single_version_source();

        std::unique_ptr<Transaction> verify_txn;
        ASSERT_EQ(txn_kv->create_txn(&verify_txn), TxnErrorCode::TXN_OK);
        MetaReader versioned_reader(source_instance, snapshot_version);
        TabletSchemaCloudPB verify_schema;
        ASSERT_EQ(versioned_reader.get_tablet_schema(verify_txn.get(), index_id, kSchemaVersion,
                                                     &verify_schema),
                  TxnErrorCode::TXN_OK);
        std::string schema_value;
        ASSERT_EQ(verify_txn->get(
                          versioned::meta_schema_key({source_instance, index_id, kSchemaVersion}),
                          &schema_value),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(verify_txn->get(
                          versioned::meta_schema_key({target_instance, index_id, kSchemaVersion}),
                          &schema_value),
                  TxnErrorCode::TXN_OK);
    }

    InstanceInfoPB make_compact_instance() const {
        InstanceInfoPB info;
        info.set_instance_id(target_instance);
        info.set_source_instance_id(source_instance);
        info.set_source_snapshot_id(snapshot_id);
        info.set_multi_version_status(MultiVersionStatus::MULTI_VERSION_READ_WRITE);
        info.set_snapshot_switch_status(SnapshotSwitchStatus::SNAPSHOT_SWITCH_ON);
        return info;
    }

    InstanceInfoPB make_migrate_instance() const {
        InstanceInfoPB info;
        info.set_instance_id(source_instance);
        info.set_multi_version_status(MultiVersionStatus::MULTI_VERSION_DISABLED);
        // META_ROWSET is not part of the 6 functions we want to exercise here.
        info.add_migrated_key_sets(KeySetType::SINGLE_VERSION_META_ROWSET);
        info.set_snapshot_switch_status(SnapshotSwitchStatus::SNAPSHOT_SWITCH_ON);
        return info;
    }

    void persist_instance_info(const InstanceInfoPB& info) {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(instance_key(info.instance_id()), info.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    void seed_versioned_source() {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        // Single logical table (1 db/table/partition/index/tablet) at data_version=100 that will be
        // compacted to target_instance.
        versioned_put(txn.get(), versioned::table_version_key({source_instance, table_id}),
                      data_version, "");

        // partition meta/index/version
        PartitionIndexPB partition_index;
        partition_index.set_db_id(db_id);
        partition_index.set_table_id(table_id);
        versioned_put(txn.get(), versioned::meta_partition_key({source_instance, partition_id}),
                      data_version, partition_index.SerializeAsString());
        txn->put(versioned::partition_index_key({source_instance, partition_id}),
                 partition_index.SerializeAsString());

        VersionPB partition_version;
        partition_version.set_version(1);
        versioned_put(txn.get(), versioned::partition_version_key({source_instance, partition_id}),
                      data_version, partition_version.SerializeAsString());

        // index meta/index/schema
        IndexIndexPB index_index;
        index_index.set_db_id(db_id);
        index_index.set_table_id(table_id);
        versioned_put(txn.get(), versioned::meta_index_key({source_instance, index_id}),
                      data_version, "index_meta_payload");
        TabletSchemaCloudPB schema;
        schema.set_schema_version(kSchemaVersion);
        ASSERT_TRUE(document_put(
                txn.get(),
                versioned::meta_schema_key({source_instance, index_id, schema.schema_version()}),
                std::move(schema)));
        txn->put(versioned::index_index_key({source_instance, index_id}),
                 index_index.SerializeAsString());

        // tablet meta/index/stats
        TabletMetaCloudPB tablet_meta;
        tablet_meta.set_tablet_id(tablet_id);
        tablet_meta.set_table_id(table_id);
        tablet_meta.set_partition_id(partition_id);
        tablet_meta.set_index_id(index_id);
        tablet_meta.set_schema_version(kSchemaVersion);
        versioned::document_put(txn.get(), versioned::meta_tablet_key({source_instance, tablet_id}),
                                data_version, std::move(tablet_meta));

        doris::cloud::TabletIndexPB tablet_index;
        tablet_index.set_db_id(db_id);
        tablet_index.set_table_id(table_id);
        tablet_index.set_index_id(index_id);
        tablet_index.set_partition_id(partition_id);
        txn->put(versioned::tablet_index_key({source_instance, tablet_id}),
                 tablet_index.SerializeAsString());

        TabletStatsPB stats;
        stats.set_num_rows(10);
        stats.set_num_rowsets(1);
        versioned::document_put(txn.get(),
                                versioned::tablet_load_stats_key({source_instance, tablet_id}),
                                data_version, std::move(stats));
        TabletStatsPB stats_compact;
        stats_compact.set_num_rows(10);
        stats_compact.set_num_rowsets(1);
        versioned::document_put(txn.get(),
                                versioned::tablet_compact_stats_key({source_instance, tablet_id}),
                                data_version, std::move(stats_compact));

        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    void seed_target_schema() {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        TabletSchemaCloudPB schema;
        schema.set_schema_version(kSchemaVersion);
        ASSERT_TRUE(document_put(
                txn.get(),
                versioned::meta_schema_key({target_instance, index_id, schema.schema_version()}),
                std::move(schema)));

        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    void seed_target_versioned() {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        versioned_put(txn.get(), versioned::table_version_key({target_instance, table_id}),
                      data_version, "");

        PartitionIndexPB partition_index;
        partition_index.set_db_id(db_id);
        partition_index.set_table_id(table_id);
        versioned_put(txn.get(), versioned::meta_partition_key({target_instance, partition_id}),
                      data_version, partition_index.SerializeAsString());
        txn->put(versioned::partition_index_key({target_instance, partition_id}),
                 partition_index.SerializeAsString());

        VersionPB partition_version;
        partition_version.set_version(1);
        versioned_put(txn.get(), versioned::partition_version_key({target_instance, partition_id}),
                      data_version, partition_version.SerializeAsString());

        IndexIndexPB index_index;
        index_index.set_db_id(db_id);
        index_index.set_table_id(table_id);
        versioned_put(txn.get(), versioned::meta_index_key({target_instance, index_id}),
                      data_version, "index_meta_payload");
        TabletSchemaCloudPB schema;
        schema.set_schema_version(kSchemaVersion);
        ASSERT_TRUE(document_put(
                txn.get(),
                versioned::meta_schema_key({target_instance, index_id, schema.schema_version()}),
                std::move(schema)));
        txn->put(versioned::index_index_key({target_instance, index_id}),
                 index_index.SerializeAsString());

        TabletMetaCloudPB tablet_meta;
        tablet_meta.set_tablet_id(tablet_id);
        tablet_meta.set_table_id(table_id);
        tablet_meta.set_partition_id(partition_id);
        tablet_meta.set_index_id(index_id);
        tablet_meta.set_schema_version(kSchemaVersion);
        versioned::document_put(txn.get(), versioned::meta_tablet_key({target_instance, tablet_id}),
                                data_version, std::move(tablet_meta));

        doris::cloud::TabletIndexPB tablet_index;
        tablet_index.set_db_id(db_id);
        tablet_index.set_table_id(table_id);
        tablet_index.set_index_id(index_id);
        tablet_index.set_partition_id(partition_id);
        txn->put(versioned::tablet_index_key({target_instance, tablet_id}),
                 tablet_index.SerializeAsString());

        TabletStatsPB stats;
        stats.set_num_rows(10);
        stats.set_num_rowsets(1);
        versioned::document_put(txn.get(),
                                versioned::tablet_load_stats_key({target_instance, tablet_id}),
                                data_version, std::move(stats));
        TabletStatsPB stats_compact;
        stats_compact.set_num_rows(10);
        stats_compact.set_num_rowsets(1);
        versioned::document_put(txn.get(),
                                versioned::tablet_compact_stats_key({target_instance, tablet_id}),
                                data_version, std::move(stats_compact));

        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    void seed_single_version_source() {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        // Same single-tablet payload in single-version space to migrate.
        txn->put(table_version_key({source_instance, db_id, table_id}), "");

        VersionPB partition_version;
        partition_version.set_version(1);
        txn->put(partition_version_key({source_instance, db_id, table_id, partition_id}),
                 partition_version.SerializeAsString());

        // tablet schema (non-versioned)
        TabletSchemaCloudPB schema;
        schema.set_schema_version(kSchemaVersion);
        document_put(txn.get(), meta_schema_key({source_instance, index_id, kSchemaVersion}),
                     std::move(schema));

        // tablet meta + index
        TabletMetaCloudPB tablet_meta;
        tablet_meta.set_tablet_id(tablet_id);
        tablet_meta.set_table_id(table_id);
        tablet_meta.set_partition_id(partition_id);
        tablet_meta.set_index_id(index_id);
        tablet_meta.set_schema_version(kSchemaVersion);
        document_put(
                txn.get(),
                meta_tablet_key({source_instance, table_id, index_id, partition_id, tablet_id}),
                std::move(tablet_meta));

        doris::cloud::TabletIndexPB tablet_index;
        tablet_index.set_db_id(db_id);
        tablet_index.set_table_id(table_id);
        tablet_index.set_index_id(index_id);
        tablet_index.set_partition_id(partition_id);
        txn->put(meta_tablet_idx_key({source_instance, tablet_id}),
                 tablet_index.SerializeAsString());

        // tablet stats
        TabletStatsPB stats;
        stats.set_num_rows(10);
        stats.set_num_rowsets(1);
        txn->put(stats_tablet_key({source_instance, table_id, index_id, partition_id, tablet_id}),
                 stats.SerializeAsString());

        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    std::shared_ptr<TxnKv> txn_kv;
    Versionstamp data_version;
    Versionstamp snapshot_version;
    std::string snapshot_id;

    const std::string source_instance = "source_instance";
    const std::string target_instance = "target_instance";
    const int64_t db_id = kDbId;
    const int64_t table_id = kTableId;
    const int64_t partition_id = kPartitionId;
    const int64_t index_id = kIndexId;
    const int64_t tablet_id = kTabletId;
};

void validate_compaction_output(Transaction* txn, const std::string& target_instance,
                                int64_t table_id, int64_t partition_id, int64_t index_id,
                                int64_t tablet_id) {
    // table version
    Versionstamp vs;
    ASSERT_EQ(versioned_get(txn, versioned::table_version_key({target_instance, table_id}), &vs,
                            nullptr),
              TxnErrorCode::TXN_OK);
    // partition version
    std::string value;
    ASSERT_EQ(versioned_get(txn, versioned::partition_version_key({target_instance, partition_id}),
                            &vs, &value),
              TxnErrorCode::TXN_OK);
    VersionPB partition_version;
    ASSERT_TRUE(partition_version.ParseFromString(value));
    ASSERT_EQ(partition_version.version(), 1);

    // index meta/schema
    ASSERT_EQ(
            versioned_get(txn, versioned::meta_index_key({target_instance, index_id}), &vs, &value),
            TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(versioned::index_index_key({target_instance, index_id}), &value),
              TxnErrorCode::TXN_OK);

    // tablet meta/index/stats
    TabletMetaCloudPB tablet_meta;
    Versionstamp tablet_meta_vs;
    ASSERT_EQ(versioned::document_get(txn, versioned::meta_tablet_key({target_instance, tablet_id}),
                                      &tablet_meta, &tablet_meta_vs),
              TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(versioned::tablet_index_key({target_instance, tablet_id}), &value),
              TxnErrorCode::TXN_OK);
    TabletStatsPB stats;
    Versionstamp stats_vs;
    ASSERT_EQ(versioned::document_get(
                      txn, versioned::tablet_load_stats_key({target_instance, tablet_id}), &stats,
                      &stats_vs),
              TxnErrorCode::TXN_OK);
    ASSERT_EQ(versioned::document_get(
                      txn, versioned::tablet_compact_stats_key({target_instance, tablet_id}),
                      &stats, &stats_vs),
              TxnErrorCode::TXN_OK);
}

void validate_migration_output(Transaction* txn, const std::string& instance_id, int64_t table_id,
                               int64_t partition_id, int64_t index_id, int64_t tablet_id) {
    Versionstamp vs;
    std::string value;
    ASSERT_EQ(
            versioned_get(txn, versioned::table_version_key({instance_id, table_id}), &vs, &value),
            TxnErrorCode::TXN_OK);

    ASSERT_EQ(versioned_get(txn, versioned::partition_version_key({instance_id, partition_id}), &vs,
                            &value),
              TxnErrorCode::TXN_OK);

    TabletSchemaCloudPB schema;
    ASSERT_EQ(document_get(txn, versioned::meta_schema_key({instance_id, index_id, kSchemaVersion}),
                           &schema),
              TxnErrorCode::TXN_OK);

    TabletMetaCloudPB tablet_meta;
    Versionstamp tablet_meta_vs;
    ASSERT_EQ(versioned::document_get(txn, versioned::meta_tablet_key({instance_id, tablet_id}),
                                      &tablet_meta, &tablet_meta_vs),
              TxnErrorCode::TXN_OK);

    ASSERT_EQ(txn->get(versioned::tablet_index_key({instance_id, tablet_id}), &value),
              TxnErrorCode::TXN_OK);

    TabletStatsPB stats;
    Versionstamp stats_vs;
    ASSERT_EQ(versioned::document_get(txn,
                                      versioned::tablet_compact_stats_key({instance_id, tablet_id}),
                                      &stats, &stats_vs),
              TxnErrorCode::TXN_OK);
}

} // namespace

TEST_F(SnapshotParallelizationTest, CompactExecutorCorrectnessAndPerformance) {
    selectdb::SnapshotManager manager(txn_kv);

    InstanceInfoPB instance_info = make_compact_instance();
    InstanceChainCompactor compactor(txn_kv, instance_info);

    // Measure end-to-end latency with the configured parallelism. The data set is tiny (one index,
    // one tablet) but still exercises all compact_* functions.
    auto start = std::chrono::steady_clock::now();
    ASSERT_EQ(manager.compact_snapshot_chains(&compactor), 0);
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start);
    ASSERT_GE(duration_ms.count(), 0);

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    validate_compaction_output(txn.get(), target_instance, table_id, partition_id, index_id,
                               tablet_id);
}

TEST_F(SnapshotParallelizationTest, MigrateExecutorCorrectnessAndPerformance) {
    selectdb::SnapshotManager manager(txn_kv);

    InstanceInfoPB instance_info = make_migrate_instance();
    InstanceDataMigrator migrator(txn_kv, instance_info);

    // Measure migrate latency; same single-tablet payload ensures every migrate_* function runs.
    auto start = std::chrono::steady_clock::now();
    ASSERT_EQ(manager.migrate_to_versioned_keys(&migrator), 0);
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start);
    ASSERT_GE(duration_ms.count(), 0);

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    validate_migration_output(txn.get(), source_instance, table_id, partition_id, index_id,
                              tablet_id);
}

class SnapshotParallelizationStressTest : public ::testing::Test {
protected:
    void SetUp() override { backend = stress_backend(); }

    KvBackend backend;
};

InstanceInfoPB make_stress_migrate_instance(const std::string& instance_id, KeySetType target) {
    InstanceInfoPB info;
    info.set_instance_id(instance_id);
    info.set_multi_version_status(MultiVersionStatus::MULTI_VERSION_DISABLED);
    info.set_snapshot_switch_status(SnapshotSwitchStatus::SNAPSHOT_SWITCH_ON);

    const KeySetType migrate_sets[] = {
            KeySetType::SINGLE_VERSION_TABLET_INDEX, KeySetType::SINGLE_VERSION_META_SCHEMA,
            KeySetType::SINGLE_VERSION_META_TABLET,  KeySetType::SINGLE_VERSION_META_ROWSET,
            KeySetType::SINGLE_VERSION_TABLET_STATS, KeySetType::SINGLE_VERSION_PARTITION_VERSION,
            KeySetType::SINGLE_VERSION_TABLE_VERSION};
    for (auto ks : migrate_sets) {
        if (ks != target) {
            info.add_migrated_key_sets(ks);
        }
    }
    return info;
}

InstanceInfoPB make_stress_compact_instance(const std::string& instance_id,
                                            const std::string& source_instance_id,
                                            const std::string& snapshot_id, KeySetType target) {
    InstanceInfoPB info;
    info.set_instance_id(instance_id);
    info.set_source_instance_id(source_instance_id);
    info.set_source_snapshot_id(snapshot_id);
    info.set_multi_version_status(MultiVersionStatus::MULTI_VERSION_READ_WRITE);
    info.set_snapshot_switch_status(SnapshotSwitchStatus::SNAPSHOT_SWITCH_ON);

    const KeySetType compact_sets[] = {
            KeySetType::MULTI_VERSION_TABLE_VERSION, KeySetType::MULTI_VERSION_PARTITION_VERSION,
            KeySetType::MULTI_VERSION_INDEX_INDEX, KeySetType::MULTI_VERSION_INDEX_TABLET,
            KeySetType::MULTI_VERSION_META_ROWSET};
    for (auto ks : compact_sets) {
        if (ks != target) {
            info.add_compacted_key_sets(ks);
        }
    }
    return info;
}

void log_timing(std::string_view label, KvBackend backend, int concurrency, int64_t object_count,
                std::chrono::milliseconds elapsed) {
    std::cout << "[backend=" << backend_name(backend) << "] " << label
              << " concurrency=" << concurrency << " objects=" << object_count
              << " elapsed_ms=" << elapsed.count() << std::endl;
}

TEST_F(SnapshotParallelizationStressTest, MigrateKeySetsStress) {
    config::enable_multi_version_status = false;
    config::enable_split_tablet_schema_pb = true;
    config::enable_split_rowset_meta_pb = true;

    const std::pair<KeySetType, std::string> migrate_targets[] = {
            {KeySetType::SINGLE_VERSION_TABLET_INDEX, "migrate_tablet_index_keys"},
            {KeySetType::SINGLE_VERSION_META_SCHEMA, "migrate_tablet_schema_keys"},
            {KeySetType::SINGLE_VERSION_META_TABLET, "migrate_meta_tablet_keys"},
            {KeySetType::SINGLE_VERSION_META_ROWSET, "migrate_meta_rowset_keys"},
            {KeySetType::SINGLE_VERSION_TABLET_STATS, "migrate_tablet_stats_keys"},
            {KeySetType::SINGLE_VERSION_PARTITION_VERSION, "migrate_partition_version_keys"},
            {KeySetType::SINGLE_VERSION_TABLE_VERSION, "migrate_table_version_keys"}};

    for (const auto& [target_set, label] : migrate_targets) {
        for (int concurrency : kStressConcurrencies) {
            config::snapshot_migrate_parallelism = concurrency;
            auto kv = make_kv(backend);
            ASSERT_TRUE(kv) << "failed to init backend " << backend_name(backend);

            std::string instance_id = make_instance_id(label, concurrency);
            seed_single_version_dataset(kv, instance_id, kStressObjectCount);
            if (target_set == KeySetType::SINGLE_VERSION_META_ROWSET) {
                // meta rowset migration reads tablet meta via MetaReader (versioned namespace),
                // so pre-seed the versioned tablet meta to satisfy dependencies without timing it.
                seed_versioned_tablet_meta_only(kv, instance_id, kStressObjectCount);
                std::unique_ptr<Transaction> verify_txn;
                ASSERT_EQ(kv->create_txn(&verify_txn), TxnErrorCode::TXN_OK);
                TabletMetaCloudPB verify_meta;
                Versionstamp vs;
                ASSERT_EQ(versioned::document_get(verify_txn.get(),
                                                  versioned::meta_tablet_key({instance_id, 1}),
                                                  &verify_meta, &vs),
                          TxnErrorCode::TXN_OK);
            }

            InstanceInfoPB instance = make_stress_migrate_instance(instance_id, target_set);
            persist_instance_info(kv, instance);
            selectdb::SnapshotManager manager(kv);
            InstanceDataMigrator migrator(kv, instance);

            auto start = std::chrono::steady_clock::now();
            ASSERT_EQ(manager.migrate_to_versioned_keys(&migrator), 0);
            auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - start);
            log_timing(label, backend, concurrency, kStressObjectCount, duration_ms);
        }
    }
}

TEST_F(SnapshotParallelizationStressTest, CompactKeySetsStress) {
    config::enable_multi_version_status = false;
    config::enable_split_tablet_schema_pb = true;
    config::enable_split_rowset_meta_pb = true;

    const std::pair<KeySetType, std::string> compact_targets[] = {
            {KeySetType::MULTI_VERSION_TABLE_VERSION, "compact_table_version_keys"},
            {KeySetType::MULTI_VERSION_PARTITION_VERSION, "compact_partition_keys"},
            {KeySetType::MULTI_VERSION_INDEX_INDEX, "compact_index_keys"},
            {KeySetType::MULTI_VERSION_INDEX_TABLET, "compact_tablet_keys"}};

    for (const auto& [target_set, label] : compact_targets) {
        for (int concurrency : kStressConcurrencies) {
            config::snapshot_compact_parallelism = concurrency;
            auto kv = make_kv(backend);
            ASSERT_TRUE(kv) << "failed to init backend " << backend_name(backend);

            Versionstamp data_version = Versionstamp::min();
            // Use a snapshot version that is newer than the seeded data so reads are snapshot
            // consistent without relying on Versionstamp::max() parsing.
            Versionstamp snapshot_version(10);
            std::string source_instance = make_instance_id(label + "_src", concurrency);
            std::string target_instance = make_instance_id(label + "_dst", concurrency);
            std::string snapshot_id = selectdb::serialize_snapshot_versionstamp(snapshot_version);

            seed_versioned_dataset(kv, source_instance, kStressObjectCount, data_version);
            // Ensure schemas are present for every index; some compaction paths depend on them.
            seed_versioned_schemas_only(kv, source_instance, kStressObjectCount);
            if (target_set == KeySetType::MULTI_VERSION_INDEX_INDEX) {
                std::unique_ptr<Transaction> verify_txn;
                ASSERT_EQ(kv->create_txn(&verify_txn), TxnErrorCode::TXN_OK);
                TabletSchemaCloudPB schema;
                Versionstamp vs;
                ASSERT_EQ(versioned::document_get(
                                  verify_txn.get(),
                                  versioned::meta_schema_key(
                                          {source_instance, /*index_id*/ 1, kStressSchemaVersion}),
                                  &schema, &vs),
                          TxnErrorCode::TXN_OK);
            }

            InstanceInfoPB instance = make_stress_compact_instance(target_instance, source_instance,
                                                                   snapshot_id, target_set);
            persist_instance_info(kv, instance);
            if (target_set == KeySetType::MULTI_VERSION_INDEX_INDEX) {
                std::unique_ptr<Transaction> verify_txn;
                ASSERT_EQ(kv->create_txn(&verify_txn), TxnErrorCode::TXN_OK);
                MetaReader reader(source_instance, kv.get(), snapshot_version);
                TabletSchemaCloudPB schema;
                ASSERT_EQ(
                        reader.get_tablet_schema(verify_txn.get(), /*index_id*/ 1,
                                                 kStressSchemaVersion, &schema, /*snapshot*/ true),
                        TxnErrorCode::TXN_OK);
            }
            selectdb::SnapshotManager manager(kv);
            InstanceChainCompactor compactor(kv, instance);

            auto start = std::chrono::steady_clock::now();
            ASSERT_EQ(manager.compact_snapshot_chains(&compactor), 0);
            auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - start);
            log_timing(label, backend, concurrency, kStressObjectCount, duration_ms);
        }
    }
}

int main(int argc, char** argv) {
    auto conf_file = "doris_cloud.conf";
    if (!cloud::config::init(conf_file, true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }
    if (!cloud::init_glog("snapshot_parallelization_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
