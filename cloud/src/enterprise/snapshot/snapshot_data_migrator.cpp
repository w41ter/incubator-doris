#include "recycler/snapshot_data_migrator.h"

#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>

#include <chrono>
#include <ranges>
#include <string_view>
#include <thread>

#include "common/config.h"
#include "common/defer.h"
#include "common/logging.h"
#include "common/stopwatch.h"
#include "common/util.h"
#include "meta-service/meta_service_tablet_stats.h"
#include "meta-store/blob_message.h"
#include "meta-store/document_message.h"
#include "meta-store/keys.h"
#include "meta-store/meta_reader.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
#include "snapshot_manager.h"

using namespace doris::cloud;

namespace selectdb {

// Retry configuration for migration
static constexpr int MAX_RETRY_TIMES = 5;
static constexpr int RETRY_INTERVAL_MS = 100;

struct SnapshotDataMigrateContext {
    std::mutex mutex;

    // The indexes that have been migrated.
    std::unordered_set<int64_t> migrated_indexes;
    // The partitions that have been migrated.
    std::unordered_set<int64_t> migrated_partitions;
};

static inline bool is_partition_migrated(SnapshotDataMigrateContext& migrate_context,
                                         int64_t partition_id) {
    std::unique_lock lock(migrate_context.mutex);
    return migrate_context.migrated_partitions.contains(partition_id);
}

static inline bool is_index_migrated(SnapshotDataMigrateContext& migrate_context,
                                     int64_t index_id) {
    std::unique_lock lock(migrate_context.mutex);
    return migrate_context.migrated_indexes.contains(index_id);
}

class MigrateExecutor {
public:
    MigrateExecutor(const std::string& instance_id, std::shared_ptr<TxnKv> txn_kv)
            : instance_id_(instance_id), txn_kv_(std::move(txn_kv)) {}
    ~MigrateExecutor() = default;

    // Migrate table version keys to versioned keys
    // Return 0 for success otherwise error.
    int migrate_table_version_keys();

    // Migrate tablet schema keys to versioned keys
    // Return 0 for success otherwise error.
    int migrate_tablet_schema_keys();

    // Migrate partition version keys to versioned keys
    // Return 0 for success otherwise error.
    int migrate_partition_version_keys();

    // Migrate meta tablet keys to versioned keys
    // Return 0 for success otherwise error.
    int migrate_meta_tablet_keys();

    // Migrate tablet index keys to versioned keys.
    // Return 0 for success otherwise error.
    int migrate_tablet_index_keys();

    // Migrate meta rowset keys to versioned keys
    // Return 0 for success otherwise error.
    int migrate_meta_rowset_keys();

    // Migrate tablet stats keys to versioned keys
    // Return 0 for success otherwise error.
    int migrate_tablet_stats_keys();

private:
    // Migrate a single table version key to versioned key space
    // Returns:
    //   0: successfully migrated
    //   1: skipped (already migrated or key no longer exists)
    //  -1: error occurred
    //  -2: transaction conflict (for retry)
    int migrate_table_version_key(int64_t db_id, int64_t table_id);

    // Migrate a single tablet schema key to versioned key space
    // Returns:
    //   0: successfully migrated
    //   1: skipped (already migrated or key no longer exists)
    //  -1: error occurred
    //  -2: transaction conflict (for retry)
    int migrate_tablet_schema_key(int64_t index_id, int64_t schema_version);

    // Migrate a single partition version key to versioned key space
    // Returns:
    //   0: successfully migrated
    //   1: skipped (already migrated or key no longer exists)
    //  -1: error occurred
    //  -2: transaction conflict (for retry)
    int migrate_partition_version_key(int64_t db_id, int64_t tbl_id, int64_t partition_id);

    // Migrate a single meta tablet key to versioned key space
    // Returns:
    //   0: successfully migrated
    //   1: skipped (already migrated or key no longer exists)
    //  -1: error occurred
    //  -2: transaction conflict (for retry)
    int migrate_meta_tablet_key(int64_t table_id, int64_t index_id, int64_t partition_id,
                                int64_t tablet_id);

    // Migrate tablet stats keys to versioned key space
    // Returns:
    //   0: successfully migrated
    //   1: skipped (already migrated)
    //  -1: error occurred
    //  -2: transaction conflict (for retry)
    int migrate_tablet_stats_key(int64_t tablet_id);

    // Migrate a single meta tablet index key to versioned key space
    //
    // It also migrates partition/index meta keys under the tablet index, if they are not migrated yet.
    //
    // Returns:
    //   0: successfully migrated
    //   1: skipped (already migrated or key no longer exists)
    //  -1: error occurred
    //  -2: transaction conflict (for retry)
    int migrate_meta_tablet_idx_key(int64_t tablet_id);

    // Migrate index meta keys under the given index_id to versioned key space
    // Returns:
    //   0: successfully migrated
    //   1: skipped (already migrated)
    //  -1: error occurred
    int migrate_index_meta_keys(Transaction* txn, int64_t db_id, int64_t table_id,
                                int64_t index_id);

    // Migrate index meta keys under the given partition_id to versioned key space
    // Returns:
    //   0: successfully migrated
    //   1: skipped (already migrated)
    //  -1: error occurred
    int migrate_partition_meta_keys(Transaction* txn, int64_t db_id, int64_t table_id,
                                    int64_t partition_id);

    // Get all tablets for the instance
    // Return 0 for success otherwise error.
    int get_all_tablets(std::vector<int64_t>* tablet_ids);

    // Get the version graph for the tablet.
    // Return 0 for success otherwise error.
    int get_tablet_version_graph(int64_t tablet_id,
                                 std::map<int64_t, doris::RowsetMetaCloudPB>* version_graph);

    // Get tablet stats for a tablet
    // Returns:
    //   0: successfully got
    //   1: skipped (tablet not found)
    //  -1: error occurred
    int get_tablet_stats(Transaction* txn, int64_t tablet_id, TabletStatsPB* tablet_stats,
                         TabletStats* detached_stats);

    // Migrate rowset meta for a tablet and version (single attempt)
    // Returns:
    //   0: successfully migrated
    //   1: skipped (already migrated)
    //  -1: error occurred
    //  -2: transaction conflict (for retry)
    int migrate_rowset_meta(int64_t tablet_id, const doris::RowsetMetaCloudPB& rowset_meta,
                            Versionstamp migrate_versionstamp);

    // Retry wrapper for migration functions. Retries up to MAX_RETRY_TIMES on TXN_CONFLICT.
    // migrate_func must return: 0 (success), 1 (skipped), -1 (error), -2 (TXN_CONFLICT to retry)
    template <typename Fn, typename... Args>
        requires std::is_member_function_pointer_v<Fn> &&
                 std::is_same_v<std::invoke_result_t<Fn, MigrateExecutor*, Args...>, int>
    int retry_if_txn_conflict(Fn migrate_func, Args&&... args) {
        for (int retry = 0; retry < MAX_RETRY_TIMES; retry++) {
            if (retry > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(RETRY_INTERVAL_MS));
            }

            int res = (this->*migrate_func)(std::forward<Args>(args)...);
            if (res == -2) {
                continue; // TXN_CONFLICT
            }
            return res;
        }
        return -1;
    }

    const std::string instance_id_;
    std::shared_ptr<TxnKv> txn_kv_;
    SnapshotDataMigrateContext migrate_context_;
};

class MigrateValidator {
public:
    struct ValidationResult {
        int total_entities = 0;
        int validated_entities = 0;
        int inconsistent_entities = 0;
        int error_entities = 0;

        void apply_validation_result(int res) {
            if (res == 0) {
                validated_entities++;
            } else if (res == 1) {
                inconsistent_entities++;
            } else {
                error_entities++;
            }
        }
    };

    struct CollectedEntities {
        // table_id -> (db_id, table_id)
        std::unordered_map<int64_t, std::pair<int64_t, int64_t>> tables;
        // partition_id -> (db_id, table_id, partition_id)
        std::unordered_map<int64_t, std::tuple<int64_t, int64_t, int64_t>> partitions;
        // tablet_id -> (table_id, index_id, partition_id, tablet_id)
        std::unordered_map<int64_t, std::tuple<int64_t, int64_t, int64_t, int64_t>> tablets;
        // index_id -> (db_id, table_id, index_id)
        std::unordered_map<int64_t, std::tuple<int64_t, int64_t, int64_t>> indexes;
        // (index_id, schema_version)
        std::vector<std::pair<int64_t, int64_t>> schemas;
    };

    MigrateValidator(const std::string& instance_id, std::shared_ptr<TxnKv> txn_kv)
            : instance_id_(instance_id), txn_kv_(std::move(txn_kv)) {}

    int validate_all_migrated_keys();

private:
    int collect_all_entities(CollectedEntities* entities);

    // Validate table version keys
    int validate_table_version_keys(const CollectedEntities& entities, ValidationResult* result);

    // Validate partition related keys
    // 1. partition version key
    // 2. partition meta key
    // 3. partition index & inverted index keys
    int validate_partition_keys(const CollectedEntities& entities, ValidationResult* result);

    // Validate tablet related keys
    // 1. tablet meta key
    // 2. tablet index & inverted index keys
    // 3. tablet stats keys
    int validate_tablet_keys(const CollectedEntities& entities, ValidationResult* result);

    // Validate tablet index schema keys
    // 1. index schema keys
    // 2. index index & inverted index keys.
    int validate_tablet_schema_keys(const CollectedEntities& entities, ValidationResult* result);

    int validate_rowset_meta_keys(const CollectedEntities& entities, ValidationResult* result);

    // Helper functions for validating single entities
    // Returns: 0=consistent, 1=inconsistent, -1=error
    int validate_table_version(Transaction* txn, MetaReader& meta_reader, int64_t db_id,
                               int64_t table_id);
    int validate_partition_version(Transaction* txn, MetaReader& meta_reader, int64_t db_id,
                                   int64_t table_id, int64_t partition_id, bool* exists);
    int validate_index_schema(Transaction* txn, MetaReader& meta_reader, int64_t index_id,
                              int64_t schema_version, bool* exists);
    int validate_tablet_index(Transaction* txn, MetaReader& meta_reader, int64_t tablet_id);
    int validate_tablet_meta(Transaction* txn, MetaReader& meta_reader, int64_t table_id,
                             int64_t index_id, int64_t partition_id, int64_t tablet_id);
    int validate_rowset_metas(Transaction* txn, MetaReader& meta_reader, int64_t tablet_id);
    int validate_tablet_stats(Transaction* txn, MetaReader& meta_reader, int64_t tablet_id);

    // Validate versioned-only keys (partition/index/tablet index and inverted_index)
    int validate_partition_index_keys(Transaction* txn, int64_t partition_id, int64_t db_id,
                                      int64_t table_id);
    int validate_partition_meta_key(Transaction* txn, int64_t partition_id, int64_t db_id,
                                    int64_t table_id);
    int validate_index_index_keys(Transaction* txn, int64_t index_id, int64_t db_id,
                                  int64_t table_id);
    int validate_index_meta_key(Transaction* txn, int64_t index_id, int64_t db_id,
                                int64_t table_id);
    int validate_tablet_inverted_index_keys(Transaction* txn, int64_t tablet_id, int64_t db_id,
                                            int64_t table_id, int64_t index_id,
                                            int64_t partition_id);

    const std::string instance_id_;
    std::shared_ptr<TxnKv> txn_kv_;
};

int MigrateExecutor::migrate_table_version_key(int64_t db_id, int64_t table_id) {
    AnnotateTag table_id_tag("table_id", table_id);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn for migrating table version key").tag("error", err);
        return -1;
    }

    std::string versioned_key = versioned::table_version_key({instance_id_, table_id});

    Versionstamp version;
    std::string existing_value;
    err = versioned_get(txn.get(), versioned_key, &version, &existing_value);
    if (err == TxnErrorCode::TXN_OK) {
        // Already migrated, skip
        return 1; // skipped
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Error occurred
        LOG_WARNING("failed to get versioned key for migrating table version key")
                .tag("error", err);
        return -1;
    }

    // Not migrated yet, verify the key still exists in 0x01 space
    std::string old_key = table_version_key({instance_id_, db_id, table_id});
    std::string old_value;
    err = txn->get(old_key, &old_value);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Key was deleted, skip
        VLOG_DEBUG << "table version key already deleted for db " << db_id << ", table "
                   << table_id;
        return 1; // skipped
    } else if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to read old key for migrating table version key").tag("error", err);
        return -1;
    }

    versioned_put(txn.get(), versioned_key, "");

    err = txn->commit();
    if (err == TxnErrorCode::TXN_OK) {
        VLOG_DEBUG << "migrate table version key for db " << db_id << ", table " << table_id;
        return 0; // success
    } else if (err == TxnErrorCode::TXN_CONFLICT) {
        LOG_WARNING("migrate table version key failed due to transaction conflict");
        return -2; // TXN_CONFLICT
    } else {
        LOG_WARNING("failed to commit txn for migrating table version key").tag("error", err);
        return -1;
    }
}

int MigrateExecutor::migrate_table_version_keys() {
    LOG_INFO("begin to migrate table version keys");

    std::unique_ptr<Transaction> scan_txn;
    TxnErrorCode err = txn_kv_->create_txn(&scan_txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn for migrate table version keys").tag("error", err);
        return -1;
    }

    std::string begin_key = table_version_key({instance_id_, 0, 0});
    std::string end_key = table_version_key({instance_id_, INT64_MAX, INT64_MAX});

    FullRangeGetOptions opts;
    opts.snapshot = true;
    opts.prefetch = true;
    auto iter = scan_txn->full_range_get(begin_key, end_key, opts);

    int total_keys = 0;
    int migrated_keys = 0;
    int skipped_keys = 0;
    StopWatch stop_watch;

    DORIS_CLOUD_DEFER {
        LOG_INFO("migrate tablet version keys finished")
                .tag("total", total_keys)
                .tag("migrated", migrated_keys)
                .tag("skipped", skipped_keys)
                .tag("cost(s)", stop_watch.elapsed_seconds());
    };

    for (auto kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
        auto&& [key, value] = *kvp;
        total_keys++;

        int64_t db_id = -1;
        int64_t table_id = -1;
        std::string_view key_view(key);
        if (!decode_table_version_key(&key_view, &db_id, &table_id)) {
            LOG_WARNING("failed to decode table version key").tag("key", hex(key));
            return -1;
        }

        int result =
                retry_if_txn_conflict(&MigrateExecutor::migrate_table_version_key, db_id, table_id);
        if (result == 0) {
            migrated_keys++;
        } else if (result == 1) {
            skipped_keys++;
        } else {
            LOG_WARNING("failed to migrate table version key")
                    .tag("db_id", db_id)
                    .tag("table_id", table_id);
            return -1;
        }
    }

    if (!iter->is_valid()) {
        LOG_WARNING("failed to iterate table version keys").tag("error", iter->error_code());
        return -1;
    }

    return 0;
}

int MigrateExecutor::migrate_tablet_schema_key(int64_t index_id, int64_t schema_version) {
    AnnotateTag index_id_tag("index_id", index_id);
    AnnotateTag schema_version_tag("schema_version", schema_version);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn for migrating tablet schema key").tag("error", err);
        return -1;
    }

    std::string versioned_key =
            versioned::meta_schema_key({instance_id_, index_id, schema_version});
    std::string value;
    err = txn->get(versioned_key, &value);
    if (err == TxnErrorCode::TXN_OK) {
        // Already migrated, skip
        VLOG_DEBUG << "tablet schema key already migrated for index " << index_id
                   << ", schema version " << schema_version;
        return 1;
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_WARNING("failed to get versioned key for migrating tablet schema key")
                .tag("error", err);
        return -1;
    }

    std::string old_key = meta_schema_key({instance_id_, index_id, schema_version});
    ValueBuf value_buf;
    doris::TabletSchemaCloudPB schema_pb;
    err = blob_get(txn.get(), old_key, &value_buf);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Key was deleted, skip
        VLOG_DEBUG << "tablet schema key already deleted for index " << index_id
                   << ", schema version " << schema_version;
        return 1;
    } else if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to read old key for migrating tablet schema key").tag("error", err);
        return -1;
    } else if (!value_buf.to_pb(&schema_pb)) {
        LOG_WARNING("failed to parse old TabletSchemaPB for migrating tablet schema key")
                .tag("key", hex(old_key));
        return -1;
    }

    if (!document_put(txn.get(), versioned_key, std::move(schema_pb))) {
        LOG_WARNING("failed to serialize TabletSchemaCloudPB for migrating tablet schema key")
                .tag("key", hex(versioned_key));
        return -1;
    }

    err = txn->commit();
    if (err == TxnErrorCode::TXN_OK) {
        VLOG_DEBUG << "migrate tablet schema key for index " << index_id << ", schema version "
                   << schema_version;
        return 0; // success
    } else if (err == TxnErrorCode::TXN_CONFLICT) {
        LOG_WARNING("migrate tablet schema key failed due to transaction conflict");
        return -2; // TXN_CONFLICT
    } else {
        LOG_WARNING("failed to commit txn for migrating tablet schema key").tag("error", err);
        return -1;
    }
}

int MigrateExecutor::migrate_tablet_schema_keys() {
    LOG_INFO("begin to migrate tablet schema keys");

    std::unique_ptr<Transaction> scan_txn;
    TxnErrorCode err = txn_kv_->create_txn(&scan_txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn for migrate tablet schema keys").tag("error", err);
        return -1;
    }

    // Construct the range for tablet schema keys in 0x01 space
    std::string begin_key = meta_schema_key({instance_id_, 0, 0});
    std::string end_key = meta_schema_key({instance_id_, INT64_MAX, INT64_MAX});

    FullRangeGetOptions opts;
    opts.snapshot = true;
    opts.prefetch = true;
    auto iter = scan_txn->full_range_get(begin_key, end_key, opts);

    int total_keys = 0;
    int migrated_keys = 0;
    int skipped_keys = 0;
    StopWatch stop_watch;

    DORIS_CLOUD_DEFER {
        LOG_INFO("migrate tablet schema keys finished")
                .tag("total", total_keys)
                .tag("migrated", migrated_keys)
                .tag("skipped", skipped_keys)
                .tag("cost(s)", stop_watch.elapsed_seconds());
    };

    std::string last_key = "";
    for (auto kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
        auto&& [key, value] = *kvp;
        if (!last_key.empty() && key.starts_with(last_key)) {
            // Skip the blobs for the same schema key
            continue;
        }

        total_keys++;

        std::string_view key_view(key);
        if (key_view.size() != begin_key.size()) {
            // compatible with old version, see blob_message.h for details
            if (key_view.size() < 9) {
                LOG_WARNING("failed to decode tablet schema key").tag("key", hex(key));
                return -1;
            }
            key_view.remove_suffix(9);
        }

        last_key = key_view; // Save the last key to skip blobs

        int64_t index_id = -1;
        int64_t schema_version = -1;
        if (!decode_tablet_schema_key(&key_view, &index_id, &schema_version)) {
            LOG_WARNING("failed to decode tablet schema key").tag("key", hex(key));
            return -1;
        }

        int result = retry_if_txn_conflict(&MigrateExecutor::migrate_tablet_schema_key, index_id,
                                           schema_version);
        if (result == 0) {
            migrated_keys++;
        } else if (result == 1) {
            skipped_keys++;
        } else {
            LOG_WARNING("failed to migrate tablet schema key")
                    .tag("index_id", index_id)
                    .tag("schema_version", schema_version);
            return -1;
        }
    }

    if (!iter->is_valid()) {
        LOG_WARNING("failed to iterate tablet schema keys").tag("error", iter->error_code());
        return -1;
    }

    return 0;
}

int MigrateExecutor::migrate_partition_version_key(int64_t db_id, int64_t table_id,
                                                   int64_t partition_id) {
    AnnotateTag partition_id_tag("partition_id", partition_id);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn for migrating partition version key").tag("error", err);
        return -1;
    }

    std::string versioned_key = versioned::partition_version_key({instance_id_, partition_id});

    Versionstamp version;
    std::string existing_value;
    err = versioned_get(txn.get(), versioned_key, &version, &existing_value);
    if (err == TxnErrorCode::TXN_OK) {
        // Already migrated, skip
        VLOG_DEBUG << "partition version key already migrated for partition " << partition_id
                   << ", db " << db_id << ", table " << table_id;
        return 1;
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_WARNING("failed to get versioned key for migrating partition version key")
                .tag("error", err);
        return -1;
    }

    std::string old_key = partition_version_key({instance_id_, db_id, table_id, partition_id});
    std::string old_value;
    err = txn->get(old_key, &old_value);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Key was deleted, skip
        VLOG_DEBUG << "partition version key already deleted for partition " << partition_id
                   << ", db " << db_id << ", table " << table_id;
        return 1;
    } else if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to read old key for migrating partition version key").tag("error", err);
        return -1;
    }

    versioned_put(txn.get(), versioned_key, old_value);

    bool migrate_partition_meta_keys = false;
    if (!is_partition_migrated(migrate_context_, partition_id)) {
        std::string partition_index_key =
                versioned::partition_index_key({instance_id_, partition_id});
        std::string value;
        err = txn->get(partition_index_key, &value);
        if (err == TxnErrorCode::TXN_OK) {
            migrate_partition_meta_keys = true;
        } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            LOG_WARNING("failed to read partition meta for migrating partition version key")
                    .tag("error", err);
            return -1;
        } else {
            std::string partition_meta_key =
                    versioned::meta_partition_key({instance_id_, partition_id});
            std::string partition_inverted_index_key = versioned::partition_inverted_index_key(
                    {instance_id_, db_id, table_id, partition_id});
            PartitionIndexPB partition_index_pb;
            partition_index_pb.set_db_id(db_id);
            partition_index_pb.set_table_id(table_id);
            txn->put(partition_index_key, partition_index_pb.SerializeAsString());
            txn->put(partition_inverted_index_key, "");
            versioned_put(txn.get(), partition_meta_key, "");
            migrate_partition_meta_keys = true;
            VLOG_DEBUG << "migrate partition meta keys for partition " << partition_id << ", db "
                       << db_id << ", table " << table_id;
        }
    }

    err = txn->commit();
    if (err == TxnErrorCode::TXN_OK) {
        if (migrate_partition_meta_keys) {
            std::unique_lock lock(migrate_context_.mutex);
            migrate_context_.migrated_partitions.insert(partition_id);
        }
        VLOG_DEBUG << "migrate partition version key for partition " << partition_id << ", db "
                   << db_id << ", table " << table_id;
        return 0; // success
    } else if (err == TxnErrorCode::TXN_CONFLICT) {
        LOG_WARNING("migrate partition version key failed due to transaction conflict");
        return -2; // TXN_CONFLICT
    } else {
        LOG_WARNING("failed to commit txn for migrating partition version key").tag("error", err);
        return -1;
    }
}

int MigrateExecutor::migrate_partition_version_keys() {
    LOG_INFO("begin to migrate partition version keys");

    std::unique_ptr<Transaction> scan_txn;
    TxnErrorCode err = txn_kv_->create_txn(&scan_txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn for migrate partition version keys").tag("error", err);
        return -1;
    }

    // Construct the range for partition version keys in 0x01 space
    std::string begin_key = partition_version_key({instance_id_, 0, 0, 0});
    std::string end_key = partition_version_key({instance_id_, INT64_MAX, INT64_MAX, INT64_MAX});

    FullRangeGetOptions opts;
    opts.snapshot = true;
    opts.prefetch = true;
    auto iter = scan_txn->full_range_get(begin_key, end_key, opts);

    int total_keys = 0;
    int migrated_keys = 0;
    int skipped_keys = 0;
    StopWatch stop_watch;

    DORIS_CLOUD_DEFER {
        LOG_INFO("migrating partition version keys finished")
                .tag("total", total_keys)
                .tag("migrated", migrated_keys)
                .tag("skipped", skipped_keys)
                .tag("cost(s)", stop_watch.elapsed_seconds());
    };

    for (auto kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
        auto&& [key, value] = *kvp;
        total_keys++;

        int64_t db_id = -1;
        int64_t tbl_id = -1;
        int64_t partition_id = -1;
        std::string_view key_view(key);
        if (!decode_partition_version_key(&key_view, &db_id, &tbl_id, &partition_id)) {
            LOG_WARNING("failed to decode partition version key").tag("key", hex(key));
            return -1;
        }

        int result = retry_if_txn_conflict(&MigrateExecutor::migrate_partition_version_key, db_id,
                                           tbl_id, partition_id);
        if (result == 0) {
            migrated_keys++;
        } else if (result == 1) {
            skipped_keys++;
        } else {
            LOG_WARNING("failed to migrate partition version key")
                    .tag("db_id", db_id)
                    .tag("table_id", tbl_id)
                    .tag("partition_id", partition_id);
            return -1;
        }
    }

    if (!iter->is_valid()) {
        LOG_WARNING("failed to iterate partition version keys").tag("error", iter->error_code());
        return -1;
    }

    return 0;
}

int MigrateExecutor::migrate_index_meta_keys(Transaction* txn, int64_t db_id, int64_t table_id,
                                             int64_t index_id) {
    std::string index_index_key = versioned::index_index_key({instance_id_, index_id});
    std::string value;
    TxnErrorCode err = txn->get(index_index_key, &value);
    if (err == TxnErrorCode::TXN_OK) {
        // Already migrated, skip
        VLOG_DEBUG << "index meta keys already migrated for index " << index_id << ", db " << db_id
                   << ", table " << table_id;
        return 1;
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_WARNING("failed to read index meta for index")
                .tag("index_id", index_id)
                .tag("error", err);
        return -1;
    } else {
        std::string index_meta_key = versioned::meta_index_key({instance_id_, index_id});
        std::string index_inverted_index_key =
                versioned::index_inverted_key({instance_id_, db_id, table_id, index_id});
        IndexIndexPB index_index_pb;
        index_index_pb.set_db_id(db_id);
        index_index_pb.set_table_id(table_id);
        txn->put(index_index_key, index_index_pb.SerializeAsString());
        txn->put(index_inverted_index_key, "");
        versioned_put(txn, index_meta_key, "");
        VLOG_DEBUG << "migrate index meta keys for index " << index_id << ", db " << db_id
                   << ", table " << table_id;
        return 0;
    }
}

int MigrateExecutor::migrate_partition_meta_keys(Transaction* txn, int64_t db_id, int64_t table_id,
                                                 int64_t partition_id) {
    std::string partition_index_key = versioned::partition_index_key({instance_id_, partition_id});
    std::string value;
    TxnErrorCode err = txn->get(partition_index_key, &value);
    if (err == TxnErrorCode::TXN_OK) {
        // Already migrated, skip
        VLOG_DEBUG << "partition meta keys already migrated for partition " << partition_id
                   << ", db " << db_id << ", table " << table_id;
        return 1;
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_WARNING("failed to read partition meta for partition")
                .tag("partition_id", partition_id)
                .tag("error", err);
        return -1;
    } else {
        std::string partition_meta_key =
                versioned::meta_partition_key({instance_id_, partition_id});
        std::string partition_inverted_index_key = versioned::partition_inverted_index_key(
                {instance_id_, db_id, table_id, partition_id});
        PartitionIndexPB partition_index_pb;
        partition_index_pb.set_db_id(db_id);
        partition_index_pb.set_table_id(table_id);
        txn->put(partition_index_key, partition_index_pb.SerializeAsString());
        txn->put(partition_inverted_index_key, "");
        versioned_put(txn, partition_meta_key, "");
        VLOG_DEBUG << "migrate partition meta keys for partition " << partition_id << ", db "
                   << db_id << ", table " << table_id;
        return 0;
    }
}

int MigrateExecutor::migrate_meta_tablet_idx_key(int64_t tablet_id) {
    AnnotateTag tablet_id_tag("tablet_id", tablet_id);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn for migrating tablet index key").tag("error", err);
        return -1;
    }

    bool any_key_migrated = false;
    bool is_tablet_idx_migrated = false;
    std::string versioned_key = versioned::tablet_index_key({instance_id_, tablet_id});
    std::string existing_value;
    err = txn->get(versioned_key, &existing_value);
    if (err == TxnErrorCode::TXN_OK) {
        // This tablet index is migrated, try migrate the partition/index meta keys if needed.
        VLOG_DEBUG << "tablet index key already migrated for tablet " << tablet_id;
        is_tablet_idx_migrated = true;
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Error occurred
        LOG_WARNING("failed to get versioned key for migrating tablet index key").tag("error", err);
        return -1;
    }

    // Not migrated yet, verify the key still exists in 0x01 space
    std::string old_key = meta_tablet_idx_key({instance_id_, tablet_id});
    std::string old_value;
    err = txn->get(old_key, &old_value);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Key was deleted, skip
        return 1;
    } else if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to read old key for migrating tablet index key").tag("error", err);
        return -1;
    }

    // Parse TabletIndexPB to get table_id, index_id, partition_id
    TabletIndexPB tablet_index_pb;
    if (!tablet_index_pb.ParseFromString(old_value)) {
        LOG_WARNING("failed to parse TabletIndexPB for migrating tablet index key");
        return -1;
    } else if (!tablet_index_pb.has_db_id()) {
        LOG_WARNING("TabletIndexPB missing db_id for migrating tablet index key");
        return -1;
    }

    int64_t db_id = tablet_index_pb.db_id();
    int64_t table_id = tablet_index_pb.table_id();
    int64_t index_id = tablet_index_pb.index_id();
    int64_t partition_id = tablet_index_pb.partition_id();

    if (!is_tablet_idx_migrated) {
        any_key_migrated = true;
        txn->put(versioned_key, old_value);

        std::string tablet_inverted_index_key = versioned::tablet_inverted_index_key(
                {instance_id_, db_id, table_id, index_id, partition_id, tablet_id});
        txn->put(tablet_inverted_index_key, "");
    }

    bool is_index_meta_keys_migrated = false, is_partition_meta_keys_migrated = false;
    if (!is_index_migrated(migrate_context_, index_id)) {
        if (int res = migrate_index_meta_keys(txn.get(), db_id, table_id, index_id); res < 0) {
            return res;
        } else if (res == 0) {
            any_key_migrated = true;
        }
        is_index_meta_keys_migrated = true;
    }
    if (!is_partition_migrated(migrate_context_, partition_id)) {
        if (int res = migrate_partition_meta_keys(txn.get(), db_id, table_id, partition_id);
            res < 0) {
            return res;
        } else if (res == 0) {
            any_key_migrated = true;
        }
        is_partition_meta_keys_migrated = true;
    }

    err = txn->commit();
    if (err == TxnErrorCode::TXN_OK) {
        if (is_index_meta_keys_migrated || is_partition_meta_keys_migrated) {
            std::unique_lock lock(migrate_context_.mutex);
            if (is_index_meta_keys_migrated) {
                migrate_context_.migrated_indexes.insert(index_id);
            }
            if (is_partition_meta_keys_migrated) {
                migrate_context_.migrated_partitions.insert(partition_id);
            }
        }
        if (any_key_migrated) {
            VLOG_DEBUG << "migrate tablet index key for tablet " << tablet_id << ", db " << db_id
                       << ", table " << table_id << ", index " << index_id << ", partition "
                       << partition_id;
        }
        return any_key_migrated ? 0 : 1; // success or skipped
    } else if (err == TxnErrorCode::TXN_CONFLICT) {
        LOG_WARNING("migrate tablet index key failed due to transaction conflict");
        return -2; // TXN_CONFLICT
    } else {
        LOG_WARNING("failed to commit txn for migrating tablet index key").tag("error", err);
        return -1;
    }
}

int MigrateExecutor::migrate_tablet_index_keys() {
    LOG_INFO("begin to migrate tablet index keys");

    std::unique_ptr<Transaction> scan_txn;
    TxnErrorCode err = txn_kv_->create_txn(&scan_txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn to migrate tablet index keys").tag("error", err);
        return -1;
    }

    std::string begin_key = meta_tablet_idx_key({instance_id_, 0});
    std::string end_key = meta_tablet_idx_key({instance_id_, INT64_MAX});

    FullRangeGetOptions opts;
    opts.snapshot = true;
    opts.prefetch = true;
    auto iter = scan_txn->full_range_get(begin_key, end_key, opts);

    int total_keys = 0;
    int migrated_keys = 0;
    int skipped_keys = 0;
    StopWatch stop_watch;

    DORIS_CLOUD_DEFER {
        LOG_INFO("migrating tablet index keys finished")
                .tag("total", total_keys)
                .tag("migrated", migrated_keys)
                .tag("skipped", skipped_keys)
                .tag("cost(s)", stop_watch.elapsed_seconds());
    };

    for (auto kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
        auto&& [key, value] = *kvp;
        total_keys++;

        int64_t tablet_id = -1;
        std::string_view key_view(key);
        if (!decode_meta_tablet_idx_key(&key_view, &tablet_id)) {
            LOG_WARNING("failed to decode meta tablet index key").tag("key", hex(key));
            return -1;
        }

        int result =
                retry_if_txn_conflict(&MigrateExecutor::migrate_meta_tablet_idx_key, tablet_id);
        if (result == 0) {
            migrated_keys++;
        } else if (result == 1) {
            skipped_keys++;
        } else {
            LOG_WARNING("failed to migrate tablet idx key").tag("tablet_id", tablet_id);
            return -1;
        }
    }

    if (!iter->is_valid()) {
        LOG_WARNING("failed to iterate tablet index keys").tag("error", iter->error_code());
        return -1;
    }

    return 0;
}

int MigrateExecutor::migrate_meta_tablet_key(int64_t table_id, int64_t index_id,
                                             int64_t partition_id, int64_t tablet_id) {
    AnnotateTag tablet_id_tag("tablet_id", tablet_id);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn for migrating meta tablet key").tag("error", err);
        return -1;
    }

    std::string versioned_key = versioned::meta_tablet_key({instance_id_, tablet_id});

    Versionstamp version;
    std::string existing_value;
    err = versioned_get(txn.get(), versioned_key, &version, &existing_value);
    if (err == TxnErrorCode::TXN_OK) {
        // Already migrated, skip
        VLOG_DEBUG << "meta tablet key already migrated for tablet " << tablet_id;
        return 1;
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_WARNING("failed to get versioned key for migrating meta tablet key").tag("error", err);
        return -1;
    }

    std::string old_key =
            meta_tablet_key({instance_id_, table_id, index_id, partition_id, tablet_id});
    std::string old_value;
    err = txn->get(old_key, &old_value);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Key was deleted, skip
        VLOG_DEBUG << "meta tablet key already deleted for tablet " << tablet_id;
        return 1;
    } else if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to read old key for migrating meta tablet key").tag("error", err);
        return -1;
    }

    versioned_put(txn.get(), versioned_key, old_value);

    err = txn->commit();
    if (err == TxnErrorCode::TXN_OK) {
        VLOG_DEBUG << "migrate meta tablet key for tablet " << tablet_id;
        return 0; // success
    } else if (err == TxnErrorCode::TXN_CONFLICT) {
        LOG_WARNING("migrate meta tablet key failed due to transaction conflict");
        return -2; // TXN_CONFLICT
    } else {
        LOG_WARNING("failed to commit txn for migrating meta tablet key").tag("error", err);
        return -1;
    }
}

int MigrateExecutor::migrate_meta_tablet_keys() {
    LOG_INFO("begin to migrate meta tablet keys");

    std::unique_ptr<Transaction> scan_txn;
    TxnErrorCode err = txn_kv_->create_txn(&scan_txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn for migrate meta tablet keys").tag("error", err);
        return -1;
    }

    std::string begin_key = meta_tablet_key({instance_id_, 0, 0, 0, 0});
    std::string end_key =
            meta_tablet_key({instance_id_, INT64_MAX, INT64_MAX, INT64_MAX, INT64_MAX});

    FullRangeGetOptions opts;
    opts.snapshot = true;
    opts.prefetch = true;
    auto iter = scan_txn->full_range_get(begin_key, end_key, opts);

    int total_keys = 0;
    int migrated_keys = 0;
    int skipped_keys = 0;
    StopWatch stop_watch;

    DORIS_CLOUD_DEFER {
        LOG_INFO("migrating meta tablet keys finished")
                .tag("total", total_keys)
                .tag("migrated", migrated_keys)
                .tag("skipped", skipped_keys)
                .tag("cost(s)", stop_watch.elapsed_seconds());
    };

    for (auto kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
        auto&& [key, value] = *kvp;
        total_keys++;

        int64_t table_id = -1;
        int64_t index_id = -1;
        int64_t partition_id = -1;
        int64_t tablet_id = -1;
        std::string_view key_view(key);
        if (!decode_meta_tablet_key(&key_view, &table_id, &index_id, &partition_id, &tablet_id)) {
            LOG_WARNING("failed to decode meta tablet key").tag("key", hex(key));
            return -1;
        }

        int result = retry_if_txn_conflict(&MigrateExecutor::migrate_meta_tablet_key, table_id,
                                           index_id, partition_id, tablet_id);
        if (result == 0) {
            migrated_keys++;
        } else if (result == 1) {
            skipped_keys++;
        } else {
            LOG_WARNING("failed to migrate meta tablet key")
                    .tag("table_id", table_id)
                    .tag("index_id", index_id)
                    .tag("partition_id", partition_id)
                    .tag("tablet_id", tablet_id);
            return -1;
        }
    }

    if (!iter->is_valid()) {
        LOG_WARNING("failed to iterate meta tablet keys").tag("error", iter->error_code());
        return -1;
    }

    return 0;
}

int MigrateExecutor::migrate_rowset_meta(int64_t tablet_id,
                                         const doris::RowsetMetaCloudPB& rowset_meta,
                                         Versionstamp migrate_versionstamp) {
    int64_t start_version = rowset_meta.start_version();
    int64_t end_version = rowset_meta.end_version();
    AnnotateTag tablet_id_tag("tablet_id", tablet_id);
    AnnotateTag version_tag("version", end_version);
    AnnotateTag rowset_id_tag("rowset_id", rowset_meta.rowset_id_v2());

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn to migrate rowset meta").tag("error", err);
        return -1;
    }

    std::string rowset_meta_key = meta_rowset_key({instance_id_, tablet_id, end_version});
    std::string value;
    err = txn->get(rowset_meta_key, &value);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Already deleted, skip
        VLOG_DEBUG << "rowset meta not found for tablet " << tablet_id << ", version "
                   << end_version;
        return 1;
    } else if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to read RowsetMetaCloudPB to migrate rowset meta").tag("error", err);
        return -1;
    }

    doris::RowsetMetaCloudPB old_rowset_meta;
    if (!old_rowset_meta.ParseFromString(value)) {
        LOG_WARNING("failed to parse RowsetMetaCloudPB to migrate rowset meta");
        return -1;
    } else if (old_rowset_meta.rowset_id_v2() != rowset_meta.rowset_id_v2()) {
        // Already modified during migration, skip
        VLOG_DEBUG << "rowset meta already modified for tablet " << tablet_id << ", version "
                   << end_version << " by another, new rowset_id "
                   << old_rowset_meta.rowset_id_v2();
        return 1;
    }

    // Check whether the versioned key already exists.
    std::string versioned_key;
    if (start_version == end_version || end_version == 1) {
        versioned_key = versioned::meta_rowset_load_key({instance_id_, tablet_id, end_version});
    } else {
        versioned_key = versioned::meta_rowset_compact_key({instance_id_, tablet_id, end_version});
    }

    doris::RowsetMetaCloudPB exists_value;
    Versionstamp exists_versionstamp;
    err = versioned::document_get(txn.get(), versioned_key, &exists_value, &exists_versionstamp);
    if (err == TxnErrorCode::TXN_OK) {
        // Already migrated, skip
        VLOG_DEBUG << "rowset meta already migrated for tablet " << tablet_id << ", version "
                   << end_version;
        return 1;
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_WARNING("failed to get versioned key to migrate rowset meta").tag("error", err);
        return -1;
    }

    std::string rowset_id = rowset_meta.rowset_id_v2();
    doris::RowsetMetaCloudPB rowset_meta_copy = rowset_meta;
    if (!versioned::document_put(txn.get(), versioned_key, migrate_versionstamp,
                                 std::move(rowset_meta_copy))) {
        LOG_WARNING("failed to serialize RowsetMetaCloudPB to migrate rowset meta keys");
        return -1;
    }

    std::string data_ref_count_key = versioned::data_rowset_ref_count_key(
            {instance_id_, tablet_id, rowset_meta.rowset_id_v2()});
    txn->atomic_add(data_ref_count_key, 1);

    err = txn->commit();
    if (err == TxnErrorCode::TXN_OK) {
        VLOG_DEBUG << "migrate rowset meta for tablet " << tablet_id << ", version " << end_version
                   << ", rowset_id " << rowset_meta.rowset_id_v2();
        return 0; // success
    } else if (err == TxnErrorCode::TXN_CONFLICT) {
        LOG_WARNING("migrate rowset meta failed due to transaction conflict");
        return -2; // TXN_CONFLICT
    } else {
        LOG_WARNING("failed to commit txn for migrating rowset meta").tag("error", err);
        return -1;
    }
}

int MigrateExecutor::migrate_meta_rowset_keys() {
    LOG_INFO("begin to migrate meta rowset keys");

    std::vector<int64_t> tablet_ids;
    int res = get_all_tablets(&tablet_ids);
    if (res != 0) {
        LOG_WARNING("failed to scan tablets for migrate meta rowset keys");
        return -1;
    }

    int total_keys = 0;
    int migrated_keys = 0;
    int skipped_keys = 0;
    StopWatch stop_watch;

    DORIS_CLOUD_DEFER {
        LOG_INFO("migrating meta rowset keys finished")
                .tag("total", total_keys)
                .tag("migrated", migrated_keys)
                .tag("skipped", skipped_keys)
                .tag("cost(s)", stop_watch.elapsed_seconds());
    };

    for (int64_t tablet_id : tablet_ids) {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to create txn for migrate meta rowset keys").tag("error", err);
            return -1;
        }

        std::map<int64_t, doris::RowsetMetaCloudPB> rowset_meta_map;
        res = get_tablet_version_graph(tablet_id, &rowset_meta_map);
        if (res != 0) {
            LOG_WARNING("failed to get version graph for migrate meta rowset keys")
                    .tag("tablet_id", tablet_id);
            return -1;
        }

        MetaReader meta_reader(instance_id_, txn_kv_.get());
        std::vector<doris::RowsetMetaCloudPB> versioned_rowset_metas;
        err = meta_reader.get_rowset_metas(tablet_id, 0, std::numeric_limits<int64_t>::max(),
                                           &versioned_rowset_metas);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to get versioned rowset metas for migrate meta rowset keys")
                    .tag("tablet_id", tablet_id)
                    .tag("error", err);
            return -1;
        }

        std::map<int64_t, doris::RowsetMetaCloudPB> versioned_rowset_meta_map;
        for (auto&& rowset_meta : versioned_rowset_metas) {
            versioned_rowset_meta_map[rowset_meta.end_version()] = rowset_meta;
        }

        int64_t read_version = -1;
        err = txn->get_read_version(&read_version);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to get read version for migrate meta rowset keys")
                    .tag("tablet_id", tablet_id)
                    .tag("error", err);
            return -1;
        }

        // The migrate versionstamp is used to determine the versionstamp of the migrated rowset meta.
        // It should ensure that the migrated rowset meta has a versionstamp smaller than the read version of
        // the current transaction, so that the migrated rowset meta is visible to the current transaction,
        // but does not affect the visibility of other data written after current transaction.
        Versionstamp migrate_versionstamp(read_version - 1, 0);
        for (auto&& [_, rowset_meta] : rowset_meta_map) {
            total_keys++;

            auto it = versioned_rowset_meta_map.lower_bound(rowset_meta.end_version());
            if (it != versioned_rowset_meta_map.end() &&
                it->second.start_version() <= rowset_meta.start_version()) {
                // This rowset_meta is already covered by a versioned rowset meta
                skipped_keys++;
                continue;
            }

            int result = retry_if_txn_conflict(&MigrateExecutor::migrate_rowset_meta, tablet_id,
                                               rowset_meta, migrate_versionstamp);
            if (result == 0) {
                migrated_keys++;
            } else if (result == 1) {
                skipped_keys++;
            } else {
                LOG_WARNING("failed to migrate rowset meta")
                        .tag("tablet_id", tablet_id)
                        .tag("version", rowset_meta.end_version())
                        .tag("rowset_id", rowset_meta.rowset_id_v2());
                return -1;
            }
        }
    }

    return 0;
}

int MigrateExecutor::get_tablet_stats(Transaction* txn, int64_t tablet_id,
                                      TabletStatsPB* tablet_stats, TabletStats* detached_stats) {
    std::string tablet_idx_key = meta_tablet_idx_key({instance_id_, tablet_id});
    std::string value;
    TabletIndexPB tablet_idx;
    TxnErrorCode err = txn->get(tablet_idx_key, &value);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Tablet was deleted, skip
        VLOG_DEBUG << "tablet index not found when getting tablet stats, tablet " << tablet_id;
        return 1;
    } else if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get tablet index when getting tablet stats")
                .tag("error", err)
                .tag("tablet_id", tablet_id);
        return -1;
    } else if (!tablet_idx.ParseFromString(value)) {
        LOG_WARNING("failed to parse tablet index when getting tablet stats")
                .tag("tablet_id", tablet_id);
        return -1;
    }

    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg;
    tablet_idx.set_tablet_id(tablet_id);
    internal_get_tablet_stats(code, msg, txn, instance_id_, tablet_idx, *tablet_stats,
                              *detached_stats);
    if (code == MetaServiceCode::TABLET_NOT_FOUND) {
        // Tablet was deleted, skip
        VLOG_DEBUG << "tablet stats not found, tablet " << tablet_id;
        return 1;
    } else if (code != MetaServiceCode::OK) {
        LOG_WARNING("failed to get tablet stats")
                .tag("code", code)
                .tag("msg", msg)
                .tag("tablet_id", tablet_id);
        return -1;
    }
    return 0;
}

int MigrateExecutor::migrate_tablet_stats_key(int64_t tablet_id) {
    AnnotateTag tablet_id_tag("tablet_id", tablet_id);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn to migrate tablet stats keys").tag("error", err);
        return -1;
    }

    TabletStatsPB tablet_stats;
    TabletStats detached_stats;
    int res = get_tablet_stats(txn.get(), tablet_id, &tablet_stats, &detached_stats);
    if (res == 1) {
        LOG_WARNING("tablet is not found when migrating stats keys, skip")
                .tag("tablet_id", tablet_id);
        return 1;
    } else if (res != 0) {
        LOG_WARNING("failed to get tablet stats to migrate stats keys");
        return -1;
    }

    bool is_load_migrated = true, is_compact_migrated = true;
    MetaReader meta_reader(instance_id_);
    TabletStatsPB load_stats, compact_stats;
    err = meta_reader.get_tablet_load_stats(txn.get(), tablet_id, &load_stats, nullptr);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        is_load_migrated = false;
    } else if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get tablet load stats to migrate stats keys").tag("error", err);
        return -1;
    }

    err = meta_reader.get_tablet_compact_stats(txn.get(), tablet_id, &compact_stats, nullptr);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        is_compact_migrated = false;
    } else if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get tablet compact stats to migrate stats keys").tag("error", err);
        return -1;
    }

    std::string load_stats_key = versioned::tablet_load_stats_key({instance_id_, tablet_id});
    std::string compact_stats_key = versioned::tablet_compact_stats_key({instance_id_, tablet_id});
    if (is_load_migrated && is_compact_migrated) {
        // Already migrated, skip
        return 1;
    }

    if (!is_load_migrated) {
        // Need to migrate load stats (detached stats)
        merge_tablet_stats(load_stats, detached_stats);
        versioned_put(txn.get(), load_stats_key, load_stats.SerializeAsString());
    }

    if (!is_compact_migrated) {
        // Need to migrate compact stats (non-detached stats)
        versioned_put(txn.get(), compact_stats_key, tablet_stats.SerializeAsString());
    }

    err = txn->commit();
    if (err == TxnErrorCode::TXN_CONFLICT) {
        LOG_WARNING("migrate tablet stats keys failed due to transaction conflict");
        return -2; // TXN_CONFLICT
    } else if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to commit txn to migrate tablet stats keys").tag("error", err);
        return -1;
    } else {
        VLOG_DEBUG << "migrate tablet stats keys for tablet " << tablet_id;
        return 0;
    }
}

int MigrateExecutor::migrate_tablet_stats_keys() {
    LOG_INFO("begin to migrate tablet stats keys");

    std::vector<int64_t> tablet_ids;
    int res = get_all_tablets(&tablet_ids);
    if (res != 0) {
        LOG_WARNING("failed to get all tablets to migrate stats keys");
        return -1;
    }

    int total_keys = 0;
    int migrated_keys = 0;
    int skipped_keys = 0;
    StopWatch stop_watch;

    DORIS_CLOUD_DEFER {
        LOG_INFO("migrate tablet stats keys finished")
                .tag("total", total_keys)
                .tag("migrated", migrated_keys)
                .tag("skipped", skipped_keys)
                .tag("cost(s)", stop_watch.elapsed_seconds());
    };

    for (int64_t tablet_id : tablet_ids) {
        total_keys++;

        int result = retry_if_txn_conflict(&MigrateExecutor::migrate_tablet_stats_key, tablet_id);
        if (result == 0) {
            migrated_keys++;
        } else if (result == 1) {
            skipped_keys++;
        } else {
            LOG_WARNING("failed to migrate tablet stats").tag("tablet_id", tablet_id);
            return -1;
        }
    }
    return 0;
}

int MigrateExecutor::get_all_tablets(std::vector<int64_t>* tablet_ids) {
    tablet_ids->clear();

    std::unique_ptr<Transaction> scan_txn;
    TxnErrorCode err = txn_kv_->create_txn(&scan_txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn for get all tablets").tag("error", err);
        return -1;
    }

    // Construct the range for meta tablet index keys in 0x01 space
    std::string begin_key = meta_tablet_idx_key({instance_id_, 0});
    std::string end_key = meta_tablet_idx_key({instance_id_, INT64_MAX});

    FullRangeGetOptions opts;
    opts.snapshot = true;
    opts.prefetch = true;
    auto iter = scan_txn->full_range_get(begin_key, end_key, opts);

    for (auto kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
        auto&& [key, value] = *kvp;

        int64_t tablet_id = -1;
        std::string_view key_view(key);
        if (!decode_meta_tablet_idx_key(&key_view, &tablet_id)) {
            LOG_WARNING("failed to decode meta tablet index key").tag("key", hex(key));
            return -1;
        }

        tablet_ids->push_back(tablet_id);
    }

    if (!iter->is_valid()) {
        LOG_WARNING("failed to iterate tablet index keys").tag("error", iter->error_code());
        return -1;
    }

    return 0;
}

int MigrateExecutor::get_tablet_version_graph(
        int64_t tablet_id, std::map<int64_t, doris::RowsetMetaCloudPB>* version_graph) {
    version_graph->clear();
    std::unique_ptr<Transaction> scan_txn;
    TxnErrorCode err = txn_kv_->create_txn(&scan_txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn for get tablet version graph")
                .tag("tablet_id", tablet_id)
                .tag("error", err);
        return -1;
    }

    std::string begin_key = meta_rowset_key({instance_id_, tablet_id, 0});
    std::string end_key = meta_rowset_key({instance_id_, tablet_id, INT64_MAX});

    FullRangeGetOptions opts;
    opts.snapshot = true;
    opts.prefetch = true;
    auto iter = scan_txn->full_range_get(begin_key, end_key, opts);
    for (auto kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
        auto&& [_, value] = *kvp;
        doris::RowsetMetaCloudPB rowset_meta;
        if (!rowset_meta.ParseFromArray(value.data(), value.size())) {
            LOG_WARNING("failed to parse RowsetMetaCloudPB for get tablet version graph")
                    .tag("tablet_id", tablet_id);
            return -1;
        }
        int64_t version = rowset_meta.end_version();
        (*version_graph)[version] = rowset_meta;
    }

    if (!iter->is_valid()) {
        LOG_WARNING("failed to iterate meta rowset keys")
                .tag("tablet_id", tablet_id)
                .tag("error", iter->error_code());
        return -1;
    }

    return 0;
}

int persist_migrated_key_set(const std::string& instance_id, KeySetType key_set,
                             std::shared_ptr<TxnKv> txn_kv) {
    for (int retry = 0; retry < MAX_RETRY_TIMES; retry++) {
        if (retry > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(RETRY_INTERVAL_MS));
        }

        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to create txn to save migrated key set").tag("error", err);
            return -1;
        }

        std::string key = instance_key(instance_id);
        std::string instance_value;
        err = txn->get(key, &instance_value);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to read instance info to save migrated key set").tag("error", err);
            return -1;
        }

        InstanceInfoPB instance_info;
        if (!instance_info.ParseFromString(instance_value)) {
            LOG_WARNING("failed to parse instance info to save migrated key set");
            return -1;
        }

        // Check if already recorded
        if (std::find(instance_info.migrated_key_sets().begin(),
                      instance_info.migrated_key_sets().end(),
                      key_set) != instance_info.migrated_key_sets().end()) {
            LOG_INFO("key set already recorded as migrated")
                    .tag("key_set", KeySetType_Name(key_set));
            return 0; // already recorded
        }

        // Add the new migrated key set
        instance_info.add_migrated_key_sets(key_set);
        std::string updated_value;
        if (!instance_info.SerializeToString(&updated_value)) {
            LOG_WARNING("failed to serialize updated instance info to save migrated key set");
            return -1;
        }

        txn->put(key, updated_value);
        err = txn->commit();
        if (err == TxnErrorCode::TXN_CONFLICT) {
            LOG_WARNING("txn conflict while saving migrated key set, will retry")
                    .tag("retry", retry);
            continue; // retry
        } else if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to commit txn to save migrated key set").tag("error", err);
            return -1;
        } else {
            return 0;
        }
    }

    LOG_WARNING("failed to save migrated key set after max retries")
            .tag("max_retries", MAX_RETRY_TIMES);
    return -1;
}

int SnapshotManager::migrate_to_versioned_keys(InstanceDataMigrator* migrator) {
    std::string instance_id(migrator->instance_id());
    const InstanceInfoPB& instance = migrator->instance_info();
    AnnotateTag instance_tag("instance", instance_id);
    MigrateExecutor executor(instance_id, txn_kv_);

    // ATTN: Order matters, some key sets depend on others being migrated first.
    //
    // The TABLET_INDEX must be migrated first, then order the keys by their modification
    // frequency from low to high, to minimize the chance of conflicts during migration.
    // (since high modification frequency keys are more likely no need to be migrated)
    // clang-format off
    KeySetType key_sets_to_migrate[] = {
            KeySetType::SINGLE_VERSION_TABLET_INDEX,
            KeySetType::SINGLE_VERSION_META_SCHEMA,
            KeySetType::SINGLE_VERSION_META_TABLET,
            KeySetType::SINGLE_VERSION_META_ROWSET,
            KeySetType::SINGLE_VERSION_TABLET_STATS,
            KeySetType::SINGLE_VERSION_PARTITION_VERSION,
            KeySetType::SINGLE_VERSION_TABLE_VERSION,
    };
    // clang-format on

    for (auto key_set : key_sets_to_migrate) {
        const auto& migrated_key_sets = instance.migrated_key_sets();
        if (std::find(migrated_key_sets.begin(), migrated_key_sets.end(), key_set) !=
            migrated_key_sets.end()) {
            // Skip already migrated key sets
            continue;
        }

        int ret = 0;
        switch (key_set) {
        case KeySetType::SINGLE_VERSION_TABLET_INDEX:
            ret = executor.migrate_tablet_index_keys();
            break;
        case KeySetType::SINGLE_VERSION_TABLE_VERSION:
            ret = executor.migrate_table_version_keys();
            break;
        case KeySetType::SINGLE_VERSION_PARTITION_VERSION:
            ret = executor.migrate_partition_version_keys();
            break;
        case KeySetType::SINGLE_VERSION_META_TABLET:
            ret = executor.migrate_meta_tablet_keys();
            break;
        case KeySetType::SINGLE_VERSION_META_SCHEMA:
            ret = executor.migrate_tablet_schema_keys();
            break;
        case KeySetType::SINGLE_VERSION_META_ROWSET:
            ret = executor.migrate_meta_rowset_keys();
            break;
        case KeySetType::SINGLE_VERSION_TABLET_STATS:
            ret = executor.migrate_tablet_stats_keys();
            break;
        default:
            LOG_WARNING("unknown key set type for migration")
                    .tag("key_set", KeySetType_Name(key_set));
            ret = -1;
            break;
        }

        if (ret == 0) {
            ret = persist_migrated_key_set(instance_id, key_set, txn_kv_);
        }

        if (ret != 0) {
            LOG_WARNING("migration failed for key set type")
                    .tag("key_set", KeySetType_Name(key_set));
            return ret;
        }
    }

    if (config::enable_snapshot_data_migrator_validation) {
        MigrateValidator validator(instance_id, txn_kv_);
        int validation_ret = validator.validate_all_migrated_keys();
        if (validation_ret != 0) {
            LOG_WARNING("validation failed after migration").tag("error_code", validation_ret);
            if (!config::allow_snapshot_data_validation_failure) {
                return -1;
            }
        }
    }

    return 0;
}

// ==================== MigrateValidator Implementation ====================

int MigrateValidator::collect_all_entities(CollectedEntities* entities) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn for collecting entities").tag("error", err);
        return -1;
    }

    // Collect table version keys
    {
        std::string begin_key = table_version_key({instance_id_, 0, 0});
        std::string end_key = table_version_key({instance_id_, INT64_MAX, INT64_MAX});
        FullRangeGetOptions opts;
        opts.snapshot = true;
        opts.prefetch = true;
        auto iter = txn->full_range_get(begin_key, end_key, opts);
        for (auto kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
            auto&& [key, value] = *kvp;
            int64_t db_id = -1, table_id = -1;
            std::string_view key_view(key);
            if (!decode_table_version_key(&key_view, &db_id, &table_id)) {
                LOG_WARNING("failed to decode table version key").tag("key", hex(key));
                return -1;
            }
            entities->tables[table_id] = {db_id, table_id};
        }

        if (!iter->is_valid()) {
            LOG_WARNING("failed to iterate table version keys").tag("error", iter->error_code());
            return -1;
        }
    }

    // Collect partition version keys
    {
        std::string begin_key = partition_version_key({instance_id_, 0, 0, 0});
        std::string end_key =
                partition_version_key({instance_id_, INT64_MAX, INT64_MAX, INT64_MAX});
        FullRangeGetOptions opts;
        opts.snapshot = true;
        opts.prefetch = true;
        auto iter = txn->full_range_get(begin_key, end_key, opts);
        for (auto kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
            auto&& [key, value] = *kvp;
            int64_t db_id = -1, table_id = -1, partition_id = -1;
            std::string_view key_view(key);
            if (!decode_partition_version_key(&key_view, &db_id, &table_id, &partition_id)) {
                LOG_WARNING("failed to decode partition version key").tag("key", hex(key));
                return -1;
            }
            entities->partitions[partition_id] = {db_id, table_id, partition_id};
        }
        if (!iter->is_valid()) {
            LOG_WARNING("failed to iterate partition version keys")
                    .tag("error", iter->error_code());
            return -1;
        }
    }

    // Collect tablet index keys
    {
        std::string begin_key = meta_tablet_idx_key({instance_id_, 0});
        std::string end_key = meta_tablet_idx_key({instance_id_, INT64_MAX});
        FullRangeGetOptions opts;
        opts.snapshot = true;
        opts.prefetch = true;
        auto iter = txn->full_range_get(begin_key, end_key, opts);
        for (auto kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
            auto&& [key, value] = *kvp;
            int64_t tablet_id = -1;
            std::string_view key_view(key);
            if (!decode_meta_tablet_idx_key(&key_view, &tablet_id)) {
                LOG_WARNING("failed to decode meta tablet idx key").tag("key", hex(key));
                return -1;
            }

            TabletIndexPB tablet_index;
            if (!tablet_index.ParseFromArray(value.data(), value.size())) {
                LOG_WARNING("failed to parse TabletIndexPB").tag("tablet_id", tablet_id);
                return -1;
            }

            int64_t db_id = tablet_index.db_id();
            int64_t table_id = tablet_index.table_id();
            int64_t index_id = tablet_index.index_id();
            int64_t partition_id = tablet_index.partition_id();

            entities->tablets[tablet_id] = {table_id, index_id, partition_id, tablet_id};
            entities->indexes[index_id] = {db_id, table_id, index_id};
        }
        if (!iter->is_valid()) {
            LOG_WARNING("failed to iterate tablet index keys").tag("error", iter->error_code());
            return -1;
        }
    }

    // Collect tablet schema keys
    {
        std::string begin_key = meta_schema_key({instance_id_, 0, 0});
        std::string end_key = meta_schema_key({instance_id_, INT64_MAX, INT64_MAX});
        FullRangeGetOptions opts;
        opts.snapshot = true;
        opts.prefetch = true;
        auto iter = txn->full_range_get(begin_key, end_key, opts);

        std::string last_key = "";
        for (auto kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
            auto&& [key, value] = *kvp;
            std::string_view key_view(key);
            if (key_view.size() != begin_key.size()) {
                // compatible with old version (blob message), see blob_message.h
                if (key_view.size() < 9) {
                    LOG_WARNING("failed to decode tablet schema key").tag("key", hex(key));
                    return -1;
                }
                key_view.remove_suffix(9);
            }

            if (!last_key.empty() && key.starts_with(last_key)) {
                // Skip blobs for the same schema key
                continue;
            }
            last_key = std::string(key_view);

            int64_t index_id = -1, schema_version = -1;
            if (!decode_tablet_schema_key(&key_view, &index_id, &schema_version)) {
                LOG_WARNING("failed to decode tablet schema key").tag("key", hex(key));
                return -1;
            }
            entities->schemas.push_back({index_id, schema_version});
        }
        if (!iter->is_valid()) {
            LOG_WARNING("failed to iterate tablet schema keys").tag("error", iter->error_code());
            return -1;
        }
    }

    LOG_INFO("collected entities for validation")
            .tag("tables", entities->tables.size())
            .tag("partitions", entities->partitions.size())
            .tag("tablets", entities->tablets.size())
            .tag("indexes", entities->indexes.size())
            .tag("schemas", entities->schemas.size());

    return 0;
}

int MigrateValidator::validate_all_migrated_keys() {
    LOG_INFO("begin to validate migrated keys");

    CollectedEntities entities;
    if (collect_all_entities(&entities) != 0) {
        LOG_WARNING("failed to collect entities for validation");
        return -1;
    }

    ValidationResult result;
    StopWatch stop_watch;

    DORIS_CLOUD_DEFER {
        LOG_INFO("validate migrated keys finished")
                .tag("total", result.total_entities)
                .tag("validated", result.validated_entities)
                .tag("inconsistent", result.inconsistent_entities)
                .tag("error", result.error_entities)
                .tag("cost(s)", stop_watch.elapsed_seconds());
    };

    // Validate each key type
    if (validate_table_version_keys(entities, &result) != 0) {
        return -1;
    }

    if (validate_partition_keys(entities, &result) != 0) {
        return -1;
    }

    if (validate_tablet_schema_keys(entities, &result) != 0) {
        return -1;
    }

    if (validate_tablet_keys(entities, &result) != 0) {
        return -1;
    }

    if (validate_rowset_meta_keys(entities, &result) != 0) {
        return -1;
    }

    return result.inconsistent_entities > 0 || result.error_entities > 0 ? -1 : 0;
}

int MigrateValidator::validate_table_version(Transaction* txn, MetaReader& meta_reader,
                                             int64_t db_id, int64_t table_id) {
    // Read old version key
    std::string old_key = table_version_key({instance_id_, db_id, table_id});
    std::string old_value;
    TxnErrorCode old_err = txn->get(old_key, &old_value);

    // Read new version key using MetaReader
    Versionstamp new_version;
    TxnErrorCode new_err = meta_reader.get_table_version(txn, table_id, &new_version, true);

    bool old_exists = (old_err == TxnErrorCode::TXN_OK);
    bool new_exists = (new_err == TxnErrorCode::TXN_OK);

    if (old_exists != new_exists) {
        LOG_WARNING("table version key existence mismatch")
                .tag("table_id", table_id)
                .tag("db_id", db_id)
                .tag("old_key", hex(old_key))
                .tag("old_exists", old_exists)
                .tag("new_exists", new_exists);
        return 1; // inconsistent
    }

    if (old_err != TxnErrorCode::TXN_OK && old_err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_WARNING("failed to read old table version key").tag("error", old_err);
        return -1;
    }

    if (new_err != TxnErrorCode::TXN_OK && new_err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_WARNING("failed to read new table version key").tag("error", new_err);
        return -1;
    }

    return 0; // consistent
}

int MigrateValidator::validate_table_version_keys(const CollectedEntities& entities,
                                                  ValidationResult* result) {
    LOG_INFO("begin to validate table version keys").tag("count", entities.tables.size());

    for (const auto& [table_id, db_table] : entities.tables) {
        result->total_entities++;
        auto [db_id, _] = db_table;

        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to create txn for validation").tag("error", err);
            result->error_entities++;
            continue;
        }

        MetaReader meta_reader(instance_id_, txn_kv_.get());
        int ret = validate_table_version(txn.get(), meta_reader, db_id, table_id);
        if (ret == 0) {
            result->validated_entities++;
        } else if (ret == 1) {
            result->inconsistent_entities++;
        } else {
            result->error_entities++;
        }
    }

    return 0;
}

int MigrateValidator::validate_partition_version(Transaction* txn, MetaReader& meta_reader,
                                                 int64_t db_id, int64_t table_id,
                                                 int64_t partition_id, bool* exists) {
    AnnotateTag partition_tag("partition_id", partition_id);
    AnnotateTag table_tag("table_id", table_id);
    AnnotateTag db_tag("db_id", db_id);

    // Read old version key
    std::string old_key = partition_version_key({instance_id_, db_id, table_id, partition_id});
    std::string old_value;
    TxnErrorCode old_err = txn->get(old_key, &old_value);

    // Read new version key using MetaReader
    VersionPB new_version;
    Versionstamp new_versionstamp;
    TxnErrorCode new_err = meta_reader.get_partition_version(txn, partition_id, &new_version,
                                                             &new_versionstamp, true);

    bool old_exists = (old_err == TxnErrorCode::TXN_OK);
    bool new_exists = (new_err == TxnErrorCode::TXN_OK);

    if (old_exists != new_exists) {
        LOG_WARNING("partition version key existence mismatch")
                .tag("old_key", hex(old_key))
                .tag("old_exists", old_exists)
                .tag("new_exists", new_exists);
        return 1; // inconsistent
    }

    if (old_err != TxnErrorCode::TXN_OK && old_err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_WARNING("failed to read old partition version key").tag("error", old_err);
        return -1;
    }

    if (new_err != TxnErrorCode::TXN_OK && new_err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_WARNING("failed to read new partition version key").tag("error", new_err);
        return -1;
    }

    if (!old_exists) {
        *exists = false;
        return 0; // both not exist
    }

    // Compare values
    VersionPB old_version;
    if (!old_version.ParseFromString(old_value)) {
        LOG_WARNING("failed to parse old partition version");
        return -1;
    }

    std::string old_serialized, new_serialized;
    if (!old_version.SerializeToString(&old_serialized) ||
        !new_version.SerializeToString(&new_serialized)) {
        LOG_WARNING("failed to serialize partition version for comparison")
                .tag("old_value", old_version.ShortDebugString())
                .tag("new_value", new_version.ShortDebugString());
        return -1;
    }

    if (old_serialized != new_serialized) {
        LOG_WARNING("partition version content mismatch")
                .tag("old_value", old_version.ShortDebugString())
                .tag("new_value", new_version.ShortDebugString());
        return 1;
    }

    *exists = true;
    return 0;
}

int MigrateValidator::validate_partition_keys(const CollectedEntities& entities,
                                              ValidationResult* result) {
    LOG_INFO("begin to validate partition keys").tag("count", entities.partitions.size());

    for (const auto& [partition_id, db_table_partition] : entities.partitions) {
        result->total_entities++;
        auto [db_id, table_id, _] = db_table_partition;

        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to create txn for validation").tag("error", err);
            result->error_entities++;
            continue;
        }

        bool exists = false;
        MetaReader meta_reader(instance_id_, txn_kv_.get());
        int ret = validate_partition_version(txn.get(), meta_reader, db_id, table_id, partition_id,
                                             &exists);
        result->apply_validation_result(ret);
        if (!exists) {
            continue;
        }

        ret = validate_partition_index_keys(txn.get(), partition_id, db_id, table_id);
        result->apply_validation_result(ret);
        ret = validate_partition_meta_key(txn.get(), partition_id, db_id, table_id);
        result->apply_validation_result(ret);
    }

    return 0;
}

int MigrateValidator::validate_tablet_index(Transaction* txn, MetaReader& meta_reader,
                                            int64_t tablet_id) {
    AnnotateTag tablet_tag("tablet_id", tablet_id);

    // Read old version key
    std::string old_key = meta_tablet_idx_key({instance_id_, tablet_id});
    std::string old_value;
    TxnErrorCode old_err = txn->get(old_key, &old_value);

    // Read new version key using MetaReader
    TabletIndexPB new_tablet_index;
    TxnErrorCode new_err = meta_reader.get_tablet_index(txn, tablet_id, &new_tablet_index, true);

    bool old_exists = (old_err == TxnErrorCode::TXN_OK);
    bool new_exists = (new_err == TxnErrorCode::TXN_OK);

    if (old_exists != new_exists) {
        LOG_WARNING("tablet index key existence mismatch")
                .tag("old_key", hex(old_key))
                .tag("old_exists", old_exists)
                .tag("new_exists", new_exists);
        return 1;
    }

    if (old_err != TxnErrorCode::TXN_OK && old_err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_WARNING("failed to read old tablet index key").tag("error", old_err);
        return -1;
    }

    if (new_err != TxnErrorCode::TXN_OK && new_err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_WARNING("failed to read new tablet index key").tag("error", new_err);
        return -1;
    }

    if (!old_exists) {
        return 0;
    }

    // Compare values
    TabletIndexPB old_tablet_index;
    if (!old_tablet_index.ParseFromString(old_value)) {
        LOG_WARNING("failed to parse old tablet index");
        return -1;
    }

    std::string old_serialized, new_serialized;
    if (!old_tablet_index.SerializeToString(&old_serialized) ||
        !new_tablet_index.SerializeToString(&new_serialized)) {
        LOG_WARNING("failed to serialize tablet index for comparison");
        return -1;
    }

    if (old_serialized != new_serialized) {
        LOG_WARNING("tablet index content mismatch")
                .tag("old_size", old_serialized.size())
                .tag("new_size", new_serialized.size());
        return 1;
    }

    int64_t db_id = new_tablet_index.db_id();
    int64_t table_id = new_tablet_index.table_id();
    int64_t index_id = new_tablet_index.index_id();
    int64_t partition_id = new_tablet_index.partition_id();
    std::string inverted_index_key = versioned::tablet_inverted_index_key(
            {instance_id_, db_id, table_id, index_id, partition_id, tablet_id});
    std::string value;
    TxnErrorCode err = txn->get(inverted_index_key, &value);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_WARNING("tablet inverted index key should exist after migration")
                .tag("key", hex(inverted_index_key));
        return 1;
    } else if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to read tablet inverted index key").tag("error", err);
        return -1;
    }

    return 0;
}

int MigrateValidator::validate_tablet_meta(Transaction* txn, MetaReader& meta_reader,
                                           int64_t table_id, int64_t index_id, int64_t partition_id,
                                           int64_t tablet_id) {
    // Read old version key
    std::string old_key =
            meta_tablet_key({instance_id_, table_id, index_id, partition_id, tablet_id});
    std::string old_value;
    TxnErrorCode old_err = txn->get(old_key, &old_value);

    // Read new version key using MetaReader
    doris::TabletMetaCloudPB new_tablet_meta;
    Versionstamp new_versionstamp;
    TxnErrorCode new_err =
            meta_reader.get_tablet_meta(txn, tablet_id, &new_tablet_meta, &new_versionstamp, true);

    bool old_exists = (old_err == TxnErrorCode::TXN_OK);
    bool new_exists = (new_err == TxnErrorCode::TXN_OK);

    if (old_exists != new_exists) {
        LOG_WARNING("tablet meta key existence mismatch")
                .tag("tablet_id", tablet_id)
                .tag("old_key", hex(old_key))
                .tag("old_exists", old_exists)
                .tag("new_exists", new_exists);
        return 1;
    }

    if (old_err != TxnErrorCode::TXN_OK && old_err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_WARNING("failed to read old tablet meta key").tag("error", old_err);
        return -1;
    }

    if (new_err != TxnErrorCode::TXN_OK && new_err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_WARNING("failed to read new tablet meta key").tag("error", new_err);
        return -1;
    }

    if (!old_exists) {
        return 0;
    }

    // Compare values
    doris::TabletMetaCloudPB old_tablet_meta;
    if (!old_tablet_meta.ParseFromString(old_value)) {
        LOG_WARNING("failed to parse old tablet meta").tag("tablet_id", tablet_id);
        return -1;
    }

    std::string old_serialized, new_serialized;
    if (!old_tablet_meta.SerializeToString(&old_serialized) ||
        !new_tablet_meta.SerializeToString(&new_serialized)) {
        LOG_WARNING("failed to serialize tablet meta for comparison");
        return -1;
    }

    if (old_serialized != new_serialized) {
        LOG_WARNING("tablet meta content mismatch")
                .tag("tablet_id", tablet_id)
                .tag("old_size", old_serialized.size())
                .tag("new_size", new_serialized.size());
        return 1;
    }

    return 0;
}

int MigrateValidator::validate_tablet_keys(const CollectedEntities& entities,
                                           ValidationResult* result) {
    LOG_INFO("begin to validate tablet keys").tag("count", entities.tablets.size());

    for (const auto& [tablet_id, table_index_partition_tablet] : entities.tablets) {
        result->total_entities++;
        auto [table_id, index_id, partition_id, _] = table_index_partition_tablet;

        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to create txn for validation").tag("error", err);
            result->error_entities++;
            continue;
        }

        MetaReader meta_reader(instance_id_, txn_kv_.get());
        int ret = validate_tablet_meta(txn.get(), meta_reader, table_id, index_id, partition_id,
                                       tablet_id);
        result->apply_validation_result(ret);
        ret = validate_tablet_index(txn.get(), meta_reader, tablet_id);
        result->apply_validation_result(ret);
        ret = validate_tablet_stats(txn.get(), meta_reader, tablet_id);
        result->apply_validation_result(ret);
    }

    return 0;
}

int MigrateValidator::validate_index_schema(Transaction* txn, MetaReader& meta_reader,
                                            int64_t index_id, int64_t schema_version,
                                            bool* exists) {
    AnnotateTag index_tag("index_id", index_id);
    AnnotateTag schema_version_tag("schema_version", schema_version);

    // Read old version key using blob_get
    std::string old_key = meta_schema_key({instance_id_, index_id, schema_version});
    ValueBuf old_value_buf;
    doris::TabletSchemaCloudPB old_schema;
    TxnErrorCode old_err = blob_get(txn, old_key, &old_value_buf);
    bool old_exists = (old_err == TxnErrorCode::TXN_OK);
    if (old_exists && !old_value_buf.to_pb(&old_schema)) {
        LOG_WARNING("failed to parse old tablet schema").tag("key", hex(old_key));
        return -1;
    }

    // Read new version key using MetaReader
    doris::TabletSchemaCloudPB new_schema;
    TxnErrorCode new_err =
            meta_reader.get_tablet_schema(txn, index_id, schema_version, &new_schema, true);
    bool new_exists = (new_err == TxnErrorCode::TXN_OK);

    if (old_exists != new_exists) {
        LOG_WARNING("tablet schema existence mismatch")
                .tag("old_key", hex(old_key))
                .tag("old_exists", old_exists)
                .tag("new_exists", new_exists);
        return 1;
    }

    if (old_err != TxnErrorCode::TXN_OK && old_err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_WARNING("failed to read old tablet schema").tag("error", old_err);
        return -1;
    }

    if (new_err != TxnErrorCode::TXN_OK && new_err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_WARNING("failed to read new tablet schema").tag("error", new_err);
        return -1;
    }

    if (!old_exists) {
        *exists = false;
        return 0;
    }

    // Compare values
    std::string old_serialized, new_serialized;
    if (!old_schema.SerializeToString(&old_serialized) ||
        !new_schema.SerializeToString(&new_serialized)) {
        LOG_WARNING("failed to serialize tablet schema for comparison");
        return -1;
    }

    if (old_serialized != new_serialized) {
        LOG_WARNING("tablet schema content mismatch")
                .tag("old_size", old_serialized.size())
                .tag("new_size", new_serialized.size());
        return 1;
    }

    *exists = true;
    return 0;
}

int MigrateValidator::validate_tablet_schema_keys(const CollectedEntities& entities,
                                                  ValidationResult* result) {
    LOG_INFO("begin to validate tablet schema keys").tag("count", entities.schemas.size());

    std::set<int64_t> existing_indexes;
    for (const auto& [index_id, schema_version] : entities.schemas) {
        result->total_entities++;

        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to create txn for validation").tag("error", err);
            result->error_entities++;
            continue;
        }

        bool exists = false;
        MetaReader meta_reader(instance_id_, txn_kv_.get());
        int ret = validate_index_schema(txn.get(), meta_reader, index_id, schema_version, &exists);
        result->apply_validation_result(ret);
        if (exists && !existing_indexes.contains(index_id)) {
            existing_indexes.insert(index_id);
            auto it = entities.indexes.find(index_id);
            if (it == entities.indexes.end()) {
                // Index not found, skip
                continue;
            }

            auto [db_id, table_id, _] = it->second;
            ret = validate_index_index_keys(txn.get(), index_id, db_id, table_id);
            result->apply_validation_result(ret);

            ret = validate_index_meta_key(txn.get(), index_id, db_id, table_id);
            result->apply_validation_result(ret);
        }
    }

    return 0;
}

int MigrateValidator::validate_rowset_metas(Transaction* txn, MetaReader& meta_reader,
                                            int64_t tablet_id) {
    // Get rowsets from old version (0x01)
    std::string begin_key = meta_rowset_key({instance_id_, tablet_id, 0});
    std::string end_key = meta_rowset_key({instance_id_, tablet_id, INT64_MAX});
    FullRangeGetOptions opts;
    opts.snapshot = true;
    opts.prefetch = true;
    auto iter = txn->full_range_get(begin_key, end_key, opts);

    std::map<int64_t, doris::RowsetMetaCloudPB> old_rowsets;
    for (auto kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
        auto&& [key, value] = *kvp;
        doris::RowsetMetaCloudPB rowset_meta;
        if (!rowset_meta.ParseFromArray(value.data(), value.size())) {
            LOG_WARNING("failed to parse old rowset meta").tag("tablet_id", tablet_id);
            return -1;
        }
        old_rowsets[rowset_meta.end_version()] = rowset_meta;
    }
    if (!iter->is_valid()) {
        LOG_WARNING("failed to iterate old rowset metas")
                .tag("tablet_id", tablet_id)
                .tag("error", iter->error_code());
        return -1;
    }

    // Get rowsets from new version (0x03) using MetaReader
    std::vector<doris::RowsetMetaCloudPB> new_rowset_metas;
    TxnErrorCode err = meta_reader.get_rowset_metas(
            txn, tablet_id, 0, std::numeric_limits<int64_t>::max(), &new_rowset_metas, true);
    if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_WARNING("failed to read new rowset metas")
                .tag("tablet_id", tablet_id)
                .tag("error", err);
        return -1;
    }

    std::map<int64_t, doris::RowsetMetaCloudPB> new_rowsets;
    for (auto&& rowset_meta : new_rowset_metas) {
        new_rowsets[rowset_meta.end_version()] = rowset_meta;
    }

    // Check if all old rowsets are covered by new rowsets
    for (auto&& [version, old_rowset] : old_rowsets) {
        auto it = new_rowsets.find(version);
        if (it == new_rowsets.end()) {
            // Not found in new rowsets
            LOG_WARNING("rowset meta missing in versioned space")
                    .tag("tablet_id", tablet_id)
                    .tag("version", version)
                    .tag("rowset_id", old_rowset.rowset_id_v2());
            return 1; // inconsistent
        }

        std::string old_serialized, new_serialized;
        if (!old_rowset.SerializeToString(&old_serialized) ||
            !it->second.SerializeToString(&new_serialized)) {
            LOG_WARNING("failed to serialize rowset meta for comparison")
                    .tag("tablet_id", tablet_id)
                    .tag("version", version);
            return -1;
        }

        if (old_serialized != new_serialized) {
            LOG_WARNING("rowset meta content mismatch")
                    .tag("tablet_id", tablet_id)
                    .tag("version", version)
                    .tag("old_rowset", old_rowset.ShortDebugString())
                    .tag("new_rowset", it->second.ShortDebugString());
            return 1; // inconsistent
        }
    }

    return 0;
}

int MigrateValidator::validate_rowset_meta_keys(const CollectedEntities& entities,
                                                ValidationResult* result) {
    LOG_INFO("begin to validate rowset meta keys").tag("count", entities.tablets.size());

    for (const auto& [tablet_id, _] : entities.tablets) {
        result->total_entities++;

        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to create txn for validation").tag("error", err);
            result->error_entities++;
            continue;
        }

        MetaReader meta_reader(instance_id_, txn_kv_.get());
        int ret = validate_rowset_metas(txn.get(), meta_reader, tablet_id);
        result->apply_validation_result(ret);
    }

    return 0;
}

int MigrateValidator::validate_tablet_stats(Transaction* txn, MetaReader& meta_reader,
                                            int64_t tablet_id) {
    // Get old version stats (0x01)
    TabletStatsPB old_stats;
    TabletStats old_detached_stats;

    std::string tablet_idx_key = meta_tablet_idx_key({instance_id_, tablet_id});
    std::string value;
    TabletIndexPB tablet_idx;
    TxnErrorCode err = txn->get(tablet_idx_key, &value);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Tablet was deleted, both should not exist
        return 0;
    } else if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get tablet index for stats validation")
                .tag("tablet_id", tablet_id)
                .tag("error", err);
        return -1;
    } else if (!tablet_idx.ParseFromString(value)) {
        LOG_WARNING("failed to parse tablet index for stats validation")
                .tag("tablet_id", tablet_id);
        return -1;
    }

    tablet_idx.set_tablet_id(tablet_id);
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg;
    internal_get_tablet_stats(code, msg, txn, instance_id_, tablet_idx, old_stats,
                              old_detached_stats);
    bool old_exists = (code == MetaServiceCode::OK);
    if (code != MetaServiceCode::OK && code != MetaServiceCode::TABLET_NOT_FOUND) {
        LOG_WARNING("failed to get old tablet stats").tag("tablet_id", tablet_id).tag("code", code);
        return -1;
    }
    merge_tablet_stats(old_stats, old_detached_stats);

    // Get new version stats (0x03)
    TabletStatsPB new_load_stats, new_compact_stats;
    TxnErrorCode load_err =
            meta_reader.get_tablet_load_stats(txn, tablet_id, &new_load_stats, nullptr, true);
    TxnErrorCode compact_err =
            meta_reader.get_tablet_compact_stats(txn, tablet_id, &new_compact_stats, nullptr, true);

    bool new_load_exists = (load_err == TxnErrorCode::TXN_OK);
    bool new_compact_exists = (compact_err == TxnErrorCode::TXN_OK);
    bool new_exists = new_load_exists || new_compact_exists;

    if (old_exists != new_exists) {
        LOG_WARNING("tablet stats existence mismatch")
                .tag("tablet_id", tablet_id)
                .tag("old_exists", old_exists)
                .tag("new_load_exists", new_load_exists)
                .tag("new_compact_exists", new_compact_exists);
        return 1;
    }

    if (!old_exists) {
        return 0;
    }

    TabletStatsPB new_stats;
    MetaReader::merge_tablet_stats(new_load_stats, new_compact_stats, &new_stats);

    std::string old_serialized, new_serialized;
    if (!old_stats.SerializeToString(&old_serialized) ||
        !new_stats.SerializeToString(&new_serialized)) {
        LOG_WARNING("failed to serialize tablet stats for comparison").tag("tablet_id", tablet_id);
        return -1;
    }

    if (old_serialized != new_serialized) {
        LOG_WARNING("tablet stats content mismatch")
                .tag("tablet_id", tablet_id)
                .tag("old_stats", old_stats.ShortDebugString())
                .tag("new_stats", new_stats.ShortDebugString());
        return 1;
    }

    return 0;
}

int MigrateValidator::validate_partition_index_keys(Transaction* txn, int64_t partition_id,
                                                    int64_t db_id, int64_t table_id) {
    AnnotateTag partition_tag("partition_id", partition_id);
    AnnotateTag table_tag("table_id", table_id);
    AnnotateTag db_tag("db_id", db_id);

    MetaReader meta_reader(instance_id_);

    // Validate partition_index_key
    PartitionIndexPB partition_index;
    TxnErrorCode err = meta_reader.get_partition_index(txn, partition_id, &partition_index, true);

    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_WARNING("partition index key not found in versioned space");
        return 1;
    } else if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to read partition index").tag("error", err);
        return -1;
    }

    // Validate content
    if (partition_index.db_id() != db_id || partition_index.table_id() != table_id) {
        LOG_WARNING("partition index content mismatch")
                .tag("actual_db_id", partition_index.db_id())
                .tag("actual_table_id", partition_index.table_id());
        return 1;
    }

    // Validate partition_inverted_index_key
    std::string inverted_key =
            versioned::partition_inverted_index_key({instance_id_, db_id, table_id, partition_id});
    std::string value;
    err = txn->get(inverted_key, &value);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("partition inverted index key not found").tag("key", hex(inverted_key));
        return 1;
    }

    return 0;
}

int MigrateValidator::validate_partition_meta_key(Transaction* txn, int64_t partition_id,
                                                  int64_t db_id, int64_t table_id) {
    AnnotateTag partition_tag("partition_id", partition_id);
    std::string partition_meta_key = versioned::meta_partition_key({instance_id_, partition_id});
    std::string value;
    Versionstamp value_version;
    TxnErrorCode err = versioned_get(txn, partition_meta_key, &value_version, &value, true);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_WARNING("partition meta key not found in versioned space");
        return 1;
    } else if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to read partition meta").tag("error", err);
        return -1;
    }

    return 0;
}

int MigrateValidator::validate_index_index_keys(Transaction* txn, int64_t index_id, int64_t db_id,
                                                int64_t table_id) {
    MetaReader meta_reader(instance_id_);

    // Validate index_index_key
    IndexIndexPB index_index;
    TxnErrorCode err = meta_reader.get_index_index(txn, index_id, &index_index, true);

    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_WARNING("index index key not found in versioned space").tag("index_id", index_id);
        return 1;
    } else if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to read index index").tag("index_id", index_id).tag("error", err);
        return -1;
    }

    // Validate content
    if (index_index.db_id() != db_id || index_index.table_id() != table_id) {
        LOG_WARNING("index index content mismatch")
                .tag("index_id", index_id)
                .tag("expected_db_id", db_id)
                .tag("actual_db_id", index_index.db_id())
                .tag("expected_table_id", table_id)
                .tag("actual_table_id", index_index.table_id());
        return 1;
    }

    // Validate index_inverted_key
    std::string inverted_key =
            versioned::index_inverted_key({instance_id_, db_id, table_id, index_id});
    std::string value;
    err = txn->get(inverted_key, &value);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("index inverted key not found")
                .tag("index_id", index_id)
                .tag("key", hex(inverted_key));
        return 1;
    }

    return 0;
}

int MigrateValidator::validate_index_meta_key(Transaction* txn, int64_t index_id, int64_t db_id,
                                              int64_t table_id) {
    AnnotateTag index_tag("index_id", index_id);
    std::string index_meta_key = versioned::meta_index_key({instance_id_, index_id});
    std::string value;
    Versionstamp value_version;
    TxnErrorCode err = versioned_get(txn, index_meta_key, &value_version, &value, true);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_WARNING("index meta key not found in versioned space");
        return 1;
    } else if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to read index meta").tag("error", err);
        return -1;
    }

    return 0;
}

} // namespace selectdb
