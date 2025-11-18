#include "recycler/snapshot_chain_compactor.h"

#include <gen_cpp/olap_file.pb.h>

#include <chrono>
#include <ranges>
#include <string_view>
#include <thread>

#include "common/defer.h"
#include "common/logging.h"
#include "common/stopwatch.h"
#include "common/util.h"
#include "meta-service/meta_service_tablet_stats.h"
#include "meta-store/blob_message.h"
#include "meta-store/codec.h"
#include "meta-store/document_message.h"
#include "meta-store/document_message_get_range.h"
#include "meta-store/keys.h"
#include "meta-store/meta_reader.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
#include "snapshot_manager.h"

using namespace doris::cloud;

namespace selectdb {

// Retry configuration for compaction
static constexpr int MAX_RETRY_TIMES = 5;
static constexpr int RETRY_INTERVAL_MS = 100;

class CompactExecutor {
public:
    CompactExecutor(const std::string& instance_id, const std::string& source_instance_id,
                    const std::string& source_snapshot_id, const Versionstamp snapshot_versionstamp,
                    std::shared_ptr<TxnKv> txn_kv)
            : instance_id_(instance_id),
              source_instance_id_(source_instance_id),
              source_snapshot_id_(source_snapshot_id),
              snapshot_versionstamp_(snapshot_versionstamp),
              txn_kv_(std::move(txn_kv)) {}
    ~CompactExecutor() = default;

    // Compact table version keys
    // Return 0 for success otherwise error.
    int compact_table_version_keys();

    // Compact partition keys
    // Return 0 for success otherwise error.
    int compact_partition_keys();

    // Compact index keys
    // Return 0 for success otherwise error.
    int compact_index_keys();

    // Compact partition keys
    // Return 0 for success otherwise error.
    int compact_tablet_keys();

    // Compact rowset keys
    // Return 0 for success otherwise error.
    int compact_rowset_keys();

private:
    // Compact a single table keys: table_version_key
    // Returns:
    //   0: successfully compacted
    //   1: skipped (already compacted or key no longer exists)
    //  -1: error occurred
    //  -2: transaction conflict (for retry)
    int compact_table_version_key(int64_t table_id);

    // Compact a single partition keys: meta_partition_key, partition_index_key, partition_inverted_index_key, partition_version_key
    // Returns:
    //   0: successfully compacted
    //   1: skipped (already compacted or key no longer exists)
    //  -1: error occurred
    //  -2: transaction conflict (for retry)
    int compact_partition_key(int64_t partition_id);

    int compact_partition_meta_key(Transaction* txn, int64_t partition_id);

    int compact_partition_index_key(Transaction* txn, int64_t partition_id,
                                    PartitionIndexPB& index_pb);

    int compact_partition_inverted_index_key(Transaction* txn, int64_t db_id, int64_t table_id,
                                             int64_t partition_id);

    // Returns:
    //   0: successfully compacted
    //   1: skipped (already compacted or key no longer exists)
    //  -1: error occurred
    //  -2: there are pending txns
    int compact_partition_version_key(Transaction* txn, int64_t partition_id);

    // Compact a single index keys: meta_index_key, index_index_key, index_inverted_key, meta_schema_key
    // Returns:
    //   0: successfully compacted
    //   1: skipped (already compacted or key no longer exists)
    //  -1: error occurred
    //  -2: transaction conflict (for retry)
    int compact_index_key(int64_t index_id);

    int compact_index_meta_key(Transaction* txn, int64_t index_id);

    int compact_index_index_key(Transaction* txn, int64_t index_id, IndexIndexPB& index_pb);

    int compact_index_inverted_index_key(Transaction* txn, int64_t db_id, int64_t table_id,
                                         int64_t index_id);

    int compact_meta_schema_key(Transaction* txn, int64_t index_id, int64_t schema_version);

    // Compact a single tablet keys: meta_tablet_key, tablet_index_key, tablet_inverted_index_key, tablet_load_stats_key, tablet_compact_stats_key
    // Returns:
    //   0: successfully compacted
    //   1: skipped (already compacted or key no longer exists)
    //  -1: error occurred
    //  -2: transaction conflict (for retry)
    int compact_tablet_key(int64_t tablet_id);

    int compact_tablet_meta_key(Transaction* txn, int64_t tablet_id);

    int compact_tablet_index_key(Transaction* txn, int64_t tablet_id, TabletIndexPB& tablet_index);

    int compact_tablet_inverted_index_key(Transaction* txn, int64_t db_id, int64_t table_id,
                                          int64_t index_id, int64_t partition_id,
                                          int64_t tablet_id);

    int compact_tablet_load_stats_key(Transaction* txn, int64_t tablet_id);

    int compact_tablet_compact_stats_key(Transaction* txn, int64_t tablet_id);

    // Compact a single tablet rowset keys: meta_rowset_load_key, meta_rowset_compact_key, meta_delete_bitmap_key
    // Returns:
    //   0: successfully compacted
    //   1: skipped (already compacted or key no longer exists)
    //  -1: error occurred
    //  -2: transaction conflict (for retry)
    int compact_rowset_key(int64_t tablet_id);

    // Returns:
    //   0: successfully compacted
    //   1: skipped (already compacted or key no longer exists)
    //  -1: error occurred
    //  -2: key not found in source instance
    int compact_rowset_load_key(Transaction* txn, int64_t tablet_id, int64_t version);

    int compact_rowset_compact_key(Transaction* txn, int64_t tablet_id, int64_t version);

    // Returns:
    //   0: successfully compacted
    //   1: skipped (already compacted or key no longer exists)
    //  -1: error occurred
    //  -2: key not found in source instance
    int compact_delete_bitmap_key(Transaction* txn, int64_t tablet_id, std::string rowset_id);

    // Get all tables for the instance
    // Return 0 for success otherwise error.
    int get_all_tables(std::vector<int64_t>* table_ids);

    // Get all partitions for the instance
    // Return 0 for success otherwise error.
    int get_all_partitions(std::vector<int64_t>* partition_ids);

    // Get all indexes for the instance
    // Return 0 for success otherwise error.
    int get_all_indexes(std::vector<int64_t>* index_ids);

    // Get all schema versions of the index for the instance
    // Return 0 for success otherwise error.
    int get_all_index_schema_versions(Transaction* txn, int64_t index_id,
                                      std::vector<int64_t>* schema_versions);

    // Get all tablets for the instance
    // Return 0 for success otherwise error.
    int get_all_tablets(std::vector<int64_t>* tablet_ids);

    // Retry wrapper for compaction functions. Retries up to MAX_RETRY_TIMES on TXN_CONFLICT.
    // compact_func must return: 0 (success), 1 (skipped), -1 (error), -2 (TXN_CONFLICT to retry)
    template <typename Fn, typename... Args>
        requires std::is_member_function_pointer_v<Fn> &&
                 std::is_same_v<std::invoke_result_t<Fn, CompactExecutor*, Args...>, int>
    int retry_if_txn_conflict(Fn compact_func, Args&&... args) {
        for (int retry = 0; retry < MAX_RETRY_TIMES; retry++) {
            if (retry > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(RETRY_INTERVAL_MS));
            }

            int res = (this->*compact_func)(std::forward<Args>(args)...);
            if (res == -2) {
                continue; // TXN_CONFLICT
            }
            return res;
        }
        return -1;
    }

    const std::string instance_id_;
    const std::string source_instance_id_;
    const std::string source_snapshot_id_;
    const Versionstamp snapshot_versionstamp_;
    std::shared_ptr<TxnKv> txn_kv_;
};

int CompactExecutor::compact_table_version_keys() {
    LOG_INFO("begin to compact table version keys");

    std::vector<int64_t> table_ids;
    if (get_all_tables(&table_ids) != 0) {
        LOG_WARNING("failed to get all tables to compact table version keys");
        return -1;
    }

    int total_keys = 0;
    int compacted_keys = 0;
    int skipped_keys = 0;
    StopWatch stop_watch;

    DORIS_CLOUD_DEFER {
        LOG_INFO("compact table version keys finished")
                .tag("total", total_keys)
                .tag("compacted", compacted_keys)
                .tag("skipped", skipped_keys)
                .tag("cost(s)", stop_watch.elapsed_seconds());
    };

    for (int64_t table_id : table_ids) {
        total_keys++;

        int result = retry_if_txn_conflict(&CompactExecutor::compact_table_version_key, table_id);
        if (result == 0) {
            compacted_keys++;
        } else if (result == 1) {
            skipped_keys++;
        } else {
            LOG_WARNING("failed to compact table version key").tag("table_id", table_id);
            return -1;
        }
    }
    return 0;
}

int CompactExecutor::compact_partition_keys() {
    LOG_INFO("begin to compact partition keys");

    std::vector<int64_t> partition_ids;
    if (get_all_partitions(&partition_ids) != 0) {
        LOG_WARNING("failed to get all partitions to compact partition keys");
        return -1;
    }

    int total_keys = 0;
    int compacted_keys = 0;
    int skipped_keys = 0;
    StopWatch stop_watch;

    DORIS_CLOUD_DEFER {
        LOG_INFO("compact partition keys finished")
                .tag("total", total_keys)
                .tag("compacted", compacted_keys)
                .tag("skipped", skipped_keys)
                .tag("cost(s)", stop_watch.elapsed_seconds());
    };

    for (int64_t partition_id : partition_ids) {
        total_keys++;

        int result = retry_if_txn_conflict(&CompactExecutor::compact_partition_key, partition_id);
        if (result == 0) {
            compacted_keys++;
        } else if (result == 1) {
            skipped_keys++;
        } else {
            LOG_WARNING("failed to compact partition key").tag("partition_id", partition_id);
            return -1;
        }
    }
    return 0;
}

int CompactExecutor::compact_index_keys() {
    LOG_INFO("begin to compact index keys");

    std::vector<int64_t> index_ids;
    if (get_all_indexes(&index_ids) != 0) {
        LOG_WARNING("failed to get all indexes to compact index keys");
        return -1;
    }

    int total_keys = 0;
    int compacted_keys = 0;
    int skipped_keys = 0;
    StopWatch stop_watch;

    DORIS_CLOUD_DEFER {
        LOG_INFO("compact index keys finished")
                .tag("total", total_keys)
                .tag("compacted", compacted_keys)
                .tag("skipped", skipped_keys)
                .tag("cost(s)", stop_watch.elapsed_seconds());
    };

    for (int64_t index_id : index_ids) {
        total_keys++;

        int result = retry_if_txn_conflict(&CompactExecutor::compact_index_key, index_id);
        if (result == 0) {
            compacted_keys++;
        } else if (result == 1) {
            skipped_keys++;
        } else {
            LOG_WARNING("failed to compact index key").tag("index_id", index_id);
            return -1;
        }
    }
    return 0;
}

int CompactExecutor::compact_tablet_keys() {
    LOG_INFO("begin to compact tablet keys");

    std::vector<int64_t> tablet_ids;
    if (get_all_tablets(&tablet_ids) != 0) {
        LOG_WARNING("failed to get all tablets to compact tablet keys");
        return -1;
    }

    int total_keys = 0;
    int compacted_keys = 0;
    int skipped_keys = 0;
    StopWatch stop_watch;

    DORIS_CLOUD_DEFER {
        LOG_INFO("compact tablet keys finished")
                .tag("total", total_keys)
                .tag("compacted", compacted_keys)
                .tag("skipped", skipped_keys)
                .tag("cost(s)", stop_watch.elapsed_seconds());
    };

    for (int64_t tablet_id : tablet_ids) {
        total_keys++;

        int result = retry_if_txn_conflict(&CompactExecutor::compact_tablet_key, tablet_id);
        if (result == 0) {
            compacted_keys++;
        } else if (result == 1) {
            skipped_keys++;
        } else {
            LOG_WARNING("failed to compact tablet key").tag("tablet_id", tablet_id);
            return -1;
        }
    }
    return 0;
}

int CompactExecutor::compact_rowset_keys() {
    LOG_INFO("begin to compact rowset keys");

    std::vector<int64_t> tablet_ids;
    int res = get_all_tablets(&tablet_ids);
    if (res != 0) {
        LOG_WARNING("failed to get all tablets to compact rowset keys");
        return -1;
    }

    for (int64_t tablet_id : tablet_ids) {
        int result = retry_if_txn_conflict(&CompactExecutor::compact_rowset_key, tablet_id);
        if (result != 0 && result != 1) {
            return -1;
        }
    }
    return 0;
}

int CompactExecutor::compact_table_version_key(int64_t table_id) {
    AnnotateTag table_id_tag("table_id", table_id);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn for compacting table version key").tag("error", err);
        return -1;
    }

    Versionstamp versionstamp;
    MetaReader meta_reader(instance_id_, snapshot_versionstamp_);
    err = meta_reader.get_table_version(txn.get(), table_id, &versionstamp);
    if (err == TxnErrorCode::TXN_OK) {
        // Already compacted, skip
        return 1; // skipped
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Error occurred
        LOG_WARNING("failed to get table version key for compacting table version key")
                .tag("error", err);
        return -1;
    }

    MetaReader source_meta_reader(source_instance_id_, snapshot_versionstamp_);
    err = source_meta_reader.get_table_version(txn.get(), table_id, &versionstamp);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get table version key for compacting table version key")
                .tag("source_instance_id", source_instance_id_)
                .tag("error", err);
        return -1;
    }

    std::string key = versioned::table_version_key({instance_id_, table_id});
    versioned_put(txn.get(), key, versionstamp, "");

    err = txn->commit();
    if (err == TxnErrorCode::TXN_OK) {
        return 0; // success
    } else if (err == TxnErrorCode::TXN_CONFLICT) {
        LOG_WARNING("compact table version key failed due to transaction conflict");
        return -2; // TXN_CONFLICT
    } else {
        LOG_WARNING("failed to commit txn for compacting table version key").tag("error", err);
        return -1;
    }
}

int CompactExecutor::compact_partition_key(int64_t partition_id) {
    AnnotateTag partition_id_tag("partition_id", partition_id);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn for compacting partition key").tag("error", err);
        return -1;
    }

    bool need_commit = false;

    int result = compact_partition_version_key(txn.get(), partition_id);
    if (result < 0) {
        // result = -2 means there are pending txns, should return to avoid data loss
        return -1;
    } else if (result == 0) {
        need_commit = true;
    }

    result = compact_partition_meta_key(txn.get(), partition_id);
    if (result == -1) {
        return -1;
    } else if (result == 0) {
        need_commit = true;
    }

    PartitionIndexPB index_pb;
    result = compact_partition_index_key(txn.get(), partition_id, index_pb);
    if (result == -1) {
        return -1;
    } else if (result == 0) {
        need_commit = true;
    }

    result = compact_partition_inverted_index_key(txn.get(), index_pb.db_id(), index_pb.table_id(),
                                                  partition_id);
    if (result == -1) {
        return -1;
    } else if (result == 0) {
        need_commit = true;
    }

    if (!need_commit) {
        return 1; // skipped
    }

    err = txn->commit();
    if (err == TxnErrorCode::TXN_OK) {
        return 0; // success
    } else if (err == TxnErrorCode::TXN_CONFLICT) {
        LOG_WARNING("compact partition key failed due to transaction conflict");
        return -2; // TXN_CONFLICT
    } else {
        LOG_WARNING("failed to commit txn for compacting partition key").tag("error", err);
        return -1;
    }
}

int CompactExecutor::compact_index_key(int64_t index_id) {
    AnnotateTag index_id_tag("index_id", index_id);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn for compacting index key").tag("error", err);
        return -1;
    }

    bool need_commit = false;

    int result = compact_index_meta_key(txn.get(), index_id);
    if (result == -1) {
        return -1;
    } else if (result == 0) {
        need_commit = true;
    }

    IndexIndexPB index_pb;
    result = compact_index_index_key(txn.get(), index_id, index_pb);
    if (result == -1) {
        return -1;
    } else if (result == 0) {
        need_commit = true;
    }

    result = compact_index_inverted_index_key(txn.get(), index_pb.db_id(), index_pb.table_id(),
                                              index_id);
    if (result == -1) {
        return -1;
    } else if (result == 0) {
        need_commit = true;
    }

    std::vector<int64_t> schema_versions;
    if (get_all_index_schema_versions(txn.get(), index_id, &schema_versions) != 0) {
        LOG_WARNING("failed to get all schema versions for compacting index key");
        return -1;
    }
    for (int64_t schema_version : schema_versions) {
        result = compact_meta_schema_key(txn.get(), index_id, schema_version);
        if (result == -1) {
            return -1;
        } else if (result == 0) {
            need_commit = true;
        }
    }

    if (!need_commit) {
        return 1; // skipped
    }

    err = txn->commit();
    if (err == TxnErrorCode::TXN_OK) {
        return 0; // success
    } else if (err == TxnErrorCode::TXN_CONFLICT) {
        LOG_WARNING("compact index key failed due to transaction conflict");
        return -2; // TXN_CONFLICT
    } else {
        LOG_WARNING("failed to commit txn for compacting index key").tag("error", err);
        return -1;
    }
}

int CompactExecutor::compact_tablet_key(int64_t tablet_id) {
    AnnotateTag tablet_id_tag("tablet_id", tablet_id);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn for compacting tablet key").tag("error", err);
        return -1;
    }

    bool need_commit = false;

    int result = compact_tablet_meta_key(txn.get(), tablet_id);
    if (result == -1) {
        return -1;
    } else if (result == 0) {
        need_commit = true;
    }

    TabletIndexPB index_pb;
    result = compact_tablet_index_key(txn.get(), tablet_id, index_pb);
    if (result == -1) {
        return -1;
    } else if (result == 0) {
        need_commit = true;
    }

    result = compact_tablet_inverted_index_key(txn.get(), index_pb.db_id(), index_pb.table_id(),
                                               index_pb.index_id(), index_pb.partition_id(),
                                               tablet_id);
    if (result == -1) {
        return -1;
    } else if (result == 0) {
        need_commit = true;
    }

    result = compact_tablet_load_stats_key(txn.get(), tablet_id);
    if (result == -1) {
        return -1;
    } else if (result == 0) {
        need_commit = true;
    }

    result = compact_tablet_compact_stats_key(txn.get(), tablet_id);
    if (result == -1) {
        return -1;
    } else if (result == 0) {
        need_commit = true;
    }

    if (!need_commit) {
        return 1; // skipped
    }

    err = txn->commit();
    if (err == TxnErrorCode::TXN_OK) {
        return 0; // success
    } else if (err == TxnErrorCode::TXN_CONFLICT) {
        LOG_WARNING("compact tablet key failed due to transaction conflict");
        return -2; // TXN_CONFLICT
    } else {
        LOG_WARNING("failed to commit txn for compacting tablet key").tag("error", err);
        return -1;
    }
}

int CompactExecutor::compact_rowset_key(int64_t tablet_id) {
    AnnotateTag tablet_id_tag("tablet_id", tablet_id);

    MetaReader source_meta_reader(source_instance_id_, txn_kv_.get(), snapshot_versionstamp_);
    std::vector<doris::RowsetMetaCloudPB> rowset_metas;
    TxnErrorCode err = source_meta_reader.get_rowset_metas(
            tablet_id, 0, std::numeric_limits<int64_t>::max(), &rowset_metas);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get rowset metas for compacting rowset keys")
                .tag("tablet_id", tablet_id)
                .tag("error", err);
        return -1;
    }
    VLOG_DEBUG << "get rowset metas for tablet_id=" << tablet_id
               << ", rowset_num=" << rowset_metas.size();

    doris::TabletMetaCloudPB tablet_meta;
    err = source_meta_reader.get_tablet_meta(tablet_id, &tablet_meta, nullptr);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get tablet meta for compacting rowset keys")
                .tag("tablet_id", tablet_id)
                .tag("error", err);
        return -1;
    }
    bool is_mow = tablet_meta.has_enable_unique_key_merge_on_write() &&
                  tablet_meta.enable_unique_key_merge_on_write();

    std::unique_ptr<Transaction> txn;
    err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn for compact rowset key").tag("error", err);
        return -1;
    }

    int total_keys = 0;
    int compacted_keys = 0;
    int skipped_keys = 0;
    StopWatch stop_watch;

    DORIS_CLOUD_DEFER {
        LOG_INFO("compact rowset key finished")
                .tag("total", total_keys)
                .tag("compacted", compacted_keys)
                .tag("skipped", skipped_keys)
                .tag("cost(s)", stop_watch.elapsed_seconds());
    };

    for (auto& rowset_meta : rowset_metas) {
        total_keys++;

        int result;
        if (rowset_meta.start_version() == rowset_meta.end_version() ||
            rowset_meta.end_version() == 1) {
            result = compact_rowset_load_key(txn.get(), tablet_id, rowset_meta.end_version());
            if (result == -2 && rowset_meta.start_version() == rowset_meta.end_version()) {
                result =
                        compact_rowset_compact_key(txn.get(), tablet_id, rowset_meta.end_version());
            }
        } else {
            result = compact_rowset_compact_key(txn.get(), tablet_id, rowset_meta.end_version());
        }
        if (result == 0) {
            compacted_keys++;
        } else if (result == 1) {
            skipped_keys++;
        } else {
            LOG_WARNING("failed to compact rowset meta")
                    .tag("rowset_id", rowset_meta.rowset_id_v2())
                    .tag("start_version", rowset_meta.start_version())
                    .tag("end_version", rowset_meta.end_version());
            return -1;
        }

        if (is_mow) {
            result = compact_delete_bitmap_key(txn.get(), tablet_id, rowset_meta.rowset_id_v2());
            if (result == 0) {
                compacted_keys++;
            } else if (result == 1) {
                skipped_keys++;
            } else if (result == -1) {
                LOG_WARNING("failed to compact delete bitmap")
                        .tag("rowset_id", rowset_meta.rowset_id_v2());
                return -1;
            }
        }
    }

    err = txn->commit();
    if (err == TxnErrorCode::TXN_OK) {
        return 0; // success
    } else if (err == TxnErrorCode::TXN_CONFLICT) {
        LOG_WARNING("compact rowset key failed due to transaction conflict");
        return -2; // TXN_CONFLICT
    } else {
        LOG_WARNING("failed to commit txn for compacting rowset key").tag("error", err);
        return -1;
    }
}

int CompactExecutor::compact_partition_meta_key(Transaction* txn, int64_t partition_id) {
    std::string key = versioned::meta_partition_key({instance_id_, partition_id});
    std::string value;
    Versionstamp versionstamp;
    TxnErrorCode err = versioned_get(txn, key, snapshot_versionstamp_, &versionstamp, &value);
    if (err == TxnErrorCode::TXN_OK) {
        // Already compacted, skip
        return 1; // skipped
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Error occurred
        LOG_WARNING("failed to get meta partition key for compacting meta partition key")
                .tag("error", err);
        return -1;
    }

    std::string source_key = versioned::meta_partition_key({source_instance_id_, partition_id});
    err = versioned_get(txn, source_key, snapshot_versionstamp_, &versionstamp, &value);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get meta partition key for compacting meta partition key")
                .tag("error", err);
        return -1;
    }

    versioned_put(txn, key, versionstamp, value);
    return 0;
}

int CompactExecutor::compact_partition_index_key(Transaction* txn, int64_t partition_id,
                                                 PartitionIndexPB& index_pb) {
    MetaReader meta_reader(instance_id_, snapshot_versionstamp_);
    TxnErrorCode err = meta_reader.get_partition_index(txn, partition_id, &index_pb);
    if (err == TxnErrorCode::TXN_OK) {
        // Already compacted, skip
        return 1; // skipped
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Error occurred
        LOG_WARNING("failed to get partition index key for compacting partition index key")
                .tag("error", err);
        return -1;
    }

    MetaReader source_meta_reader(source_instance_id_, snapshot_versionstamp_);
    err = source_meta_reader.get_partition_index(txn, partition_id, &index_pb);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get partition index key for compacting partition key")
                .tag("error", err);
        return -1;
    }
    std::string index_value;
    if (!index_pb.SerializeToString(&index_value)) {
        LOG_WARNING("failed to serialize versioned partition index");
        return -1;
    }

    std::string key = versioned::partition_index_key({instance_id_, partition_id});
    txn->put(key, index_value);
    return 0;
}

int CompactExecutor::compact_partition_inverted_index_key(Transaction* txn, int64_t db_id,
                                                          int64_t table_id, int64_t partition_id) {
    std::string key =
            versioned::partition_inverted_index_key({instance_id_, db_id, table_id, partition_id});
    std::string value;
    TxnErrorCode err = txn->get(key, &value);
    if (err == TxnErrorCode::TXN_OK) {
        // Already compacted, skip
        return 1; // skipped
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Error occurred
        LOG_WARNING(
                "failed to get partition inverted index key for compacting partition inverted "
                "index key")
                .tag("error", err);
        return -1;
    }

    txn->put(key, "");
    return 0;
}

int CompactExecutor::compact_partition_version_key(Transaction* txn, int64_t partition_id) {
    MetaReader meta_reader(instance_id_, snapshot_versionstamp_);
    VersionPB version_pb;
    Versionstamp versionstamp;
    TxnErrorCode err =
            meta_reader.get_partition_version(txn, partition_id, &version_pb, &versionstamp);
    if (err == TxnErrorCode::TXN_OK) {
        // Already compacted, skip
        return 1; // skipped
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Error occurred
        LOG_WARNING("failed to get partition version key for compacting partition version key")
                .tag("error", err);
        return -1;
    }

    MetaReader source_meta_reader(source_instance_id_, snapshot_versionstamp_);
    err = source_meta_reader.get_partition_version(txn, partition_id, &version_pb, &versionstamp);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        VLOG_DEBUG << "partition version key not found for compacting partition key, "
                      "source_instance_id="
                   << source_instance_id_ << ", partition_id=" << partition_id;
        return 1;
    } else if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get partition version key for compacting partition version key")
                .tag("error", err);
        return -1;
    }
    if (version_pb.pending_txn_ids_size() > 0) {
        LOG_WARNING("partition has pending txns, cannot compact partition version key")
                .tag("source_instance_id", source_instance_id_)
                .tag("pending_txns_size", version_pb.pending_txn_ids_size())
                .tag("error", err);
        return -2;
    }
    std::string value;
    if (!version_pb.SerializeToString(&value)) {
        LOG_WARNING("failed to serialize versioned partition version");
        return -1;
    }

    std::string key = versioned::partition_version_key({instance_id_, partition_id});
    versioned_put(txn, key, versionstamp, value);
    return 0;
}

int CompactExecutor::compact_index_meta_key(Transaction* txn, int64_t index_id) {
    std::string meta_key = versioned::meta_index_key({instance_id_, index_id});
    std::string value;
    Versionstamp versionstamp;
    TxnErrorCode err = versioned_get(txn, meta_key, snapshot_versionstamp_, &versionstamp, &value);
    if (err == TxnErrorCode::TXN_OK) {
        // Already compacted, skip
        return 1; // skipped
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Error occurred
        LOG_WARNING("failed to get meta index key for compacting index key").tag("error", err);
        return -1;
    }

    std::string source_meta_key = versioned::meta_index_key({source_instance_id_, index_id});
    err = versioned_get(txn, source_meta_key, snapshot_versionstamp_, &versionstamp, &value);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get meta index key for compacting index key")
                .tag("source_instance_id", source_instance_id_)
                .tag("error", err);
        return -1;
    }

    versioned_put(txn, meta_key, versionstamp, value);
    return 0;
}

int CompactExecutor::compact_index_index_key(Transaction* txn, int64_t index_id,
                                             IndexIndexPB& index_pb) {
    MetaReader meta_reader(instance_id_, snapshot_versionstamp_);
    TxnErrorCode err = meta_reader.get_index_index(txn, index_id, &index_pb);
    if (err == TxnErrorCode::TXN_OK) {
        // Already compacted, skip
        return 1; // skipped
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Error occurred
        LOG_WARNING("failed to get index index key for compacting index key").tag("error", err);
        return -1;
    }

    MetaReader source_meta_reader(source_instance_id_, snapshot_versionstamp_);
    err = source_meta_reader.get_index_index(txn, index_id, &index_pb);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get index index key for compacting index key")
                .tag("source_instance_id", source_instance_id_)
                .tag("error", err);
        return -1;
    }
    std::string index_value;
    if (!index_pb.SerializeToString(&index_value)) {
        LOG_WARNING("failed to serialize versioned index index");
        return -1;
    }

    std::string index_key = versioned::index_index_key({instance_id_, index_id});
    txn->put(index_key, index_value);
    return 0;
}

int CompactExecutor::compact_index_inverted_index_key(Transaction* txn, int64_t db_id,
                                                      int64_t table_id, int64_t index_id) {
    std::string key = versioned::index_inverted_key({instance_id_, db_id, table_id, index_id});
    std::string value;
    TxnErrorCode err = txn->get(key, &value);
    if (err == TxnErrorCode::TXN_OK) {
        // Already compacted, skip
        return 1; // skipped
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Error occurred
        LOG_WARNING("failed to get index inverted index key for compacting index key")
                .tag("error", err);
        return -1;
    }

    txn->put(key, "");
    return 0;
}

int CompactExecutor::compact_tablet_meta_key(Transaction* txn, int64_t tablet_id) {
    MetaReader meta_reader(instance_id_, snapshot_versionstamp_);
    doris::TabletMetaCloudPB tablet_meta;
    Versionstamp meta_version;
    TxnErrorCode err = meta_reader.get_tablet_meta(txn, tablet_id, &tablet_meta, &meta_version);
    if (err == TxnErrorCode::TXN_OK) {
        // Already compacted, skip
        return 1; // skipped
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Error occurred
        LOG_WARNING("failed to get meta tablet key for compacting tablet key").tag("error", err);
        return -1;
    }

    MetaReader source_meta_reader(source_instance_id_, snapshot_versionstamp_);
    err = source_meta_reader.get_tablet_meta(txn, tablet_id, &tablet_meta, &meta_version);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get tablet meta key for compacting tablet key")
                .tag("source_instance_id", source_instance_id_)
                .tag("error", err);
        return -1;
    }

    std::string meta_key = versioned::meta_tablet_key({instance_id_, tablet_id});
    if (!versioned::document_put(txn, meta_key, meta_version, std::move(tablet_meta))) {
        LOG_WARNING("failed to serialize versioned tablet meta");
        return -1;
    }
    return 0;
}

int CompactExecutor::compact_tablet_index_key(Transaction* txn, int64_t tablet_id,
                                              TabletIndexPB& tablet_index) {
    MetaReader meta_reader(instance_id_, snapshot_versionstamp_);
    TxnErrorCode err = meta_reader.get_tablet_index(txn, tablet_id, &tablet_index);
    if (err == TxnErrorCode::TXN_OK) {
        // Already compacted, skip
        return 1; // skipped
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Error occurred
        LOG_WARNING("failed to get tablet index key for compacting tablet key").tag("error", err);
        return -1;
    }

    MetaReader source_meta_reader(source_instance_id_, snapshot_versionstamp_);
    err = source_meta_reader.get_tablet_index(txn, tablet_id, &tablet_index);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get tablet index key for compacting tablet key")
                .tag("source_instance_id", source_instance_id_)
                .tag("error", err);
        return -1;
    }
    std::string index_value;
    if (!tablet_index.SerializeToString(&index_value)) {
        LOG_WARNING("failed to serialize versioned tablet index");
        return -1;
    }

    std::string index_key = versioned::tablet_index_key({instance_id_, tablet_id});
    txn->put(index_key, index_value);
    return 0;
}

int CompactExecutor::compact_tablet_inverted_index_key(Transaction* txn, int64_t db_id,
                                                       int64_t table_id, int64_t index_id,
                                                       int64_t partition_id, int64_t tablet_id) {
    std::string key = versioned::tablet_inverted_index_key(
            {instance_id_, db_id, table_id, index_id, partition_id, tablet_id});
    std::string value;
    TxnErrorCode err = txn->get(key, &value);
    if (err == TxnErrorCode::TXN_OK) {
        // Already compacted, skip
        return 1; // skipped
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Error occurred
        LOG_WARNING("failed to get tablet index inverted index key for compacting tablet key")
                .tag("error", err);
        return -1;
    }

    txn->put(key, "");
    return 0;
}

int CompactExecutor::compact_tablet_load_stats_key(Transaction* txn, int64_t tablet_id) {
    MetaReader meta_reader(instance_id_, snapshot_versionstamp_);
    TabletStatsPB tablet_stats;
    Versionstamp versionstamp;
    TxnErrorCode err =
            meta_reader.get_tablet_load_stats(txn, tablet_id, &tablet_stats, &versionstamp);
    if (err == TxnErrorCode::TXN_OK) {
        // Already compacted, skip
        return 1; // skipped
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Error occurred
        LOG_WARNING("failed to get tablet load stats for compacting tablet load stats key")
                .tag("error", err);
        return -1;
    }

    MetaReader source_meta_reader(source_instance_id_, snapshot_versionstamp_);
    err = source_meta_reader.get_tablet_load_stats(txn, tablet_id, &tablet_stats, &versionstamp);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        VLOG_DEBUG << "tablet load stats not found, source_instance_id=" << source_instance_id_
                   << ", tablet_id=" << tablet_id;
        return 1;
    } else if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get tablet load stats for compacting tablet load stats key")
                .tag("source_instance_id", source_instance_id_)
                .tag("error", err);
        return -1;
    }

    std::string stats_key = versioned::tablet_load_stats_key({instance_id_, tablet_id});
    if (!versioned::document_put(txn, stats_key, versionstamp, std::move(tablet_stats))) {
        LOG_WARNING("failed to serialize versioned tablet stats");
        return -1;
    }
    return 0;
}

int CompactExecutor::compact_tablet_compact_stats_key(Transaction* txn, int64_t tablet_id) {
    MetaReader meta_reader(instance_id_, snapshot_versionstamp_);
    TabletStatsPB tablet_stats;
    Versionstamp versionstamp;
    TxnErrorCode err =
            meta_reader.get_tablet_compact_stats(txn, tablet_id, &tablet_stats, &versionstamp);
    if (err == TxnErrorCode::TXN_OK) {
        // Already compacted, skip
        return 1; // skipped
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Error occurred
        LOG_WARNING("failed to get tablet compact stats for compacting tablet compact stats key")
                .tag("error", err);
        return -1;
    }

    MetaReader source_meta_reader(source_instance_id_, snapshot_versionstamp_);
    err = source_meta_reader.get_tablet_compact_stats(txn, tablet_id, &tablet_stats, &versionstamp);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        VLOG_DEBUG << "tablet compact stats not found, source_instance_id=" << source_instance_id_
                   << ", tablet_id=" << tablet_id;
        return 1;
    } else if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get key for compacting tablet compact stats key")
                .tag("source_instance_id", source_instance_id_)
                .tag("error", err);
        return -1;
    }

    std::string stats_key = versioned::tablet_compact_stats_key({instance_id_, tablet_id});
    if (!versioned::document_put(txn, stats_key, versionstamp, std::move(tablet_stats))) {
        LOG_WARNING("failed to serialize versioned tablet stats");
        return -1;
    }
    return 0;
}

int CompactExecutor::compact_meta_schema_key(Transaction* txn, int64_t index_id,
                                             int64_t schema_version) {
    AnnotateTag schema_version_tag("schema_version", schema_version);

    MetaReader meta_reader(instance_id_, snapshot_versionstamp_);
    doris::TabletSchemaCloudPB tablet_schema;
    TxnErrorCode err = meta_reader.get_tablet_schema(txn, index_id, schema_version, &tablet_schema);
    if (err == TxnErrorCode::TXN_OK) {
        // Already compacted, skip
        return 1; // skipped
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Error occurred
        LOG_WARNING("failed to get meta schema key for compacting meta schema key")
                .tag("error", err);
        return -1;
    }

    MetaReader source_meta_reader(source_instance_id_, snapshot_versionstamp_);
    doris::TabletSchemaCloudPB source_tablet_schema;
    err = source_meta_reader.get_tablet_schema(txn, index_id, schema_version,
                                               &source_tablet_schema);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get meta schema key for compacting meta schema key")
                .tag("source_instance_id", source_instance_id_)
                .tag("error", err);
        return -1;
    }

    std::string schema_key = versioned::meta_schema_key({instance_id_, index_id, schema_version});
    if (!document_put(txn, schema_key, std::move(source_tablet_schema))) {
        LOG_WARNING("failed to serialize versioned tablet schema");
        return -1;
    }
    return 0;
}

int CompactExecutor::compact_rowset_load_key(Transaction* txn, int64_t tablet_id, int64_t version) {
    AnnotateTag version_id_tag("version", version);

    MetaReader meta_reader(instance_id_, snapshot_versionstamp_);
    doris::RowsetMetaCloudPB rowset_meta;
    Versionstamp versionstamp;
    TxnErrorCode err =
            meta_reader.get_load_rowset_meta(txn, tablet_id, version, &rowset_meta, &versionstamp);
    if (err == TxnErrorCode::TXN_OK) {
        // Already compacted, skip
        return 1; // skipped
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Error occurred
        LOG_WARNING("failed to get rowset load key for compacting rowset load key")
                .tag("error", err);
        return -1;
    }

    MetaReader source_meta_reader(source_instance_id_, snapshot_versionstamp_);
    err = source_meta_reader.get_load_rowset_meta(txn, tablet_id, version, &rowset_meta,
                                                  &versionstamp);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_WARNING("rowset load key not found")
                .tag("source_instance_id,", source_instance_id_)
                .tag("tablet_id", tablet_id)
                .tag("version", version);
        return -2;
    } else if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get key for compacting rowset load key")
                .tag("source_instance_id", source_instance_id_)
                .tag("error", err);
        return -1;
    }

    if (!rowset_meta.has_reference_instance_id()) {
        rowset_meta.set_reference_instance_id(source_instance_id_);
    }
    std::string load_key = versioned::meta_rowset_load_key({instance_id_, tablet_id, version});
    if (!versioned::document_put(txn, load_key, versionstamp, std::move(rowset_meta))) {
        LOG_WARNING("failed to serialize versioned rowset meta");
        return -1;
    }

    auto reference_instance_id = rowset_meta.reference_instance_id();
    std::string rowset_ref_count_key = versioned::data_rowset_ref_count_key(
            {reference_instance_id, tablet_id, rowset_meta.rowset_id_v2()});
    LOG_INFO("add rowset ref count key")
            .tag("reference_instance_id", reference_instance_id)
            .tag("rowset_id", rowset_meta.rowset_id_v2())
            .tag("key", hex(rowset_ref_count_key));
    txn->atomic_add(rowset_ref_count_key, 1);
    return 0;
}

int CompactExecutor::compact_rowset_compact_key(Transaction* txn, int64_t tablet_id,
                                                int64_t version) {
    AnnotateTag version_id_tag("version", version);

    MetaReader meta_reader(instance_id_, snapshot_versionstamp_);
    doris::RowsetMetaCloudPB rowset_meta;
    Versionstamp versionstamp;
    TxnErrorCode err = meta_reader.get_compact_rowset_meta(txn, tablet_id, version, &rowset_meta,
                                                           &versionstamp);
    if (err == TxnErrorCode::TXN_OK) {
        // Already compacted, skip
        return 1; // skipped
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Error occurred
        LOG_WARNING("failed to get rowset compact key for compacting rowset compact key")
                .tag("error", err);
        return -1;
    }

    MetaReader source_meta_reader(source_instance_id_, snapshot_versionstamp_);
    err = source_meta_reader.get_compact_rowset_meta(txn, tablet_id, version, &rowset_meta,
                                                     &versionstamp);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get key for compacting rowset compact key")
                .tag("source_instance_id", source_instance_id_)
                .tag("error", err);
        return -1;
    }

    if (!rowset_meta.has_reference_instance_id()) {
        rowset_meta.set_reference_instance_id(source_instance_id_);
    }
    std::string compact_key =
            versioned::meta_rowset_compact_key({instance_id_, tablet_id, version});
    if (!versioned::document_put(txn, compact_key, versionstamp, std::move(rowset_meta))) {
        LOG_WARNING("failed to serialize versioned rowset meta");
        return -1;
    }

    auto reference_instance_id = rowset_meta.reference_instance_id();
    std::string rowset_ref_count_key = versioned::data_rowset_ref_count_key(
            {reference_instance_id, tablet_id, rowset_meta.rowset_id_v2()});
    LOG_INFO("add rowset ref count key")
            .tag("reference_instance_id", reference_instance_id)
            .tag("rowset_id", rowset_meta.rowset_id_v2())
            .tag("key", hex(rowset_ref_count_key));
    txn->atomic_add(rowset_ref_count_key, 1);
    return 0;
}

int CompactExecutor::compact_delete_bitmap_key(Transaction* txn, int64_t tablet_id,
                                               std::string rowset_id) {
    AnnotateTag rowset_id_tag("rowset_id", rowset_id);

    std::string key = versioned::meta_delete_bitmap_key({instance_id_, tablet_id, rowset_id});
    ValueBuf val_buf;
    TxnErrorCode err = doris::cloud::blob_get(txn, key, &val_buf);
    if (err == TxnErrorCode::TXN_OK) {
        // Already compacted, skip
        return 1; // skipped
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Error occurred
        LOG_WARNING("failed to get delete bitmap key for compacting delete bitmap key")
                .tag("error", err);
        return -1;
    }

    std::string source_key =
            versioned::meta_delete_bitmap_key({source_instance_id_, tablet_id, rowset_id});
    err = doris::cloud::blob_get(txn, source_key, &val_buf);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        return -2; // skipped
    } else if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get delete bitmap key for compacting delete bitmap key")
                .tag("source_instance_id", source_instance_id_)
                .tag("error", err);
        return -1;
    }

    DeleteBitmapStoragePB delete_bitmap;
    if (!val_buf.to_pb(&delete_bitmap)) {
        LOG_WARNING("failed to serialize delete bitmap");
        return -1;
    }
    std::string val;
    if (!delete_bitmap.SerializeToString(&val)) {
        LOG_WARNING("failed to serialize delete bitmap");
        return -1;
    }
    doris::cloud::blob_put(txn, key, val, 0);
    return 0;
}

int CompactExecutor::get_all_tables(std::vector<int64_t>* table_ids) {
    table_ids->clear();

    std::string begin_key = versioned::table_version_key({source_instance_id_, 0});
    std::string end_key = versioned::table_version_key({source_instance_id_, INT64_MAX});

    FullRangeGetOptions opts;
    opts.snapshot = true;
    opts.prefetch = true;
    opts.txn_kv = txn_kv_;
    auto iter = txn_kv_->full_range_get(begin_key, end_key, opts);

    int64_t last_table_id = -1;
    for (auto kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
        auto&& [key, _] = *kvp;

        int64_t table_id = -1;
        Versionstamp versionstamp;
        std::string_view key_view(key);
        if (!versioned::decode_table_version_key(&key_view, &table_id, &versionstamp)) {
            LOG_WARNING("failed to decode table version key").tag("key", hex(key));
            return -1;
        }

        if (table_id == last_table_id || versionstamp > snapshot_versionstamp_) {
            continue;
        }
        table_ids->push_back(table_id);
        last_table_id = table_id;
    }

    if (!iter->is_valid()) {
        LOG_ERROR("failed to get all tables").tag("error_code", iter->error_code());
        return -1;
    }

    return 0;
}

int CompactExecutor::get_all_partitions(std::vector<int64_t>* partition_ids) {
    partition_ids->clear();

    std::string begin_key = versioned::meta_partition_key({source_instance_id_, 0});
    std::string end_key = versioned::meta_partition_key({source_instance_id_, INT64_MAX});

    FullRangeGetOptions opts;
    opts.snapshot = true;
    opts.prefetch = true;
    opts.txn_kv = txn_kv_;
    auto iter = txn_kv_->full_range_get(begin_key, end_key, opts);

    int64_t last_partition_id = -1;
    for (auto kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
        auto&& [key, _] = *kvp;

        int64_t partition_id = -1;
        Versionstamp versionstamp;
        std::string_view key_view(key);
        if (!versioned::decode_meta_partition_key(&key_view, &partition_id, &versionstamp)) {
            LOG_WARNING("failed to decode meta partition key").tag("key", hex(key));
            return -1;
        }

        if (partition_id == last_partition_id || versionstamp > snapshot_versionstamp_) {
            continue;
        }

        partition_ids->push_back(partition_id);
        last_partition_id = partition_id;
    }

    if (!iter->is_valid()) {
        LOG_ERROR("failed to get all partitions").tag("error_code", iter->error_code());
        return -1;
    }

    return 0;
}

int CompactExecutor::get_all_indexes(std::vector<int64_t>* index_ids) {
    index_ids->clear();

    std::string begin_key = versioned::meta_index_key({source_instance_id_, 0});
    std::string end_key = versioned::meta_index_key({source_instance_id_, INT64_MAX});

    FullRangeGetOptions opts;
    opts.snapshot = true;
    opts.prefetch = true;
    opts.txn_kv = txn_kv_;
    auto iter = txn_kv_->full_range_get(begin_key, end_key, opts);

    int64_t last_index_id = -1;
    for (auto kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
        auto&& [key, _] = *kvp;

        int64_t index_id = -1;
        Versionstamp versionstamp;
        std::string_view key_view(key);
        if (!versioned::decode_meta_index_key(&key_view, &index_id, &versionstamp)) {
            LOG_WARNING("failed to decode index version key").tag("key", hex(key));
            return -1;
        }

        if (index_id == last_index_id || versionstamp > snapshot_versionstamp_) {
            continue;
        }
        index_ids->push_back(index_id);
        last_index_id = index_id;
    }

    if (!iter->is_valid()) {
        LOG_ERROR("failed to get all indexes").tag("error_code", iter->error_code());
        return -1;
    }

    return 0;
}

int CompactExecutor::get_all_index_schema_versions(Transaction* txn, int64_t index_id,
                                                   std::vector<int64_t>* schema_versions) {
    schema_versions->clear();

    std::string begin_key = versioned::meta_schema_key({source_instance_id_, index_id, 0});
    std::string end_key = versioned::meta_schema_key(
            {source_instance_id_, index_id, std::numeric_limits<int64_t>::max()});

    FullRangeGetOptions opts;
    opts.snapshot = true;
    opts.prefetch = true;
    auto iter = txn->full_range_get(begin_key, end_key, opts);
    int64_t last_schema_version = -1;
    for (auto kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
        auto&& [key, _] = *kvp;

        int64_t decode_index_id = -1;
        int64_t schema_version = -1;
        std::string_view key_view(key);
        if (!versioned::decode_meta_schema_key(&key_view, &decode_index_id, &schema_version)) {
            LOG_WARNING("failed to decode meta schema key").tag("key", hex(key));
            return -1;
        }

        if (schema_version != last_schema_version) {
            schema_versions->push_back(schema_version);
        }
        last_schema_version = schema_version;
    }

    if (!iter->is_valid()) {
        LOG_ERROR("failed to get all schema versions").tag("error_code", iter->error_code());
        return -1;
    }

    return 0;
}

int CompactExecutor::get_all_tablets(std::vector<int64_t>* tablet_ids) {
    tablet_ids->clear();

    std::string begin_key = versioned::meta_tablet_key({source_instance_id_, 0});
    std::string end_key = versioned::meta_tablet_key({source_instance_id_, INT64_MAX});

    FullRangeGetOptions opts;
    opts.snapshot = true;
    opts.prefetch = true;
    opts.txn_kv = txn_kv_;
    auto iter = txn_kv_->full_range_get(begin_key, end_key, opts);

    int64_t last_tablet_id = -1;
    for (auto kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
        auto&& [key, _] = *kvp;

        int64_t tablet_id = -1;
        Versionstamp versionstamp;
        std::string_view key_view(key);
        if (!versioned::decode_meta_tablet_key(&key_view, &tablet_id, &versionstamp)) {
            LOG_WARNING("failed to decode meta tablet key").tag("key", hex(key));
            return -1;
        }

        if (tablet_id == last_tablet_id || versionstamp > snapshot_versionstamp_) {
            continue;
        }
        tablet_ids->push_back(tablet_id);
        last_tablet_id = tablet_id;
    }

    if (!iter->is_valid()) {
        LOG_ERROR("failed to get all tablets").tag("error_code", iter->error_code());
        return -1;
    }

    return 0;
}

int persist_compacted_key_set(const std::string& instance_id, KeySetType key_set,
                              std::shared_ptr<TxnKv> txn_kv) {
    for (int retry = 0; retry < MAX_RETRY_TIMES; retry++) {
        if (retry > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(RETRY_INTERVAL_MS));
        }

        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to create txn to save compacted key set").tag("error", err);
            return -1;
        }

        std::string key = instance_key(instance_id);
        std::string instance_value;
        err = txn->get(key, &instance_value);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to read instance info to save compacted key set").tag("error", err);
            return -1;
        }

        InstanceInfoPB instance_info;
        if (!instance_info.ParseFromString(instance_value)) {
            LOG_WARNING("failed to parse instance info to save compacted key set");
            return -1;
        }

        // Check if already recorded
        if (std::find(instance_info.compacted_key_sets().begin(),
                      instance_info.compacted_key_sets().end(),
                      key_set) != instance_info.compacted_key_sets().end()) {
            LOG_INFO("key set already recorded as compacted")
                    .tag("key_set", KeySetType_Name(key_set));
            return 0; // already recorded
        }

        // Add the new compacted key set
        instance_info.add_compacted_key_sets(key_set);
        std::string updated_value;
        if (!instance_info.SerializeToString(&updated_value)) {
            LOG_WARNING("failed to serialize updated instance info to save compacted key set");
            return -1;
        }

        txn->put(key, updated_value);
        err = txn->commit();
        if (err == TxnErrorCode::TXN_CONFLICT) {
            LOG_WARNING("txn conflict while saving compacted key set, will retry")
                    .tag("retry", retry);
            continue; // retry
        } else if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to commit txn to save compacted key set").tag("error", err);
            return -1;
        } else {
            return 0;
        }
    }

    LOG_WARNING("failed to save compacted key set after max retries")
            .tag("max_retries", MAX_RETRY_TIMES);
    return -1;
}

int SnapshotManager::compact_snapshot_chains(InstanceChainCompactor* compactor) {
    std::string instance_id(compactor->instance_id());
    const InstanceInfoPB& instance = compactor->instance_info();
    Versionstamp snapshot_versionstamp;
    if (!parse_snapshot_versionstamp(instance.source_snapshot_id(), &snapshot_versionstamp)) {
        LOG(WARNING) << "failed to parse snapshot version stamp from instance=" << instance_id
                     << ", snapshot_id=" << instance.source_snapshot_id();
        return -1;
    }

    AnnotateTag instance_tag("instance", instance_id);
    CompactExecutor executor(instance_id, instance.source_instance_id(),
                             instance.source_snapshot_id(), snapshot_versionstamp, txn_kv_);
    KeySetType key_sets_to_compact[] = {
            MULTI_VERSION_TABLE_VERSION,
            MULTI_VERSION_PARTITION_VERSION /*MULTI_VERSION_META_PARTITION, MULTI_VERSION_INDEX_PARTITION*/
            ,
            MULTI_VERSION_INDEX_INDEX /*MULTI_VERSION_META_INDEX, MULTI_VERSION_META_SCHEMA*/,
            MULTI_VERSION_INDEX_TABLET /*MULTI_VERSION_META_TABLET, MULTI_VERSION_TABLET_LOAD_STATS, MULTI_VERSION_TABLET_COMPACT_STATS*/
            ,
            MULTI_VERSION_META_ROWSET,
    };
    for (auto key_set : key_sets_to_compact) {
        const auto& compacted_key_sets = instance.compacted_key_sets();
        if (std::find(compacted_key_sets.begin(), compacted_key_sets.end(), key_set) !=
            compacted_key_sets.end()) {
            continue;
        }

        int ret = 0;
        switch (key_set) {
        case KeySetType::MULTI_VERSION_TABLE_VERSION:
            ret = executor.compact_table_version_keys();
            break;
        case KeySetType::MULTI_VERSION_PARTITION_VERSION:
            ret = executor.compact_partition_keys();
            break;
        case KeySetType::MULTI_VERSION_INDEX_INDEX:
            ret = executor.compact_index_keys();
            break;
        case KeySetType::MULTI_VERSION_INDEX_TABLET:
            ret = executor.compact_tablet_keys();
            break;
        case KeySetType::MULTI_VERSION_META_ROWSET:
            ret = executor.compact_rowset_keys();
            break;
        default:
            LOG_WARNING("unknown key set type for compaction")
                    .tag("key_set", KeySetType_Name(key_set));
            ret = -1;
            break;
        }

        if (ret == 0) {
            ret = persist_compacted_key_set(instance_id, key_set, txn_kv_);
        }

        if (ret != 0) {
            LOG_WARNING("compaction failed for key set type")
                    .tag("key_set", KeySetType_Name(key_set));
            return ret;
        }
    }

    return 0;
}

} // namespace selectdb
