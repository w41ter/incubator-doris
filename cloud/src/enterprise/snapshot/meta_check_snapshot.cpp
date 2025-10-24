#include "meta_check_snapshot.h"

#include "meta-store/meta_reader.h"
using namespace doris::cloud;
using namespace std::chrono;
namespace selectdb {
int init_mvcc_tablet_index_info(const std::string& instance_id, TxnKv* txn_kv,
                                std::vector<doris::cloud::TabletInfo>* tablets_info) {
    std::vector<int64_t> tablet_ids;

    MetaReader reader(instance_id, txn_kv);
    TxnErrorCode err = reader.get_all_tablet_ids(&tablet_ids, false);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get all tablet ids by versioned tablet index key")
                .tag("instance_id", instance_id)
                .tag("error_code", err);
        return -1;
    }

    for (int64_t tablet_id : tablet_ids) {
        TabletIndexPB tablet_index;
        err = reader.get_tablet_index(tablet_id, &tablet_index, false);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to get tablet index")
                    .tag("instance_id", instance_id)
                    .tag("tablet_id", tablet_id)
                    .tag("error_code", err);
            return -1;
        }
        TabletInfo tablet_info;
        tablet_info.tablet_id = tablet_id;
        tablet_info.db_id = tablet_index.db_id();
        tablet_info.table_id = tablet_index.table_id();
        tablets_info->emplace_back(tablet_info);
    }
    return 0;
}

int init_mvcc_tablet_meta_info(const std::string& instance_id, TxnKv* txn_kv,
                               std::vector<doris::cloud::TabletInfo>* tablet_metas) {
    std::vector<int64_t> tablet_ids;

    MetaReader reader(instance_id, txn_kv);
    TxnErrorCode err = reader.get_all_tablet_ids(&tablet_ids, false);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get all tablet ids by versioned tablet index key")
                .tag("instance_id", instance_id)
                .tag("error_code", err);
        return -1;
    }

    for (int64_t tablet_id : tablet_ids) {
        doris::TabletMetaCloudPB tablet_meta;
        Versionstamp versionstamp;
        err = reader.get_tablet_meta(tablet_id, &tablet_meta, &versionstamp);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to get tablet meta")
                    .tag("instance_id", instance_id)
                    .tag("tablet_id", tablet_id)
                    .tag("error_code", err);
            return -1;
        }
        TabletInfo tablet_info;
        tablet_info.tablet_id = tablet_id;
        tablet_info.table_id = tablet_meta.table_id();
        tablet_info.partition_id = tablet_meta.partition_id();
        tablet_info.index_id = tablet_meta.index_id();
        tablet_info.schema_version = tablet_meta.schema_version();
        tablet_metas->emplace_back(tablet_info);
    }
    return 0;
}

int init_mvcc_partition_info(const std::string& instance_id, TxnKv* txn_kv,
                             std::vector<doris::cloud::PartitionInfo>* partitions_info) {
    std::vector<int64_t> tablet_ids;

    MetaReader reader(instance_id, txn_kv);
    TxnErrorCode err = reader.get_all_tablet_ids(&tablet_ids, false);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get all tablet ids by versioned tablet index key")
                .tag("instance_id", instance_id)
                .tag("error_code", err);
        return -1;
    }

    for (int64_t tablet_id : tablet_ids) {
        TabletIndexPB tablet_index;
        err = reader.get_tablet_index(tablet_id, &tablet_index, false);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to get tablet meta")
                    .tag("instance_id", instance_id)
                    .tag("tablet_id", tablet_id)
                    .tag("error_code", err);
            return -1;
        }
        PartitionInfo partition_info;
        partition_info.tablet_id = tablet_id;
        partition_info.table_id = tablet_index.table_id();
        partition_info.db_id = tablet_index.db_id();
        partition_info.partition_id = tablet_index.partition_id();
        partitions_info->emplace_back(partition_info);
    }
    return 0;
}

int init_mvcc_table_info(const std::string& instance_id, TxnKv* txn_kv,
                         std::vector<TableInfo>* tables_info) {
    std::vector<int64_t> tablet_ids;

    MetaReader reader(instance_id, txn_kv);
    TxnErrorCode err = reader.get_all_tablet_ids(&tablet_ids, false);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get all tablet ids by versioned tablet index key")
                .tag("instance_id", instance_id)
                .tag("error_code", err);
        return -1;
    }

    for (int64_t tablet_id : tablet_ids) {
        TabletIndexPB tablet_index;
        err = reader.get_tablet_index(tablet_id, &tablet_index, false);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to get tablet meta")
                    .tag("instance_id", instance_id)
                    .tag("tablet_id", tablet_id)
                    .tag("error_code", err);
            return -1;
        }
        TableInfo table_info;
        table_info.table_id = tablet_index.table_id();
        table_info.db_id = tablet_index.db_id();
        tables_info->emplace_back(table_info);
    }
    return 0;
}

int do_check_meta(const std::string& instance_id, MetaChecker* meta_checker, TxnKv* txn_kv) {
    int check_ret = 0;
    {
        std::vector<TabletInfo> tablets_info;
        if (init_mvcc_tablet_index_info(instance_id, txn_kv, &tablets_info) != 0) {
            LOG(WARNING) << "init_mvcc_tablet_index_info failed";
            return -1;
        }
        // check MetaTabletIdxKey inverted
        if (!meta_checker->do_meta_tablet_key_index_check(tablets_info)) {
            check_ret = 1;
            LOG(WARNING) << "do_meta_tablet_key_index_check failed";
        } else {
            LOG(INFO) << "do_meta_tablet_key_index_check success";
        }

        if (init_mvcc_tablet_meta_info(instance_id, txn_kv, &tablets_info) != 0) {
            LOG(WARNING) << "init_mvcc_tablet_meta_info failed";
            return -1;
        }
        // check MetaTabletKey
        if (!meta_checker->do_meta_tablet_key_check(tablets_info)) {
            check_ret = 1;
            LOG(WARNING) << "do_meta_tablet_key_check failed";
        } else {
            LOG(INFO) << "do_meta_tablet_key_check success";
        }
        // check MetaSchemaKey
        if (!meta_checker->do_meta_schema_key_check(tablets_info)) {
            check_ret = 1;
            LOG(WARNING) << "do_meta_schema_key_check failed";
        } else {
            LOG(INFO) << "do_meta_schema_key_check success";
        }
    }

    {
        std::vector<PartitionInfo> partitions_info;
        if (!init_mvcc_partition_info(instance_id, txn_kv, &partitions_info)) {
            LOG(WARNING) << "init_mvcc_partition_info failed";
            return -1;
        }
        // check PartitionVersionKey
        if (!meta_checker->do_version_partition_key_check(partitions_info)) {
            check_ret = 1;
            LOG(WARNING) << "do_version_partition_key_check failed";
        } else {
            LOG(INFO) << "do_version_partition_key_check success";
        }
    }

    {
        std::vector<TableInfo> tables_info;
        if (!init_mvcc_table_info(instance_id, txn_kv, &tables_info)) {
            LOG(WARNING) << "init_mvcc_table_info failed";
            return -1;
        }
        // check TableVersionKey
        if (!meta_checker->do_version_table_key_check(tables_info)) {
            check_ret = 1;
            LOG(WARNING) << "do_version_table_key_check failed";
        } else {
            LOG(INFO) << "do_version_table_key_check success";
        }
    }
    return check_ret;
}

int do_mvcc_meta_tablet_index_key_inverted_check(const std::string& instance_id, TxnKv* txn_kv,
                                                 MetaChecker* meta_checker) {
    int check_res = 0;
    MetaReader reader(instance_id, txn_kv);
    // check tablet idx
    for (const auto& tablet_info : meta_checker->tablets_info_ref()) {
        TabletIndexPB tablet_idx;
        TxnErrorCode err = reader.get_tablet_index(tablet_info.tablet_id, &tablet_idx, false);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            LOG(WARNING) << "tablet idx not found: " << tablet_info.tablet_id;
            check_res = 1;
            continue;
        } else if (err != TxnErrorCode::TXN_OK) [[unlikely]] {
            LOG(WARNING) << "failed to get tablet idx, err: " << err;
            return -1;
        }

        if (tablet_info.db_id != tablet_idx.db_id()) [[unlikely]] {
            LOG(WARNING) << "tablet idx check failed, fe db id: " << tablet_info.db_id
                         << " tablet idx db id: " << tablet_idx.db_id();
            check_res = 1;
            continue;
        }

        if (tablet_info.table_id != tablet_idx.table_id()) [[unlikely]] {
            LOG(WARNING) << "tablet idx check failed, fe table id: " << tablet_info.table_id
                         << " tablet idx table id: " << tablet_idx.table_id();
            check_res = 1;
            continue;
        }

        if (tablet_info.partition_id != tablet_idx.partition_id()) [[unlikely]] {
            LOG(WARNING) << "tablet idx check failed, fe part id: " << tablet_info.partition_id
                         << " tablet idx part id: " << tablet_idx.partition_id();
            check_res = 1;
            continue;
        }

        if (tablet_info.index_id != tablet_idx.index_id()) [[unlikely]] {
            LOG(WARNING) << "tablet idx check failed, fe index id: " << tablet_info.index_id
                         << " tablet idx index id: " << tablet_idx.index_id();
            check_res = 1;
            continue;
        }

        if (tablet_info.tablet_id != tablet_idx.tablet_id()) [[unlikely]] {
            LOG(WARNING) << "tablet idx check failed, fe tablet id: " << tablet_info.tablet_id
                         << " tablet idx tablet id: " << tablet_idx.tablet_id();
            check_res = 1;
            continue;
        }
    }
    return check_res;
}

int do_mvcc_meta_tablet_key_inverted_check(const std::string& instance_id, TxnKv* txn_kv,
                                           MetaChecker* meta_checker) {
    int check_res = 0;
    MetaReader reader(instance_id, txn_kv);
    for (const auto& tablet_info : meta_checker->tablets_info_ref()) {
        doris::TabletMetaCloudPB tablet_meta;
        Versionstamp versionstamp;
        TxnErrorCode err =
                reader.get_tablet_meta(tablet_info.tablet_id, &tablet_meta, &versionstamp, false);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            LOG(WARNING) << "tablet meta not found: " << tablet_info.tablet_id;
            check_res = 1;
            continue;
        } else if (err != TxnErrorCode::TXN_OK) [[unlikely]] {
            LOG(WARNING) << "failed to get tablet, err: " << err;
            return -1;
            continue;
        }
    }

    return check_res;
}

int do_mvcc_meta_schema_key_inverted_check(const std::string& instance_id, TxnKv* txn_kv,
                                           MetaChecker* meta_checker) {
    int check_res = 0;
    MetaReader reader(instance_id, txn_kv);

    for (const auto& tablet_info : meta_checker->tablets_info_ref()) {
        doris::TabletSchemaCloudPB tablet_schema;
        TxnErrorCode err = reader.get_tablet_schema(
                tablet_info.index_id, tablet_info.schema_version, &tablet_schema, false);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            LOG(WARNING) << "tablet schema not found: " << tablet_info.debug_string();
            check_res = 1;
            continue;
        } else if (err != TxnErrorCode::TXN_OK) [[unlikely]] {
            LOG(WARNING) << "failed to get tablet schema, err: " << err;
            return -1;
        }
    }
    return check_res;
}

int do_inverted_check_meta(const std::string& instance_id, MetaChecker* meta_checker,
                           TxnKv* txn_kv) {
    meta_checker->init_db_meta();
    meta_checker->init_tablet_and_partition_info_from_fe_meta();

    int check_res = 0;
    // check MetaTabletIdxKey
    if (do_mvcc_meta_tablet_index_key_inverted_check(instance_id, txn_kv, meta_checker) != 0) {
        check_res = -1;
        LOG(WARNING) << "do_mvcc_meta_tablet_index_key_inverted_check failed";
    } else {
        LOG(INFO) << "do_mvcc_meta_tablet_index_key_inverted_check success";
    }

    // check MetaTabletKey
    if (do_mvcc_meta_tablet_key_inverted_check(instance_id, txn_kv, meta_checker) != 0) {
        check_res = -1;
        LOG(WARNING) << "do_mvcc_meta_tablet_key_inverted_check failed";
    } else {
        LOG(INFO) << "do_mvcc_meta_tablet_key_inverted_check success";
    }

    // check MetaSchemaKey
    if (do_mvcc_meta_schema_key_inverted_check(instance_id, txn_kv, meta_checker) != 0) {
        check_res = -1;
        LOG(WARNING) << "do_mvcc_meta_schema_key_inverted_check failed";
    } else {
        LOG(INFO) << "do_mvcc_meta_schema_key_inverted_check success";
    }

    return check_res;
}
} // namespace selectdb