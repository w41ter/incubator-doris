#include <brpc/builtin_service.pb.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <butil/strings/string_split.h>
#include <bvar/status.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>

#include "common/util.h"
#include "meta-store/txn_kv.h"
#include "recycler/checker.h"
#include "recycler/meta_checker.h"
#include "recycler/util.h"
#include "snapshot/snapshot_manager.h"

using namespace doris::cloud;
using namespace std::chrono;

namespace selectdb {
int init_mvcc_tablet_index_info(const std::string& instance_id, TxnKv* txn_kv,
                                std::vector<TabletInfo>* tablets_info);

int init_mvcc_tablet_meta_info(const std::string& instance_id, TxnKv* txn_kv,
                               std::vector<TabletInfo>* tablet_metas);

int init_mvcc_partition_info(const std::string& instance_id, TxnKv* txn_kv,
                             std::vector<PartitionInfo>* partitions_info);

int init_mvcc_table_info(const std::string& instance_id, TxnKv* txn_kv,
                         std::vector<TableInfo>* tables_info);

int do_check_meta(const std::string& instance_id, MetaChecker* meta_checker, TxnKv* txn_kv);

int do_mvcc_meta_tablet_index_key_inverted_check(const std::string& instance_id, TxnKv* txn_kv,
                                                 MetaChecker* meta_checker);

int do_mvcc_meta_tablet_key_inverted_check(const std::string& instance_id, TxnKv* txn_kv,
                                           MetaChecker* meta_checker);

int do_mvcc_meta_schema_key_inverted_check(const std::string& instance_id, TxnKv* txn_kv,
                                           MetaChecker* meta_checker);

int do_inverted_check_meta(const std::string& instance_id, MetaChecker* meta_checker,
                           TxnKv* txn_kv);

} // namespace selectdb