#include "check_helper.h"

using namespace doris::cloud;
using namespace std::chrono;

namespace selectdb {

int SnapshotManager::check_snapshots(InstanceChecker* checker) {
    int check_res = 0;
    std::string_view instance_id = checker->instance_id();
    AnnotateTag tag("instance_id", instance_id);
    LOG(INFO) << "begin to check cluster snapshots";

    StopWatch stop_watch;
    size_t total_snapshots = 0;
    size_t num_loss = 0;

    DORIS_CLOUD_DEFER {
        int64_t cost = stop_watch.elapsed_us() / 1000'000;

        LOG_INFO("check cluster snapshots, cost={}s", cost)
                .tag("total_snapshots", total_snapshots)
                .tag("num_loss", num_loss);
    };

    std::vector<std::pair<SnapshotPB, Versionstamp>> snapshots;
    {
        MetaReader reader(instance_id, txn_kv_.get());
        TxnErrorCode err = reader.get_snapshots(&snapshots);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to get snapshots").tag("error_code", err);
            return -1;
        }
    }

    for (auto&& [snapshot_pb, snapshot_versionstamp] : snapshots) {
        ++total_snapshots;
        if (is_snapshot_normal(snapshot_pb)) {
            auto* accessor = checker->get_accessor(snapshot_pb.resource_id());
            if (accessor == nullptr) {
                LOG_WARNING("failed to get accessor").tag("resource_id", snapshot_pb.resource_id());
                check_res = -1;
                continue;
            }
            std::string snapshot_id = serialize_snapshot_id(snapshot_versionstamp);
            std::string snapshot_path = "snapshot/" + snapshot_id + "/";
            std::unique_ptr<ListIterator> list_iter;
            if (accessor->list_directory(snapshot_path, &list_iter) == 0) {
                if (!list_iter->has_next()) {
                    LOG_WARNING("snapshot path not exist, snapshot file loss or snapshot key leak")
                            .tag("resource_id", snapshot_pb.resource_id())
                            .tag("snapshot versionstamp", snapshot_id)
                            .tag("snapshot_path", snapshot_path);
                    num_loss++;
                    check_res = 1;
                }
            } else {
                LOG_WARNING("failed to check snapshot path existence")
                        .tag("resource_id", snapshot_pb.resource_id())
                        .tag("snapshot versionstamp", snapshot_id)
                        .tag("snapshot_path", snapshot_path);
                check_res = -1;
            }
        }
    }

    return num_loss > 0 ? 1 : check_res;
}

int SnapshotManager::inverted_check_snapshots(InstanceChecker* checker) {
    int check_res = 0;
    std::string_view instance_id = checker->instance_id();
    AnnotateTag tag("instance_id", instance_id);
    LOG_INFO("begin to inverted check cluster snapshots");

    StopWatch stop_watch;
    size_t total_snapshots = 0;
    size_t num_leak = 0;

    DORIS_CLOUD_DEFER {
        int64_t cost = stop_watch.elapsed_us() / 1000'000;

        LOG_INFO("check cluster snapshots, cost={}s", cost)
                .tag("total_snapshots", total_snapshots)
                .tag("num_leak", num_leak);
    };

    std::vector<StorageVaultAccessor*> accessors;
    checker->get_all_accessor(&accessors);

    for (auto& accessor : accessors) {
        std::unique_ptr<ListIterator> list_iter;
        int ret = accessor->list_directory("snapshot", &list_iter);
        if (ret != 0) {
            return -1;
        }

        // to skip already checked snapshot versionstamp
        std::string already_checked_snapshot_vs;

        for (auto file = list_iter->next(); file.has_value(); file = list_iter->next()) {
            std::vector<std::string> str;
            butil::SplitString(file->path, '/', &str);
            if (str.size() < 2) {
                LOG_WARNING("invalid snapshot file path format").tag("path", file->path);
                continue;
            }
            ++total_snapshots;

            if (!already_checked_snapshot_vs.empty() && str[1] == already_checked_snapshot_vs) {
                // already checked
                continue;
            }

            already_checked_snapshot_vs = str[1];
            int ret = check_snapshot_key_exist(txn_kv_.get(), instance_id,
                                               already_checked_snapshot_vs);
            if (ret == 1) {
                num_leak++;
                check_res = 1;
            } else if (ret != 0) {
                check_res = -1;
            }
        }

        if (!list_iter->is_valid()) {
            LOG(WARNING) << "failed to list snapshot directory. uri=" << accessor->uri();
            return -1;
        }
    }
    return num_leak > 0 ? 1 : check_res;
}

int SnapshotManager::check_mvcc_meta_key(InstanceChecker* checker) {
    int ret = 0;

    int check_res = check_mvcc_meta_rowset_key(checker, txn_kv_.get());
    if (check_res == 1) {
        LOG(INFO) << "failed to check mvcc meta rowset key";
        ret = 1;
    } else if (check_res == -1) {
        LOG(INFO) << "error occurred when check mvcc meta rowset key";
        ret = -1;
    }
    return ret;
}

int SnapshotManager::inverted_check_mvcc_meta_key(InstanceChecker* checker) {
    int ret = 0;

    int check_res = inverted_check_mvcc_meta_rowset_key(checker, txn_kv_.get());
    if (check_res == 1) {
        LOG(INFO) << "failed to inverted check mvcc meta rowset key";
        ret = 1;
    } else if (check_res == -1) {
        LOG(INFO) << "error occurred when inverted check mvcc meta rowset key";
        ret = -1;
    }
    return ret;
}

int SnapshotManager::check_meta(MetaChecker* meta_checker) {
    bool check_res = true;
    std::string instance_id = meta_checker->instance_id();

    if (do_check_meta(instance_id, meta_checker, txn_kv_.get()) != 0) {
        LOG(WARNING) << "do_check_meta failed";
        check_res = false;
    } else {
        LOG(INFO) << "do_check_meta success";
    }

    return check_res ? 0 : -1;
}
} // namespace selectdb