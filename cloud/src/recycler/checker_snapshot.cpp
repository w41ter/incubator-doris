#include <brpc/builtin_service.pb.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <butil/strings/string_split.h>
#include <bvar/status.h>
#include <cpp/sync_point.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>

#include "recycler/checker.h"

namespace doris::cloud {

int InstanceChecker::do_snapshots_check() {
    int ret = snapshot_manager_->check_snapshots(this);
    int success = 0;
    if (ret != 0) {
        if (ret == 1) {
            LOG(WARNING) << "failed to check snapshots"
                         << ", snapshot file lost or snapshot key leaked"
                         << ", instance_id=" << instance_id_;
        } else if (ret < 0) {
            LOG(WARNING) << "failed to check snapshots"
                         << ", instance_id=" << instance_id_;
        }
        success = 1;
    }
    ret = snapshot_manager_->inverted_check_snapshots(this);
    if (ret != 0) {
        if (ret == 1) {
            LOG(WARNING) << "failed to inverted check snapshots"
                         << ", snapshot key lost or snapshot file leaked"
                         << ", instance_id=" << instance_id_;
        } else if (ret < 0) {
            LOG(WARNING) << "failed to inverted check snapshots"
                         << ", instance_id=" << instance_id_;
        }
        success = 1;
    }
    return success;
}
} // namespace doris::cloud