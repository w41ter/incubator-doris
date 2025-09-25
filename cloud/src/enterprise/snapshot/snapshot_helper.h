
#include <brpc/builtin_service.pb.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <butil/strings/string_split.h>
#include <bvar/status.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>

#include <chrono>
#include <cstdint>

#include "common/config.h"
#include "meta-store/versionstamp.h"

using namespace doris::cloud;
using namespace std::chrono;
namespace selectdb {

inline static int64_t system_clock_now_seconds() {
    return duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
}

// Is the snapshot in preparing status timed out?
inline static bool is_creating_snapshot_timeout(const SnapshotPB& snapshot_pb) {
    if (config::force_immediate_recycle) {
        return true;
    }

    int64_t created_at = snapshot_pb.create_at();
    int64_t deadline = created_at + snapshot_pb.timeout_seconds();
    return system_clock_now_seconds() >= deadline;
}

// Is the aborted snapshot can be pruned?
inline static bool is_aborted_snapshot_pruneable(const SnapshotPB& snapshot_pb) {
    if (config::force_immediate_recycle) {
        return true;
    }

    int64_t aborted_at = snapshot_pb.finish_at();
    int64_t deadline = aborted_at + config::prune_aborted_snapshot_seconds;
    return system_clock_now_seconds() >= deadline;
}

// Is the manually created snapshot expired?
inline static bool is_snapshot_expired(const SnapshotPB& snapshot_pb) {
    if (config::force_immediate_recycle) {
        return true;
    }

    int64_t created_at = snapshot_pb.create_at();
    int64_t deadline = created_at + snapshot_pb.ttl_seconds();
    return system_clock_now_seconds() >= deadline;
}

// inline static Versionstamp parse_snapshot_versionstamp(const std::string& str) {
//     if (str.size() != 20) {
//         return Versionstamp::min();
//     }

//     std::array<uint8_t, 10> data;
//     for (size_t i = 0; i < 10; ++i) {
//         std::string byte_str = str.substr(i * 2, 2);
//         data[i] = static_cast<uint8_t>(std::stoul(byte_str, nullptr, 16));
//     }
//     return {data};
// }

// inline static std::string serialize_snapshot_versionstamp(Versionstamp snapshot_versionstamp) {
//     return snapshot_versionstamp.to_string();
// }

// // Parse hex-encoded versionstamp from snapshot ID.
// // The snapshot ID is expected to be a 20-character hex string representing 10 bytes.
// inline static bool parse_snapshot_versionstamp(std::string_view snapshot_id,
//                                                Versionstamp* versionstamp) {
//     if (snapshot_id.size() != 20) {
//         return false;
//     }

//     std::array<uint8_t, 10> versionstamp_data;
//     for (size_t i = 0; i < 10; ++i) {
//         const char* hex_chars = snapshot_id.data() + (i * 2);

//         // Convert two hex digits to one byte more efficiently
//         uint8_t high_nibble = 0, low_nibble = 0;

//         // Parse high nibble
//         if (hex_chars[0] >= '0' && hex_chars[0] <= '9') {
//             high_nibble = hex_chars[0] - '0';
//         } else if (hex_chars[0] >= 'a' && hex_chars[0] <= 'f') {
//             high_nibble = hex_chars[0] - 'a' + 10;
//         } else if (hex_chars[0] >= 'A' && hex_chars[0] <= 'F') {
//             high_nibble = hex_chars[0] - 'A' + 10;
//         } else {
//             return false;
//         }

//         // Parse low nibble
//         if (hex_chars[1] >= '0' && hex_chars[1] <= '9') {
//             low_nibble = hex_chars[1] - '0';
//         } else if (hex_chars[1] >= 'a' && hex_chars[1] <= 'f') {
//             low_nibble = hex_chars[1] - 'a' + 10;
//         } else if (hex_chars[1] >= 'A' && hex_chars[1] <= 'F') {
//             low_nibble = hex_chars[1] - 'A' + 10;
//         } else {
//             return false;
//         }

//         versionstamp_data[i] = (high_nibble << 4) | low_nibble;
//     }

//     *versionstamp = Versionstamp(versionstamp_data);
//     return true;
// }

} // namespace selectdb