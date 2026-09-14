// merge.cc
#include "fan/merge.h"

#include <string>
#include <unordered_map>

namespace device_ingest {
namespace fan {

std::vector<IngestEvent> mergeBatch(const std::vector<IngestEvent>& batch) {
    if (batch.size() <= 1) return batch;

    // 先判有没有重复：没有重复就不做任何拷贝（1000×1 Hz 下这是常态）
    std::unordered_map<std::string, std::size_t> seen;
    seen.reserve(batch.size() * 2);
    bool hasDuplicate = false;
    for (const auto& ev : batch) {
        const std::string key = ev.normalized.deviceId.empty() ? ev.type : ev.normalized.deviceId;
        if (!seen.emplace(key, 0).second) { hasDuplicate = true; break; }
    }
    if (!hasDuplicate) return batch;

    std::vector<IngestEvent> out;
    out.reserve(batch.size());
    std::unordered_map<std::string, std::size_t> index;
    index.reserve(batch.size() * 2);
    for (const auto& ev : batch) {
        const std::string key = ev.normalized.deviceId.empty() ? ev.type : ev.normalized.deviceId;
        auto it = index.find(key);
        if (it == index.end()) {
            index.emplace(key, out.size());
            out.push_back(ev);            // 首次出现：占位
        } else {
            out[it->second] = ev;         // 再次出现：只留最后一次（位置不变）
        }
    }
    return out;
}

std::size_t mergedCount(const std::vector<IngestEvent>& batch) {
    if (batch.size() <= 1) return batch.size();
    std::unordered_map<std::string, std::size_t> seen;
    seen.reserve(batch.size() * 2);
    for (const auto& ev : batch) {
        const std::string key = ev.normalized.deviceId.empty() ? ev.type : ev.normalized.deviceId;
        seen.emplace(key, 0);
    }
    return seen.size();
}

}  // namespace fan
}  // namespace device_ingest
