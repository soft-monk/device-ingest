// health.cc · 台账 / 失联判定 / 丢包与乱序精确统计
//
// 验收口径（需求专篇 §3.4）逐条对应：
//   ING-HLT-01 离线：超时 = 心跳周期 × 倍数（默认 3），巡检粒度 ≤ 1 s
//   ING-HLT-02 上线：离线设备再上报即发上线事件，携带离线时长
//   ING-HLT-03 丢包率：(maxSeq - minSeq + 1 - 实收数) / (maxSeq - minSeq + 1)
//   ING-HLT-04 乱序：后到包 seq < 窗口内最大 seq 计一次；无 seq → 不可精确统计
//   ING-HLT-05 台账：全部已发现设备（含离线设备），字段完整
#include "hlt/health.h"

#include <algorithm>
#include <cmath>

#include "device_ingest/parser.h"
#include "nrm/normalize.h"

namespace device_ingest {
namespace hlt {

namespace {

/// seq 回绕/重置判定（契约 §4.2：回绕 MUST 视为**新会话**，不计为负丢包）。
///
/// 判据（只认"明显回落"）：
///   新 seq 明显小于窗口内最大 seq（差值超过窗口跨度的一半）→ 设备重启/回绕，开新会话。
///
/// 关键取舍：正常推进（seq 从小到大）**永远不会**命中这里，
/// 否则每来一个新包都会把窗口重置成"只有它自己"，丢包率就永远算不对。
bool looksLikeNewSession(long long seq, long long windowMax, long long windowMin) {
    if (seq >= windowMax) return false;                 // 正常推进 / 乱序到达，都不算新会话
    const long long span = windowMax - windowMin + 1;
    const long long threshold = span > 2 ? span / 2 : 1;
    return (windowMax - seq) > threshold;
}

}  // namespace

DeviceInfo DeviceRecord::toInfo(Millis now) const {
    DeviceInfo info;
    info.deviceId   = deviceId;
    info.deviceType = deviceType;
    info.firstSeen  = firstSeen;
    info.lastSeen   = lastSeen;
    info.online     = online;
    info.offlineSec = online ? 0.0 : static_cast<double>(now - lastSeen) / 1000.0;
    info.hasLastPos = hasLastPos;
    info.lastLng    = lastLng;
    info.lastLat    = lastLat;
    info.hasLastAlt = hasLastAlt;
    info.lastAlt    = lastAlt;
    info.ingestId   = ingestId;
    info.peer       = peer;
    info.hasSeq     = hasSeq;
    info.lastSeq    = lastSeq;
    info.packets    = static_cast<int>(packets > 2147483647LL ? 2147483647LL : packets);
    return info;
}

DeviceStats DeviceRecord::toStats(const IngestConfig& cfg, Millis now) const {
    DeviceStats st;
    st.deviceId  = deviceId;
    st.online    = online;
    st.windowSec = cfg.healthWindowSec;
    st.timeoutSec = timeoutSec;
    st.received  = received;

    if (!hasSeq) {
        // 决策 D6：无 seq 不给估计值，明确标注"不可精确统计"
        st.seqPrecise = false;
        st.reason = "设备报文无 seq，不可精确统计";
        return st;
    }
    st.seqPrecise = true;
    st.minSeq = minSeq;
    st.maxSeq = maxSeq;
    const long long expected = maxSeq - minSeq + 1;
    st.expected = expected > 0 ? expected : 0;
    if (expected > 0) {
        double loss = static_cast<double>(expected - received) / static_cast<double>(expected);
        if (loss < 0.0) loss = 0.0;
        if (loss > 1.0) loss = 1.0;
        st.hasLossRate = true;
        st.packetLossRate = loss;
        st.hasOutOfOrderRate = true;
        st.outOfOrderCount = outOfOrder;
        st.outOfOrderRate = static_cast<double>(outOfOrder) / static_cast<double>(expected);
    }
    if (seqWrapped) {
        st.reason = "窗口内 seq 回绕/重置，已按新会话处理（不计为负丢包）";
    }
    (void)now;
    return st;
}

// ---------------------------------------------------------------- HealthTracker
HealthTracker::HealthTracker(const IngestConfig& cfg) : cfg_(cfg) {
    ledgerLimit_ = cfg.ledgerLimit;
}

void HealthTracker::setConfig(const IngestConfig& cfg) {
    std::lock_guard<std::mutex> lk(mtx_);
    cfg_ = cfg;
    ledgerLimit_ = cfg.ledgerLimit;
    for (auto& kv : devices_) {
        const int mult = kv.second.timeoutSec > 0 && cfg.heartbeatPeriodMs > 0
            ? static_cast<int>(std::lround(kv.second.timeoutSec * 1000.0 / cfg.heartbeatPeriodMs))
            : cfg.defaultTimeoutMultiple;
        kv.second.timeoutSec =
            static_cast<double>(cfg.heartbeatPeriodMs) / 1000.0 *
            static_cast<double>(mult > 0 ? mult : cfg.defaultTimeoutMultiple);
    }
}

void HealthTracker::setLedgerLimit(std::size_t limit) {
    std::lock_guard<std::mutex> lk(mtx_);
    ledgerLimit_ = limit;
    // 上限调小要**立即收缩**（ING-NFR-05：容器不超限），按最久未上报淘汰
    while (ledgerLimit_ > 0 && devices_.size() > ledgerLimit_) {
        auto victim = devices_.begin();
        for (auto i = devices_.begin(); i != devices_.end(); ++i) {
            if (i->second.lastSeen < victim->second.lastSeen) victim = i;
        }
        if (victim == devices_.end()) break;
        devices_.erase(victim);
        ++evicted_;
    }
}

HealthTracker::PushResult HealthTracker::push(const NormalizedEvent& ev) {
    PushResult result;
    std::lock_guard<std::mutex> lk(mtx_);
    const std::int64_t windowSize = static_cast<std::int64_t>(cfg_.healthWindowSec) * 1000;
    const std::int64_t window = windowSize > 0 ? (ev.recvAt / windowSize) : 0;

    auto it = devices_.find(ev.deviceId);
    if (it == devices_.end()) {
        // 台账超限：淘汰最久未上报的一条（不能无限增长，ING-NFR-05）
        if (ledgerLimit_ > 0 && devices_.size() >= ledgerLimit_) {
            auto victim = devices_.begin();
            for (auto i = devices_.begin(); i != devices_.end(); ++i) {
                if (i->second.lastSeen < victim->second.lastSeen) victim = i;
            }
            devices_.erase(victim);
            ++evicted_;
        }
        DeviceRecord rec;
        rec.deviceId   = ev.deviceId;
        rec.deviceType = ev.deviceType.empty() ? "-" : ev.deviceType;
        rec.firstSeen  = ev.recvAt;
        rec.lastSeen   = ev.recvAt;
        rec.online     = true;
        rec.timeoutSec = static_cast<double>(cfg_.heartbeatPeriodMs) / 1000.0 *
                         static_cast<double>(cfg_.defaultTimeoutMultiple);
        it = devices_.emplace(ev.deviceId, std::move(rec)).first;
        result.isNewDevice = true;
        result.cameOnline  = true;   // 新发现即在线（ING-HLT-06 上线事件）
    }

    DeviceRecord& r = it->second;
    r.lastSeen   = ev.recvAt;
    r.deviceType = ev.deviceType.empty() ? r.deviceType : ev.deviceType;
    r.ingestId   = ev.source.ingestId;
    r.peer       = ev.source.peer;
    ++r.packets;

    if (ev.hasLng && ev.hasLat) {
        r.hasLastPos = true;
        r.lastLng = ev.lng;
        r.lastLat = ev.lat;
    }
    if (ev.hasAlt) {
        r.hasLastAlt = true;
        r.lastAlt = ev.alt;
    }

    if (!r.online) {
        // ING-HLT-02：恢复上线，携带离线时长
        r.online = true;
        r.lastOfflineSec = r.offlineSince > 0
            ? static_cast<double>(ev.recvAt - r.offlineSince) / 1000.0
            : 0.0;
        r.offlineSince = 0;
        result.cameOnline = true;
        result.offlineSec = r.lastOfflineSec;
    }

    result.deviceType = r.deviceType;
    result.lastSeen   = r.lastSeen;

    // ---------------- 序号统计（ING-HLT-03/04）
    if (!ev.hasSeq) {
        r.hasSeq = false;   // 该设备标"不可精确统计"
        return result;
    }
    r.hasSeq = true;

    if (r.windowIndex != window) {
        r.windowIndex = window;   // 滚窗：区间统计随窗口重置
        r.minSeq = ev.seq;
        r.maxSeq = ev.seq;
        r.received = 0;
        r.outOfOrder = 0;
        r.seqWrapped = false;
    }

    if (looksLikeNewSession(ev.seq, r.maxSeq, r.minSeq)) {
        // seq 回绕/设备重启：按新会话处理，不计为负丢包（契约 §4.2 MUST）
        r.minSeq = ev.seq;
        r.maxSeq = ev.seq;
        r.received = 0;
        r.outOfOrder = 0;
        r.seqWrapped = true;
    } else {
        // 乱序：本包 seq 严格小于窗口内已见最大值（契约 §4.2 口径）
        if (ev.seq < r.maxSeq) ++r.outOfOrder;
        if (ev.seq > r.maxSeq) r.maxSeq = ev.seq;
        if (ev.seq < r.minSeq) r.minSeq = ev.seq;
    }
    ++r.received;
    r.lastSeq = ev.seq;
    return result;
}

void HealthTracker::check(Millis now, std::vector<DeviceRecord>& wentOffline) {
    std::lock_guard<std::mutex> lk(mtx_);
    for (auto& kv : devices_) {
        DeviceRecord& r = kv.second;
        if (!r.online) continue;
        const double silentSec = static_cast<double>(now - r.lastSeen) / 1000.0;
        if (silentSec > r.timeoutSec) {
            r.online = false;
            r.everOffline = true;
            r.offlineSince = now;
            wentOffline.push_back(r);
        }
    }
}

std::vector<DeviceStats> HealthTracker::dueStats(Millis now) {
    std::lock_guard<std::mutex> lk(mtx_);
    std::vector<DeviceStats> out;
    const Millis interval = cfg_.statsPushIntervalMs > 0 ? cfg_.statsPushIntervalMs : 5000;
    for (auto& kv : devices_) {
        const auto sit = lastStatsAt_.find(kv.first);
        if (sit == lastStatsAt_.end()) {
            lastStatsAt_[kv.first] = now;
            continue;
        }
        if (now - sit->second < interval) continue;
        sit->second = now;
        if (!kv.second.hasSeq) continue;   // 无 seq 的设备不推 stats（避免噪声）
        out.push_back(kv.second.toStats(cfg_, now));
    }
    return out;
}

bool HealthTracker::find(const std::string& deviceId, DeviceRecord& out) const {
    std::lock_guard<std::mutex> lk(mtx_);
    auto it = devices_.find(deviceId);
    if (it == devices_.end()) return false;
    out = it->second;
    return true;
}

std::vector<DeviceInfo> HealthTracker::list(Millis now) const {
    std::lock_guard<std::mutex> lk(mtx_);
    std::vector<DeviceInfo> out;
    out.reserve(devices_.size());
    for (const auto& kv : devices_) out.push_back(kv.second.toInfo(now));
    std::sort(out.begin(), out.end(),
              [](const DeviceInfo& a, const DeviceInfo& b) {
                  if (a.deviceId != b.deviceId) return a.deviceId < b.deviceId;
                  return a.deviceType < b.deviceType;
              });
    return out;
}

bool HealthTracker::stats(const std::string& deviceId, DeviceStats& out) const {
    std::lock_guard<std::mutex> lk(mtx_);
    auto it = devices_.find(deviceId);
    if (it == devices_.end()) return false;
    out = it->second.toStats(cfg_, nowMs());
    return true;
}

std::size_t HealthTracker::size() const {
    std::lock_guard<std::mutex> lk(mtx_);
    return devices_.size();
}

std::size_t HealthTracker::onlineCount(Millis now) const {
    std::lock_guard<std::mutex> lk(mtx_);
    std::size_t n = 0;
    for (const auto& kv : devices_) {
        const DeviceRecord& r = kv.second;
        if (r.online && static_cast<double>(now - r.lastSeen) / 1000.0 <= r.timeoutSec) ++n;
    }
    return n;
}

std::uint64_t HealthTracker::evictedTotal() const {
    std::lock_guard<std::mutex> lk(mtx_);
    return evicted_;
}

void HealthTracker::clear() {
    std::lock_guard<std::mutex> lk(mtx_);
    devices_.clear();
    lastStatsAt_.clear();
    evicted_ = 0;
}

}  // namespace hlt
}  // namespace device_ingest
