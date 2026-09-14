// types.cc · 对外数据类型的序列化（严格对齐契约 §2 字段清单）
//
// 一条硬规矩（契约 §2）：**只输出契约里有的字段，缺什么就不出什么键**。
// 前端与第三方都按"字段存在与否"判断降级，凭空补 0 会被当成真实值。
#include <cmath>

#include "device_ingest/types.h"

#include "util/clock.h"

namespace device_ingest {

namespace {

/// 有限性检查：NaN / Inf 一律视为非法值（ING-NRM-02 的"非数字只丢该字段"）。
bool finite(double v) { return std::isfinite(v); }

}  // namespace

// nowMs() 的唯一实现在 src/util/clock.h（inline）。types.h 只是声明它，
// 让 include/ 面不反向依赖 src/。

// ---------------------------------------------------------------- NormalizedEvent
nlohmann::json NormalizedEvent::toJson() const {
    nlohmann::json j = nlohmann::json::object();
    j[field::kDeviceId]   = deviceId;
    j[field::kDeviceType] = deviceType;
    j[field::kKind]       = kind;
    if (hasLng && finite(lng)) j[field::kLng] = lng;
    if (hasLat && finite(lat)) j[field::kLat] = lat;
    if (hasAlt && finite(alt)) j[field::kAlt] = alt;
    if (hasHeading && finite(heading)) j[field::kHeading] = heading;
    if (hasSpeed && finite(speed)) j[field::kSpeed] = speed;
    if (hasBattery) j[field::kBattery] = battery;
    if (hasSeq) j[field::kSeq] = seq;
    j[field::kTs]       = ts;
    j[field::kTsSource] = toString(tsSource);
    j[field::kRecvAt]   = recvAt;
    if (!quality.is_null() && !quality.empty()) j[field::kQuality] = quality;
    j[field::kSource] = {
        {field::kIngestId, source.ingestId},
        {field::kPeer, source.peer},
        {field::kBytes, source.bytes},
    };
    if (!raw.is_null()) j[field::kRaw] = raw;
    return j;
}

// ---------------------------------------------------------------- DeviceInfo
nlohmann::json DeviceInfo::toJson(Millis now) const {
    nlohmann::json j = nlohmann::json::object();
    j["deviceId"]   = deviceId;
    j["deviceType"] = deviceType;
    j["firstSeen"]  = firstSeen;
    j["lastSeen"]   = lastSeen;
    j["online"]     = online;
    j["offlineSec"] = online ? 0.0 : offlineSec;
    if (hasLastPos) {
        j["lastPos"] = {{"lng", lastLng}, {"lat", lastLat}};
        if (hasLastAlt) j["lastPos"]["alt"] = lastAlt;
    }
    j["ingestId"] = ingestId;
    j["peer"]     = peer;
    j["packets"]  = packets;
    if (hasSeq) j["seq"] = lastSeq;
    if (!online) j["silentSec"] = static_cast<double>(now - lastSeen) / 1000.0;
    return j;
}

// ---------------------------------------------------------------- DeviceHealthEvent
nlohmann::json DeviceHealthEvent::toJson() const {
    nlohmann::json j = nlohmann::json::object();
    switch (kind) {
        case Kind::Online:
            j["deviceId"]    = deviceId;
            j["deviceType"]  = deviceType;
            j["online"]      = true;
            j["offlineSec"]  = offlineSec;
            j["lastSeen"]    = lastSeen;
            j["reason"]      = reason.empty() ? "resumed" : reason;
            break;
        case Kind::Offline:
            j["deviceId"]    = deviceId;
            j["deviceType"]  = deviceType;
            j["online"]      = false;
            j["lastSeen"]    = lastSeen;
            j["timeoutSec"]  = timeoutSec;
            j["offlineSec"]  = offlineSec;
            j["reason"]      = reason.empty() ? "timeout" : reason;
            break;
        case Kind::Stats:
            j["deviceId"]       = deviceId;
            j["deviceType"]     = deviceType;
            j["seqPrecise"]     = seqPrecise;
            j["windowSec"]      = windowSec;
            if (seqPrecise) {
                j["packetLossRate"] = packetLossRate;
                j["outOfOrderRate"] = outOfOrderRate;
            } else {
                j["packetLossRate"] = nullptr;
                j["outOfOrderRate"] = nullptr;
                j["reason"] = reason.empty() ? "设备报文无 seq，不可精确统计" : reason;
            }
            break;
    }
    return j;
}

// ---------------------------------------------------------------- DeviceStats
nlohmann::json DeviceStats::toJson() const {
    nlohmann::json j = nlohmann::json::object();
    j["deviceId"]       = deviceId;
    j["online"]         = online;
    j["seqPrecise"]     = seqPrecise;
    j["windowSec"]      = windowSec;
    j["timeoutSec"]     = timeoutSec;
    j["received"]       = received;
    if (seqPrecise) {
        j["packetLossRate"]  = hasLossRate ? nlohmann::json(packetLossRate) : nlohmann::json(nullptr);
        j["outOfOrderCount"] = outOfOrderCount;
        j["outOfOrderRate"]  = hasOutOfOrderRate ? nlohmann::json(outOfOrderRate) : nlohmann::json(nullptr);
        j["expected"]        = expected;
        j["minSeq"]          = minSeq;
        j["maxSeq"]          = maxSeq;
    } else {
        // 禁止返回估计值冒充精确值（契约 §4.2 seqPrecise 语义 MUST）
        j["packetLossRate"]  = nullptr;
        j["outOfOrderRate"]  = nullptr;
        j["outOfOrderCount"] = nullptr;
        j["reason"] = reason.empty() ? "设备报文无 seq，不可精确统计" : reason;
    }
    if (!reason.empty() && seqPrecise) j["reason"] = reason;
    return j;
}

// ---------------------------------------------------------------- CommandRecord
nlohmann::json CommandRecord::toJson() const {
    nlohmann::json j = nlohmann::json::object();
    j["cmdId"]  = cmdId;
    j["state"]  = toString(state);
    j["type"]   = type;
    j["payload"] = payload;
    if (!deviceId.empty()) j["deviceId"] = deviceId;
    if (!ingestId.empty()) j["ingestId"] = ingestId;
    j["target"]  = target;
    j["actor"]   = actor;
    j["retries"] = retries;
    j["maxRetries"] = maxRetries;
    j["timeoutMs"]  = timeoutMs;
    j["createdAt"]  = createdAt;
    j["sentAt"]     = sentAt;
    if (ackedAt)    j["ackedAt"]    = ackedAt;
    if (finishedAt) j["finishedAt"] = finishedAt;
    if (!detail.empty()) j["detail"] = detail;
    nlohmann::json tl = nlohmann::json::array();
    for (const auto& e : timeline) {
        nlohmann::json t = {{"at", e.at}, {"event", e.event}};
        if (!e.detail.empty()) t["detail"] = e.detail;
        tl.push_back(std::move(t));
    }
    j["timeline"] = std::move(tl);
    return j;
}

// ---------------------------------------------------------------- AlertEvent
nlohmann::json AlertEvent::toJson() const {
    const char* lvl = "warn";
    switch (level) {
        case Level::Info:     lvl = "info";     break;
        case Level::Warn:     lvl = "warn";     break;
        case Level::Critical: lvl = "critical"; break;
    }
    nlohmann::json j = {
        {"level", lvl},
        {"code",  code},
        {"title", title},
        {"text",  text},
    };
    if (!ingestId.empty()) j["ingestId"] = ingestId;
    return j;
}

// ---------------------------------------------------------------- PointMetrics
nlohmann::json PointMetrics::toJson() const {
    nlohmann::json reasons = nlohmann::json::object();
    if (truncated) reasons["truncated"] = truncated;
    if (empty)     reasons["empty"]     = empty;
    return nlohmann::json{
        {"id",          id},
        {"running",     running},
        {"packets",     packets},
        {"bytes",       bytes},
        {"dropped",     dropped},
        {"truncated",   truncated},
        {"empty",       empty},
        {"parseFailed", parseFailed},
        {"events",      events},
        {"lastRecvAt",  lastRecvAt},
        {"lastPeer",    lastPeer},
        {"dropReasons", reasons},
        {"lastError",   lastError},
    };
}

// ---------------------------------------------------------------- GatewayHealth
nlohmann::json GatewayHealth::toJson() const {
    return nlohmann::json{
        {"points",        points},
        {"pointsRunning", pointsRunning},
        {"devices",       devices},
        {"online",        online},
        {"packets",       packets},
        {"events",        events},
        {"dropped",       dropped},
        {"parseFailed",   parseFailed},
        {"packetsPerSec", packetsPerSec},
        {"queueDepth",    queueDepth},
        {"queueDropped",  queueDropped},
    };
}

}  // namespace device_ingest
