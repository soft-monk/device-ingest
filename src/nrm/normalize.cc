// normalize.cc · 归一化实现
//
// 契约 §2（字段级）+ §5（事件名）+ 需求 ING-NRM-01~04。
//
// 一句话职责：把"各说各话的外设字段"变成**唯一形状**的 NormalizedEvent，
// 非法值只丢该字段（不丢事件、不报错），缺失值明确标注来源而不是猜。
#include "nrm/normalize.h"

#include <algorithm>

#include "prs/registry.h"
#include "nrm/validate.h"

namespace device_ingest {
namespace nrm {

void NormalizeStats::merge(const NormalizeStats& other) {
    coordDroppedLng  += other.coordDroppedLng;
    coordDroppedLat  += other.coordDroppedLat;
    headingFixed     += other.headingFixed;
    headingDropped   += other.headingDropped;
    batteryDropped   += other.batteryDropped;
    tsFromServer     += other.tsFromServer;
    deviceIdFallback += other.deviceIdFallback;
    seqMissing       += other.seqMissing;
}

nlohmann::json NormalizeStats::toJson() const {
    return nlohmann::json{
        {"coordDroppedLng",  coordDroppedLng},
        {"coordDroppedLat",  coordDroppedLat},
        {"headingFixed",     headingFixed},
        {"headingDropped",   headingDropped},
        {"batteryDropped",   batteryDropped},
        {"tsFromServer",     tsFromServer},
        {"deviceIdFallback", deviceIdFallback},
        {"seqMissing",       seqMissing},
    };
}

namespace {

/// 各 kind 的"设备标识字段"优先级。
/// 既有 4 类 kind 的标识字段名来自主仓现状报文（Telemetry.cc:149-152 的注释格式），
/// 新设备类型可继续加行，或直接在报文里给 deviceId。
const char* const* idFieldsForKind(const std::string& kind, std::size_t& count) {
    static const char* kUav[]    = {"uavId", "deviceId", "id"};
    static const char* kLink[]   = {"linkId", "deviceId", "id"};
    static const char* kTarget[] = {"targetId", "deviceId", "id"};
    static const char* kNode[]   = {"nodeId", "deviceId", "id"};
    static const char* kGeneric[] = {"deviceId", "uavId", "nodeId", "targetId", "linkId", "id"};

    if (kind == "uav.pos")       { count = 3; return kUav; }
    if (kind == "link.quality")  { count = 3; return kLink; }
    if (kind == "target.state")  { count = 3; return kTarget; }
    if (kind == "node.state")    { count = 3; return kNode; }
    count = 6;
    return kGeneric;
}

}  // namespace

NormalizedEvent normalize(const ParsedEvent& pe,
                          const PointConfig& point,
                          const std::string& peer,
                          std::size_t bytes,
                          Millis recvAt,
                          NormalizeStats* stats) {
    NormalizeStats local;
    NormalizeStats& st = stats ? *stats : local;

    NormalizedEvent ev;
    ev.kind       = pe.kind;
    ev.deviceType = point.deviceType;
    ev.recvAt     = recvAt;
    ev.source.ingestId = point.id;
    ev.source.peer     = peer;
    ev.source.bytes    = bytes;

    // ---------------- deviceId（MUST；缺失用 source.peer 兜底并计数）
    ev.deviceId = pe.deviceId;
    if (ev.deviceId.empty()) {
        std::size_t n = 0;
        const char* const* keys = idFieldsForKind(pe.kind, n);
        for (std::size_t i = 0; i < n && ev.deviceId.empty(); ++i) {
            auto v = asString(pe.fields, keys[i]);
            if (v.has && !v.value.empty()) ev.deviceId = v.value;
        }
    }
    if (ev.deviceId.empty()) {
        ev.deviceId = peer.empty() ? std::string("unknown") : peer;
        ++st.deviceIdFallback;
    }

    // ---------------- 时间戳（MUST；优先设备 ts，缺失用 recvAt 并标 server）
    if (pe.ts > 0) {
        ev.ts = pe.ts;
        ev.tsSource = TsSource::Device;
    } else {
        ev.ts = recvAt;
        ev.tsSource = TsSource::Server;
        ++st.tsFromServer;
    }

    // ---------------- 坐标（越界/非数字只丢该字段，事件仍上行）
    const auto lng = asDouble(pe.fields, "lng");
    const auto lat = asDouble(pe.fields, "lat");
    if (lng.has && validLng(lng.value)) {
        ev.hasLng = true;
        ev.lng = lng.value;
    } else if (lng.has) {
        ++st.coordDroppedLng;
    }
    if (lat.has && validLat(lat.value)) {
        ev.hasLat = true;
        ev.lat = lat.value;
    } else if (lat.has) {
        ++st.coordDroppedLat;
    }

    // ---------------- 其余可选字段
    const auto alt = asDouble(pe.fields, "alt");
    if (alt.has) { ev.hasAlt = true; ev.alt = alt.value; }

    const auto heading = asDouble(pe.fields, "heading");
    if (heading.has) {
        double h = 0.0;
        if (normalizeHeading(heading.value, h)) {
            ev.hasHeading = true;
            ev.heading = h;
            if (h != heading.value) ++st.headingFixed;
        } else {
            ++st.headingDropped;
        }
    }

    const auto speed = asDouble(pe.fields, "speed");
    if (speed.has && speed.value >= 0.0) { ev.hasSpeed = true; ev.speed = speed.value; }

    const auto battery = asInt(pe.fields, "battery");
    if (battery.has) {
        if (validBattery(battery.value)) {
            ev.hasBattery = true;
            ev.battery = static_cast<int>(battery.value);
        } else {
            ++st.batteryDropped;
        }
    }

    // ---------------- seq（缺失 → 丢包/乱序"不可精确统计"）
    if (pe.seq >= 0) {
        ev.hasSeq = true;
        ev.seq = pe.seq;
    } else {
        ++st.seqMissing;
    }

    if (!pe.quality.is_null() && !pe.quality.empty()) ev.quality = pe.quality;
    if (!pe.raw.is_null()) ev.raw = pe.raw;

    return ev;
}

std::string resolveEventName(const std::string& kind, const std::string& topic) {
    // 1) 既有 4 类 kind 的事件名固定（兼容承诺，不可改名）
    const std::string legacy = legacyEventName(kind);
    if (!legacy.empty()) return legacy;

    // 2) 接入点显式声明的 topic
    if (!topic.empty()) return topic;

    // 3) 兜底：telemetry.<kind>
    if (kind.empty()) return "telemetry.event";
    return "telemetry." + kind;
}

nlohmann::json toEventData(const NormalizedEvent& ev, const ParsedEvent& pe,
                           bool legacyCompat) {
    nlohmann::json data = nlohmann::json::object();

    if (legacyCompat) {
        // 既有 4 类：**原字段一字不改**（前端零改动），随后只做字段补充。
        if (pe.fields.is_object()) data = pe.fields;
        const bool hasTs = data.contains("ts");
        if (!hasTs) data["ts"] = ev.ts;
        // 新增可选字段（契约 §5：只增不改）
        if (!data.contains("deviceId")) data["deviceId"] = ev.deviceId;
        if (ev.hasSeq && !data.contains("seq")) data["seq"] = ev.seq;
        if (!data.contains("tsSource")) data["tsSource"] = toString(ev.tsSource);
        if (!data.contains("source")) {
            data["source"] = {
                {field::kIngestId, ev.source.ingestId},
                {field::kPeer, ev.source.peer},
                {field::kBytes, ev.source.bytes},
            };
        }
        if (ev.hasLng && !data.contains("lng")) data["lng"] = ev.lng;
        if (ev.hasLat && !data.contains("lat")) data["lat"] = ev.lat;
        return data;
    }

    // 非既有类型：标准归一字段；原始透传还要带上解析器给的原始包信息
    // （契约 §2 `raw` 与 ING-NRM-04 溯源：来源地址、原始包长度）
    data = ev.toJson();
    if (pe.rawPassthrough && pe.fields.is_object()) {
        for (auto it = pe.fields.begin(); it != pe.fields.end(); ++it) {
            if (it.value().is_object() || it.value().is_array()) continue;
            if (!data.contains(it.key())) data[it.key()] = it.value();
        }
    }
    return data;
}

}  // namespace nrm
}  // namespace device_ingest
