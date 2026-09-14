// legacy_kind.cc · 既有 4 类 kind 解析器
//
// 设计方案 §11 迁移映射：
//   net/Telemetry.cc:147-170 injectPacket  →  src/prs/parsers/legacy_kind.cc
//   「重写为解析器（4 类 kind 语义与事件名保持不变）」
//
// 兼容承诺（ING-PRS-05 / 契约 §5）：
//   `telemetry.uav.pos` / `telemetry.link.quality` / `target.state` / `node.state`
//   这 4 个事件名与既有字段**不可改**；本解析器把原始 JSON 的全部键原样带走
//   （见 nrm::toEventData 的 legacyCompat 分支），前端因此零改动。
#include <string>
#include <vector>

#include "device_ingest/parser.h"
#include "prs/registry.h"
#include "nrm/validate.h"

namespace device_ingest {

namespace {

/// kind → 既有 WS 事件名（兼容范围的真值表，只此一处）。
struct KindMap {
    const char* kind;
    const char* event;
};
const KindMap kLegacyKinds[] = {
    {"uav.pos",       "telemetry.uav.pos"},
    {"link.quality",  "telemetry.link.quality"},
    {"target.state",  "target.state"},
    {"node.state",    "node.state"},
};

class LegacyKindParser final : public IParser {
public:
    std::string id() const override { return "legacy.kind.v1"; }
    std::string deviceType() const override { return "legacy"; }
    std::string name() const override { return "既有 4 类 kind 分派（兼容解析器）"; }
    std::string version() const override { return "1"; }

    bool parse(const char* data, std::size_t len,
               const std::string& peer,
               std::vector<ParsedEvent>& out) override {
        if (data == nullptr || len == 0) return false;

        nlohmann::json j;
        try {
            j = nlohmann::json::parse(data, data + len);
        } catch (...) {
            return false;   // 非法报文丢弃，调用方计入 parseFailed（现状行为一致）
        }
        if (!j.is_object()) return false;

        const auto kindOpt = nrm::asString(j, "kind");
        if (!kindOpt.has) return false;
        const std::string& kind = kindOpt.value;

        bool known = false;
        for (const auto& m : kLegacyKinds) {
            if (kind == m.kind) { known = true; break; }
        }
        if (!known) return false;   // 不是本解析器负责的 kind

        ParsedEvent pe;
        pe.kind = kind;
        pe.fields = std::move(j);

        // 设备时间戳：既有报文用 ts；优先采信，缺失留给归一化用到达时刻补齐。
        const auto ts = nrm::asInt(pe.fields, "ts");
        if (ts.has && ts.value > 0) pe.ts = static_cast<Millis>(ts.value);

        // 设备序号：既有 4 类报文里没有 seq（评审确认新设备必须带）；
        // 有则采信，无则明确标缺（丢包/乱序走"不可精确统计"，不给估计值）。
        const auto seq = nrm::asInt(pe.fields, "seq");
        if (seq.has && seq.value >= 0) pe.seq = seq.value;

        (void)peer;
        out.push_back(std::move(pe));
        return true;
    }
};

}  // namespace

ParserPtr makeLegacyKindParser() {
    return std::make_shared<LegacyKindParser>();
}

std::string legacyEventName(const std::string& kind) {
    for (const auto& m : kLegacyKinds) {
        if (kind == m.kind) return m.event;
    }
    return std::string();
}

bool isLegacyKind(const std::string& kind) {
    return !legacyEventName(kind).empty();
}

}  // namespace device_ingest
