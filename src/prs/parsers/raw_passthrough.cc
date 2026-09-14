// raw_passthrough.cc · 原始透传解析器（ING-PRS-03 / 契约 §7）
//
// 用途：**协议未定的设备现在就能接**——把原始字节包成 kind:"raw" 事件，
// 带来源地址、长度与十六进制摘要，不需要任何协议文档。
//
// 明确不做（需求专篇 §1.2"不做协议反向工程"）：
//   不猜测任何字段语义、不尝试识别坐标/时间戳、不做部分解析。
#include <cstddef>
#include <string>
#include <vector>

#include "device_ingest/parser.h"
#include "prs/registry.h"
#include "nrm/validate.h"
#include "util/net.h"

namespace device_ingest {

namespace {

class RawPassthroughParser final : public IParser {
public:
    std::string id() const override { return "raw.passthrough"; }
    std::string deviceType() const override { return "raw"; }
    std::string name() const override { return "原始透传（协议未定设备）"; }
    std::string version() const override { return "1"; }

    bool parse(const char* data, std::size_t len,
               const std::string& peer,
               std::vector<ParsedEvent>& out) override {
        if (data == nullptr || len == 0) return false;

        std::string ip = peer;
        int port = 0;
        net::splitPeer(peer, ip, port);

        const std::string digest = nrm::hexDigest(data, len);

        ParsedEvent pe;
        pe.kind = "raw";
        pe.rawPassthrough = true;
        // 不给 deviceId：归一化会用 source.peer 兜底并计数（契约 §2 降级约定）
        pe.ts  = 0;    // 无协议 → 无设备时间戳，归一化置 tsSource:"server"
        pe.seq = -1;   // 无协议 → 无 seq，丢包/乱序"不可精确统计"
        pe.fields = nlohmann::json::object();
        pe.fields["kind"]      = "raw";
        pe.fields["peer"]      = peer;
        pe.fields["ip"]        = ip;
        if (port > 0) pe.fields["port"] = port;
        pe.fields["bytes"]     = len;
        pe.fields["payload"]   = digest;
        pe.raw = digest;   // 归一层会把它带进 NormalizedEvent.raw

        out.push_back(std::move(pe));
        return true;
    }
};

}  // namespace

ParserPtr makeRawPassthroughParser() {
    return std::make_shared<RawPassthroughParser>();
}

}  // namespace device_ingest
