// tests/selftest.cc · 零依赖自测（对齐 map-2d 的做法：一个可执行 + 手写断言）
//
// 覆盖范围与需求条目一一对应，**不需要外设、不需要网络**：
//   S2  ING-PRS-01/03/05 · ING-NRM-01/02/03/04
//   S3  ING-HLT-01/02/03/04/05/06 · ING-FAN-01/02/03 · ING-NFR-05
//
// 运行： selftest          → 跑全部
//        selftest --list   → 只列用例名
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "device_ingest/gateway.h"
#include "device_ingest/hub.h"
#include "device_ingest/parser.h"
#include "device_ingest/version.h"

#include "hlt/health.h"   // 纯逻辑自测直接驱动 HealthTracker（不经过线程/网络）

using namespace device_ingest;

namespace {

// ---------------------------------------------------------------- 断言框架
int g_checks = 0;
int g_failed = 0;
std::string g_case;
std::vector<std::string> g_failures;

void check(bool ok, const std::string& what) {
    ++g_checks;
    if (!ok) {
        ++g_failed;
        g_failures.push_back("[" + g_case + "] " + what);
    }
}

template <typename A, typename B>
void checkEq(const A& got, const B& want, const std::string& what) {
    ++g_checks;
    if (!(got == want)) {
        ++g_failed;
        std::ostringstream oss;
        oss << "[" << g_case << "] " << what << "  期望=" << want << " 实际=" << got;
        g_failures.push_back(oss.str());
    }
}

void checkNear(double got, double want, double tol, const std::string& what) {
    ++g_checks;
    if (!(got >= want - tol && got <= want + tol)) {
        ++g_failed;
        std::ostringstream oss;
        oss << "[" << g_case << "] " << what << "  期望≈" << want << "(±" << tol
            << ") 实际=" << got;
        g_failures.push_back(oss.str());
    }
}

// ---------------------------------------------------------------- 测试用 Sink
struct Captured {
    std::vector<IngestEvent>        events;
    std::vector<DeviceHealthEvent>  health;
    std::vector<AlertEvent>         alerts;
    std::vector<std::size_t>        batchSizes;
    mutable std::mutex              mtx;

    void clear() {
        std::lock_guard<std::mutex> lk(mtx);
        events.clear();
        health.clear();
        alerts.clear();
        batchSizes.clear();
    }
    std::size_t eventCount() const {
        std::lock_guard<std::mutex> lk(mtx);
        return events.size();
    }
    /// 含某个 deviceId 的事件条数（ING-FAN-02 合并断言用）。
    std::size_t eventCountOf(const std::string& deviceId) const {
        std::lock_guard<std::mutex> lk(mtx);
        std::size_t n = 0;
        for (const auto& e : events) {
            if (e.normalized.deviceId == deviceId) ++n;
        }
        return n;
    }
    bool lastEventOf(const std::string& deviceId, IngestEvent& out) const {
        std::lock_guard<std::mutex> lk(mtx);
        for (auto it = events.rbegin(); it != events.rend(); ++it) {
            if (it->normalized.deviceId == deviceId) { out = *it; return true; }
        }
        return false;
    }
    std::size_t healthCount(DeviceHealthEvent::Kind kind) const {
        std::lock_guard<std::mutex> lk(mtx);
        std::size_t n = 0;
        for (const auto& h : health) {
            if (h.kind == kind) ++n;
        }
        return n;
    }
    bool lastHealth(DeviceHealthEvent::Kind kind, DeviceHealthEvent& out) const {
        std::lock_guard<std::mutex> lk(mtx);
        for (auto it = health.rbegin(); it != health.rend(); ++it) {
            if (it->kind == kind) { out = *it; return true; }
        }
        return false;
    }
};

class TestSink final : public ISink {
public:
    explicit TestSink(std::shared_ptr<Captured> c) : c_(std::move(c)) {}
    void onEvent(const IngestEvent& ev) override {
        std::lock_guard<std::mutex> lk(c_->mtx);
        c_->events.push_back(ev);
    }
    void onBatch(const std::vector<IngestEvent>& evs) override {
        std::lock_guard<std::mutex> lk(c_->mtx);
        c_->batchSizes.push_back(evs.size());
        for (const auto& e : evs) c_->events.push_back(e);
    }
    void onDeviceHealth(const DeviceHealthEvent& ev) override {
        std::lock_guard<std::mutex> lk(c_->mtx);
        c_->health.push_back(ev);
    }
    void onAlert(const AlertEvent& ev) override {
        std::lock_guard<std::mutex> lk(c_->mtx);
        c_->alerts.push_back(ev);
    }

private:
    std::shared_ptr<Captured> c_;
};

/// 必抛异常的解析器（ING-PRS-04 用）：id = "test.throw.v1"
class ThrowingParser final : public IParser {
public:
    std::string id() const override { return "test.throw.v1"; }
    std::string deviceType() const override { return "test"; }
    bool parse(const char*, std::size_t, const std::string&,
               std::vector<ParsedEvent>&) override {
        throw std::runtime_error("故意抛出的解析器异常");
    }
};

// ---------------------------------------------------------------- 公共装置
IngestConfig testConfig(int basePort, int mergeWindowMs) {
    IngestConfig cfg;
    cfg.enabled = true;
    cfg.mergeWindowMs = mergeWindowMs;
    cfg.heartbeatPeriodMs = 1000;
    cfg.defaultTimeoutMultiple = 3;      // 失联 = 3 s
    cfg.offlineCheckIntervalMs = 100;    // 判定粒度 ≤ 1 s（ING-HLT-01）
    cfg.healthWindowSec = 60;
    cfg.statsPushIntervalMs = 100000;    // 自测里不主动推 stats，避免噪声
    cfg.emitRawEvents = false;

    PointConfig legacy;
    legacy.id = "p-legacy";
    legacy.port = basePort;
    legacy.parserId = "legacy.kind.v1";
    legacy.deviceType = "uav";
    legacy.topic = "";                    // 由 kind 决定事件名

    PointConfig raw;
    raw.id = "p-raw";
    raw.port = basePort + 1;
    raw.parserId = "raw.passthrough";
    raw.deviceType = "unknown";
    raw.topic = "telemetry.raw";

    PointConfig thr;
    thr.id = "p-throw";
    thr.port = basePort + 2;
    thr.parserId = "test.throw.v1";
    thr.deviceType = "test";
    thr.topic = "telemetry.test";

    cfg.points = {legacy, raw, thr};
    return cfg;
}

std::string uavJson(const std::string& id, long long seq, long long ts,
                    const std::string& extra = std::string()) {
    std::ostringstream oss;
    oss << "{\"kind\":\"uav.pos\",\"uavId\":\"" << id << "\",\"groupId\":\"g1\","
        << "\"type\":\"optical\",\"lng\":116.4,\"lat\":39.9,\"alt\":900,"
        << "\"heading\":120,\"speed\":22.5,\"battery\":85";
    if (ts > 0)  oss << ",\"ts\":" << ts;
    if (seq >= 0) oss << ",\"seq\":" << seq;
    if (!extra.empty()) oss << "," << extra;
    oss << "}";
    return oss.str();
}

/// 等待条件成立（带超时），返回是否等到。
template <typename Pred>
bool waitFor(Pred pred, int timeoutMs, int stepMs = 20) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(stepMs));
    }
    return pred();
}

// ---------------------------------------------------------------- 用例
/// S2 · ING-PRS-05：既有 4 类 kind 的事件名与字段保持不变的回归。
void testLegacyCompat(Gateway& gw, const std::shared_ptr<Captured>& cap) {
    g_case = "S2/ING-PRS-05 既有 4 类 kind 兼容";
    cap->clear();
    const long long ts = nowMs();

    checkEq(gw.inject("p-legacy", uavJson("uav-1", 1, ts)), 1, "uav.pos 产出 1 个事件");
    checkEq(gw.inject("p-legacy",
                      "{\"kind\":\"link.quality\",\"linkId\":\"link-1\",\"from\":\"a\","
                      "\"to\":\"b\",\"signal\":\"strong\",\"bandwidthMbps\":82.5,"
                      "\"latencyMs\":38,\"lossRate\":0.3,\"state\":\"green\",\"ts\":" +
                          std::to_string(ts) + "}"),
            1, "link.quality 产出 1 个事件");
    checkEq(gw.inject("p-legacy",
                      "{\"kind\":\"target.state\",\"targetId\":\"t-1\",\"targetNo\":\"T-001\","
                      "\"threat\":\"high\",\"status\":\"red\",\"lng\":116.41,\"lat\":39.89,"
                      "\"ts\":" + std::to_string(ts) + "}"),
            1, "target.state 产出 1 个事件");
    checkEq(gw.inject("p-legacy",
                      "{\"kind\":\"node.state\",\"nodeId\":\"n-1\",\"name\":\"节点1\","
                      "\"online\":true,\"ts\":" + std::to_string(ts) + "}"),
            1, "node.state 产出 1 个事件");

    check(waitFor([&] { return cap->eventCount() >= 4; }, 2000), "4 个事件都到达");
    std::this_thread::sleep_for(std::chrono::milliseconds(250));   // 等合并窗过去

    std::vector<std::string> types;
    {
        std::lock_guard<std::mutex> lk(cap->mtx);
        for (const auto& e : cap->events) types.push_back(e.type);
    }
    auto has = [&](const char* t) {
        for (const auto& s : types) {
            if (s == t) return true;
        }
        return false;
    };
    check(has("telemetry.uav.pos"),     "事件名 telemetry.uav.pos 不变");
    check(has("telemetry.link.quality"), "事件名 telemetry.link.quality 不变");
    check(has("target.state"),          "事件名 target.state 不变");
    check(has("node.state"),            "事件名 node.state 不变");

    // 既有字段必须原样保留（只增不改）
    IngestEvent ev;
    check(cap->lastEventOf("uav-1", ev), "能取到 uav-1 的事件");
    checkEq(ev.normalized.deviceId, std::string("uav-1"), "deviceId 取自 uavId");
    checkEq(ev.data.value("groupId", std::string()), std::string("g1"), "既有字段 groupId 保留");
    checkEq(ev.data.value("type", std::string()), std::string("optical"), "既有字段 type 保留");
    checkEq(ev.data.value("battery", -1), 85, "既有字段 battery 保留");
    checkEq(ev.data.value("tsSource", std::string()), std::string("device"),
            "新增字段 tsSource=device");
    check(ev.data.contains("source"), "新增字段 source 存在");
    checkEq(ev.data["source"].value("ingestId", std::string()), std::string("p-legacy"),
            "source.ingestId 正确");
    check(!ev.data.contains("padding"), "无 padding 字段时不凭空造字段");
}

/// S2 · ING-NRM-02/03：坐标越界、缺 ts、字符串数字、非法值只丢字段不丢事件。
void testNormalizeDegrade(Gateway& gw, const std::shared_ptr<Captured>& cap) {
    g_case = "S2/ING-NRM-02/03 归一化降级";
    cap->clear();
    const long long now = nowMs();

    // ① 缺 ts → ts 用到达时刻，tsSource = server
    checkEq(gw.inject("p-legacy",
                      "{\"kind\":\"uav.pos\",\"uavId\":\"n-1\",\"lng\":116.4,\"lat\":39.9}"),
            1, "缺 ts 仍产出事件");
    // ② 坐标越界 → 丢 lng/lat，事件仍在
    checkEq(gw.inject("p-legacy",
                      "{\"kind\":\"uav.pos\",\"uavId\":\"n-2\",\"lng\":200.0,\"lat\":39.9,"
                      "\"ts\":" + std::to_string(now) + "}"),
            1, "经度越界仍产出事件");
    // ③ 坐标写成字符串 → 能解析就接受
    checkEq(gw.inject("p-legacy",
                      "{\"kind\":\"uav.pos\",\"uavId\":\"n-3\",\"lng\":\"116.45\","
                      "\"lat\":\"39.91\",\"ts\":" + std::to_string(now) + "}"),
            1, "字符串坐标可解析");
    // ④ 航向 400 → 归一化到 40；电量 150 → 丢弃
    checkEq(gw.inject("p-legacy",
                      "{\"kind\":\"uav.pos\",\"uavId\":\"n-4\",\"lng\":116.4,\"lat\":39.9,"
                      "\"heading\":400,\"battery\":150,\"ts\":" + std::to_string(now) + "}"),
            1, "航向越界/电量越界仍产出事件");
    // ⑤ 未知字段 → 忽略不报错
    checkEq(gw.inject("p-legacy",
                      "{\"kind\":\"uav.pos\",\"uavId\":\"n-5\",\"lng\":116.4,\"lat\":39.9,"
                      "\"未知字段\":{\"a\":1},\"ts\":" + std::to_string(now) + "}"),
            1, "未知字段不导致失败");
    // ⑥ 非法 JSON → parseFailed，不产出事件
    checkEq(gw.inject("p-legacy", "{不是 JSON"), 0, "非法 JSON 不产出事件");
    // ⑦ 缺 kind / 未知 kind → 该解析器不认
    checkEq(gw.inject("p-legacy", "{\"uavId\":\"n-6\"}"), 0, "缺 kind 不产出事件");
    checkEq(gw.inject("p-legacy", "{\"kind\":\"unknown.kind\",\"uavId\":\"n-6\"}"), 0,
            "未知 kind 不产出事件");

    std::this_thread::sleep_for(std::chrono::milliseconds(250));

    IngestEvent ev;
    check(cap->lastEventOf("n-1", ev), "取到 n-1");
    checkEq(std::string(toString(ev.normalized.tsSource)), std::string("server"),
            "缺 ts → tsSource=server");
    check(ev.normalized.ts > 0, "缺 ts 时 ts 被补成到达时刻");
    checkNear(static_cast<double>(ev.normalized.ts - ev.normalized.recvAt), 0.0, 5.0,
              "补的 ts 等于 recvAt");

    check(cap->lastEventOf("n-2", ev), "取到 n-2");
    check(!ev.normalized.hasLng, "经度越界 → lng 字段被丢弃");
    check(ev.normalized.hasLat, "纬度合法 → lat 保留");
    checkEq(ev.normalized.lat, 39.9, "lat 值正确");

    check(cap->lastEventOf("n-3", ev), "取到 n-3");
    check(ev.normalized.hasLng, "字符串经度被接受");
    checkNear(ev.normalized.lng, 116.45, 1e-9, "字符串经度值正确");
    checkEq(ev.normalized.hasSeq, false, "无 seq → hasSeq=false（不可精确统计）");

    check(cap->lastEventOf("n-4", ev), "取到 n-4");
    checkNear(ev.normalized.heading, 40.0, 1e-9, "航向 400 → 归一化到 40");
    checkEq(ev.normalized.hasBattery, false, "电量 150 越界 → 丢弃该字段");

    check(cap->lastEventOf("n-5", ev), "取到 n-5");
    check(!ev.normalized.toJson().contains("未知字段"),
          "未知字段不进**归一对象**");
    // 说明：契约 §2 的"未知字段原样忽略"针对归一对象；
    // 既有 4 类 kind 的事件 data 为兑现 ING-PRS-05（字段不变）而原样透传，
    // 因此这里不断言 data 里没有未知字段。

    // 接入点指标：解析失败被计数（ING-ACC-06）
    const auto pm = gw.pointMetrics("p-legacy");
    check(pm.value("parseFailed", 0ull) >= 3ull, "非法/缺 kind 报文计入 parseFailed");
}

/// S2 · ING-PRS-03：原始透传。
void testRawPassthrough(Gateway& gw, const std::shared_ptr<Captured>& cap) {
    g_case = "S2/ING-PRS-03 原始透传";
    cap->clear();
    const std::string bytes = std::string("\x01\x02\x03\xff\x00", 5);
    checkEq(gw.inject("p-raw", bytes, "192.168.1.31:51000"), 1, "任意字节产出 1 个 raw 事件");
    std::this_thread::sleep_for(std::chrono::milliseconds(250));

    IngestEvent ev;
    check(cap->lastEventOf("192.168.1.31:51000", ev), "raw 事件的 deviceId 用 peer 兜底");
    checkEq(ev.type, std::string("telemetry.raw"), "raw 事件名来自接入点 topic");
    checkEq(ev.normalized.kind, std::string("raw"), "kind=raw");
    checkEq(std::string(toString(ev.normalized.tsSource)), std::string("server"),
            "raw 无设备 ts → tsSource=server");
    checkEq(ev.normalized.hasSeq, false, "raw 无 seq → 不可精确统计");
    check(ev.normalized.raw.is_string(), "raw 字段带十六进制摘要");
    checkEq(ev.normalized.raw.get<std::string>(), std::string("010203ff00"), "摘要内容正确");
    checkEq(ev.normalized.source.peer, std::string("192.168.1.31:51000"), "source.peer 正确");
    checkEq(ev.normalized.source.bytes, static_cast<std::size_t>(5), "source.bytes 正确");
    checkEq(ev.data.value("bytes", 0ull), 5ull, "事件里带原始包长度（ING-NRM-04 溯源）");
}

/// S2 · ING-PRS-02/04：未注册解析器拒绝启动；解析器抛异常被隔离。
void testParserIsolation(Gateway& gw, const std::shared_ptr<Captured>& cap) {
    g_case = "S2/ING-PRS-02/04 解析器注册与异常隔离";
    cap->clear();

    // 未注册的 parserId → 错误码 3001
    PointConfig bad;
    bad.id = "p-missing";
    bad.port = 45900;
    bad.parserId = "no.such.parser";
    bad.deviceType = "x";
    int code = 0;
    std::string err;
    check(!gw.addPoint(bad, &code, &err), "未注册解析器 → 拒绝启动接入点");
    checkEq(code, 3001, "错误码 3001（解析器未注册）");
    check(!err.empty(), "给出可读原因");

    // 注册一个必抛异常的解析器 → 注入时被捕获，接入点继续工作
    const auto thrown = std::make_shared<std::atomic<int>>(0);
    class CountingThrow final : public IParser {
    public:
        explicit CountingThrow(std::shared_ptr<std::atomic<int>> n) : n_(std::move(n)) {}
        std::string id() const override { return "test.throw.v1"; }
        std::string deviceType() const override { return "test"; }
        bool parse(const char*, std::size_t, const std::string&,
                   std::vector<ParsedEvent>&) override {
            ++(*n_);
            throw std::runtime_error("boom");
        }
    private:
        std::shared_ptr<std::atomic<int>> n_;
    };
    gw.registerParser(std::make_shared<CountingThrow>(thrown));

    bool anyAlert = false;
    for (int i = 0; i < 3; ++i) {
        checkEq(gw.inject("p-throw", "whatever"), 0, "抛异常的解析器不产出事件");
    }
    checkEq(thrown->load(), 3, "解析器确实被调用了 3 次（异常被捕获而不是崩溃）");
    {
        std::lock_guard<std::mutex> lk(cap->mtx);
        for (const auto& a : cap->alerts) {
            if (a.code == "parser_exception") anyAlert = true;
        }
    }
    check(anyAlert, "解析器异常上报了 alert");

    // 其它接入点不受影响（故障隔离 ING-PRS-04）
    const long long ts = nowMs();
    checkEq(gw.inject("p-legacy", uavJson("iso-1", 1, ts)), 1,
            "解析器异常后其它接入点仍正常产出事件");
    const auto pm = gw.pointMetrics("p-throw");
    check(pm.value("parseFailed", 0ull) >= 3ull, "异常计入该接入点的 parseFailed");
}

/// S3 · ING-FAN-02：单帧合并（窗内同设备只留最后一次）。
void testMergeWindow(Gateway& gw, const std::shared_ptr<Captured>& cap) {
    g_case = "S3/ING-FAN-02 单帧合并";
    cap->clear();

    // 同一窗内对同一设备发 10 次（seq 递增），客户端应只收到 1 次且是最后一次
    for (int i = 1; i <= 10; ++i) {
        gw.inject("p-legacy", uavJson("merge-1", i, nowMs()), "10.0.0.1:1000");
    }
    check(waitFor([&] { return cap->eventCountOf("merge-1") >= 1; }, 2000), "合并后事件到达");
    std::this_thread::sleep_for(std::chrono::milliseconds(300));   // 等窗关闭

    checkEq(cap->eventCountOf("merge-1"), static_cast<std::size_t>(1),
            "窗内 10 次上报合并为 1 次");
    IngestEvent ev;
    check(cap->lastEventOf("merge-1", ev), "取到合并后的事件");
    checkEq(ev.normalized.seq, 10LL, "合并取的是最后一次（seq=10）");
}

/// S3 · ING-HLT-03：丢包率精确统计。
void testPacketLoss(Gateway& gw, const std::shared_ptr<Captured>&) {
    g_case = "S3/ING-HLT-03 丢包率精确统计";
    const long long ts = nowMs();
    // 发 seq 1..100 但跳过其中 10 个（10,20,...,100 中除 100 外都在范围内：
    // 实际跳过 10 个 seq，最大 seq = 99）→ 期望丢包率 = 10 / (99 - 1 + 1) ≈ 0.10101
    int skipped = 0;
    long long maxSeq = 0;
    for (int seq = 1; seq <= 100; ++seq) {
        if (seq % 10 == 0) { ++skipped; continue; }   // 该 seq 的包"丢"在网上了
        maxSeq = seq;
        gw.inject("p-legacy", uavJson("loss-1", seq, ts), "10.0.0.2:1000");
    }
    checkEq(skipped, 10, "注入了 10 个丢包");
    checkEq(maxSeq, 99LL, "最大 seq = 99（100 被跳过）");

    DeviceStats st;
    check(gw.deviceHealth("loss-1", st), "能查到 loss-1 的统计");
    check(st.seqPrecise, "有 seq → seqPrecise=true");
    checkEq(st.expected, 99LL, "期望包数 = maxSeq - minSeq + 1 = 99");
    checkEq(st.received, 90LL, "实收 90 包");
    // 口径：丢包率 = (maxSeq - minSeq + 1 - 实收) / (maxSeq - minSeq + 1) = 9/99
    checkNear(st.packetLossRate, 9.0 / 99.0, 0.001, "丢包率 ≈ 9/99 = 0.0909（口径一致）");
    check(st.toJson().contains("packetLossRate"), "序列化含 packetLossRate");

    DeviceInfo info;
    check(gw.deviceInfo("loss-1", info), "台账里有 loss-1（ING-HLT-05）");
    checkEq(info.hasSeq, true, "台账标了 seq 可用");
    checkEq(info.packets, 90, "台账累计包数 = 90");
}

/// S3 · ING-HLT-04：乱序精确统计；无 seq → 明确"不可精确统计"。
void testOutOfOrderAndNoSeq(Gateway& gw, const std::shared_ptr<Captured>&) {
    g_case = "S3/ING-HLT-04 乱序统计与不可精确统计";
    const long long ts = nowMs();
    // 1,2,5,3,4：seq=3 与 4 都是"后到且小于窗口内最大 seq" → 乱序 2 次
    const long long seqs[] = {1, 2, 5, 3, 4};
    for (long long s : seqs) gw.inject("p-legacy", uavJson("ooo-1", s, ts), "10.0.0.3:1000");

    DeviceStats st;
    check(gw.deviceHealth("ooo-1", st), "能查到 ooo-1 的统计");
    check(st.seqPrecise, "有 seq → 精确统计");
    checkEq(st.outOfOrderCount, 2LL, "乱序计数 = 2（人为打乱数一致）");
    checkEq(st.minSeq, 1LL, "minSeq = 1");
    checkEq(st.maxSeq, 5LL, "maxSeq = 5");
    checkEq(st.expected, 5LL, "期望包数 = 5");

    // 无 seq 的设备（raw 透传）：MUST 标注"不可精确统计"，禁止给估计值
    gw.inject("p-raw", std::string("abc", 3), "10.0.0.9:1000");
    DeviceStats raw;
    check(gw.deviceHealth("10.0.0.9:1000", raw), "能查到无 seq 设备的统计");
    check(!raw.seqPrecise, "无 seq → seqPrecise=false");
    const nlohmann::json j = raw.toJson();
    check(j["packetLossRate"].is_null(), "packetLossRate 返回 null（不给估计值）");
    check(j["outOfOrderRate"].is_null(), "outOfOrderRate 返回 null");
    check(!j.value("reason", std::string()).empty(), "给出不可精确统计的原因");
}

/// S3 · 契约 §4.2：seq 回绕/设备重启按新会话处理，不计为负丢包。
void testSeqWrap(Gateway& gw, const std::shared_ptr<Captured>&) {
    g_case = "S3/契约§4.2 seq 回绕按新会话";
    const long long ts = nowMs();
    gw.inject("p-legacy", uavJson("wrap-1", 400, ts), "10.0.0.4:1000");
    gw.inject("p-legacy", uavJson("wrap-1", 401, ts), "10.0.0.4:1000");
    gw.inject("p-legacy", uavJson("wrap-1", 402, ts), "10.0.0.4:1000");
    gw.inject("p-legacy", uavJson("wrap-1", 1, ts), "10.0.0.4:1000");   // 设备重启
    gw.inject("p-legacy", uavJson("wrap-1", 2, ts), "10.0.0.4:1000");

    DeviceStats st;
    check(gw.deviceHealth("wrap-1", st), "能查到 wrap-1 的统计");
    check(st.packetLossRate >= 0.0, "回绕后丢包率不为负");
    checkNear(st.packetLossRate, 0.0, 0.001, "新会话内无丢包 → 丢包率 0");
    checkEq(st.expected, 2LL, "新会话窗口期望包数 = 2（按新会话重置）");
}

/// S3 · ING-HLT-01/02/06：离线判定、恢复上线、健康事件上行。
/// 注意：本用例只断言**本设备**（off-1）的事件与台账；
/// 前面用例留下的设备也会超时离线，因此不能用"最近一条离线事件"来判断。
void testOfflineOnline(Gateway& gw, const std::shared_ptr<Captured>& cap) {
    g_case = "S3/ING-HLT-01/02/06 离线判定与恢复上线";
    cap->clear();

    auto offlineOf = [&](const char* id) {
        std::lock_guard<std::mutex> lk(cap->mtx);
        for (const auto& h : cap->health) {
            if (h.kind == DeviceHealthEvent::Kind::Offline && h.deviceId == id) return true;
        }
        return false;
    };
    auto onlineOf = [&](const char* id) {
        std::lock_guard<std::mutex> lk(cap->mtx);
        for (const auto& h : cap->health) {
            if (h.kind == DeviceHealthEvent::Kind::Online && h.deviceId == id) return true;
        }
        return false;
    };
    auto pickOffline = [&](const char* id, DeviceHealthEvent& out) {
        std::lock_guard<std::mutex> lk(cap->mtx);
        for (const auto& h : cap->health) {
            if (h.kind == DeviceHealthEvent::Kind::Offline && h.deviceId == id) {
                out = h;
                return true;
            }
        }
        return false;
    };
    auto pickOnline = [&](const char* id, DeviceHealthEvent& out) {
        // 取**最后一条**：新设备"发现即在线"也会发上线事件，
        // 本用例要的是"离线恢复"那条，因此必须取最新的。
        std::lock_guard<std::mutex> lk(cap->mtx);
        for (auto it = cap->health.rbegin(); it != cap->health.rend(); ++it) {
            if (it->kind == DeviceHealthEvent::Kind::Online && it->deviceId == id) {
                out = *it;
                return true;
            }
        }
        return false;
    };

    gw.inject("p-legacy", uavJson("off-1", 1, nowMs()), "10.0.0.5:1000");
    // 心跳周期 1 s × 倍数 3 = 3 s 超时；巡检 100 ms（ING-HLT-01 判定粒度 ≤ 1 s）
    const auto t0 = std::chrono::steady_clock::now();
    const bool offline = waitFor([&] { return offlineOf("off-1"); }, 6000, 50);
    const double elapsed =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    check(offline, "停发 3 s 后产生了 off-1 的 device.offline 事件"
                   "（实测 " + std::to_string(elapsed) + " s）");
    check(elapsed >= 2.5 && elapsed <= 4.5, "离线判定时延 ≈ 超时阈值（3 s）");

    DeviceHealthEvent off;
    check(pickOffline("off-1", off), "取到 off-1 的离线事件");
    checkEq(off.deviceId, std::string("off-1"), "离线事件带 deviceId");
    checkEq(off.reason, std::string("timeout"), "reason=timeout");
    checkNear(off.timeoutSec, 3.0, 0.001, "timeoutSec = 心跳周期 × 倍数 = 3");
    checkEq(off.toJson().value("online", true), false, "事件 online=false");

    // 台账里仍保留离线设备并标为离线（ING-HLT-05）
    DeviceInfo info;
    check(gw.deviceInfo("off-1", info), "离线设备仍在台账中");
    check(!info.online, "台账标为离线");
    check(info.offlineSec >= 2.5, "台账给出失联时长");

    // 恢复上报 → 上线事件，携带离线时长（ING-HLT-02）
    gw.inject("p-legacy", uavJson("off-1", 2, nowMs()), "10.0.0.5:1000");
    check(waitFor([&] { return onlineOf("off-1"); }, 3000, 20),
          "恢复上报后产生 off-1 的 device.online 事件");
    DeviceHealthEvent on;
    check(pickOnline("off-1", on), "取到 off-1 的上线事件");
    check(on.offlineSec > 0.0, "恢复上线事件带非零离线时长（实测 "
                               + std::to_string(on.offlineSec) + " s）");
    checkEq(on.reason, std::string("resumed"), "reason=resumed（区别于首次发现）");
    // 离线时长口径 = 判离线时刻 → 恢复上报时刻（本次约 0.3 s，巡检周期 100 ms）
    check(on.offlineSec < 2.0, "离线时长小于测试内等待时长（< 2 s）");
    check(gw.deviceInfo("off-1", info) && info.online, "台账恢复为在线");
}

/// S3 · ING-NFR-05：有界容器超限按最旧淘汰。
void testBoundedLedger(Gateway& gw, const std::shared_ptr<Captured>&) {
    g_case = "S3/ING-NFR-05 有界台账淘汰";
    const std::size_t before = gw.listDevices().size();

    IngestConfig cfg = gw.config();
    cfg.ledgerLimit = 5;
    std::string err;
    check(gw.reload(cfg, &err), "reload 生效（ledgerLimit=5）");
    checkEq(gw.config().ledgerLimit, static_cast<std::size_t>(5),
            "配置里的 ledgerLimit 已变为 5");

    const long long ts = nowMs();
    for (int i = 1; i <= 12; ++i) {
        gw.inject("p-legacy",
                  uavJson("bound-" + std::to_string(i), 1, ts), "10.0.0.6:1000");
    }
    const std::size_t after = gw.listDevices().size();
    check(after <= 5, "台账不超过上限（实际 " + std::to_string(after) + "）");
    check(after < before + 12, "确有淘汰发生（没有无限增长）");
    // 最旧的被淘汰、最新的保留
    DeviceInfo info;
    check(gw.deviceInfo("bound-12", info), "最新设备在台账里");
    check(!gw.deviceInfo("bound-1", info), "最旧设备已被淘汰");

    // 恢复上限，避免影响后续用例
    cfg.ledgerLimit = 20000;
    gw.reload(cfg, &err);
}

/// S3 · ING-ACC-06 + FAN-01：接入点指标与队列不阻塞接收。
void testMetricsAndQueue(Gateway& gw, const std::shared_ptr<Captured>&) {
    g_case = "S3/ING-ACC-06 + ING-FAN-01 指标与队列";
    const long long ts = nowMs();
    const auto before = gw.pointMetrics("p-legacy");
    const unsigned long long beforePackets = before.value("events", 0ull);
    for (int i = 0; i < 50; ++i) {
        gw.inject("p-legacy", uavJson("metric-" + std::to_string(i % 5), i + 1, ts),
                  "10.0.0.7:1000");
    }
    const auto after = gw.pointMetrics("p-legacy");
    checkEq(after.value("events", 0ull) - beforePackets, 50ull, "接入点事件计数 +50");
    check(after.value("packets", 0ull) > 0ull, "接入点到达包数被计数（inject 也算到达）");
    check(after.value("lastRecvAt", 0ll) > 0, "接入点指标带 lastRecvAt");
    checkEq(after.value("lastPeer", std::string()), std::string("10.0.0.7:1000"),
            "接入点指标带 lastPeer 溯源");
    check(after.contains("dropReasons"), "接入点指标带 dropReasons");

    const GatewayHealth gh = gw.gatewayHealth();
    check(gh.points >= 3, "总览里接入点数 ≥ 3");
    checkEq(gh.queueDropped, 0ull, "队列未因超限丢数据（上限 20000 / 实发 50）");

    const GatewayStatus stt = gw.status();
    checkEq(stt.running, true, "状态 running=true");
    checkEq(stt.parsers, static_cast<std::size_t>(3),
            "已注册解析器数 = 2 个内置 + 1 个测试解析器");
    check(stt.toJson().contains("hubEnabled"), "状态含 hubEnabled");
}

/// S3 · ING-ACC-03/04：热增删接入点，既有接入点不受影响。
void testHotAddRemove(Gateway& gw, const std::shared_ptr<Captured>&) {
    g_case = "S3/ING-ACC-03/04 热增删与故障隔离";
    const long long ts = nowMs();

    // 热增一个接入点
    PointConfig np;
    np.id = "p-hot";
    np.port = 45901;
    np.parserId = "raw.passthrough";
    np.deviceType = "hot";
    np.topic = "telemetry.hot";
    int code = 0;
    std::string err;
    check(gw.addPoint(np, &code, &err), "热增接入点成功");
    checkEq(code, 0, "错误码 0");
    checkEq(gw.inject("p-hot", std::string("zz", 2), "10.0.0.8:1000"), 1,
            "新接入点立刻可用");

    // 端口冲突：另一个接入点用同一端口 → 被拒，既有接入点不受影响
    PointConfig dup = np;
    dup.id = "p-dup";
    check(!gw.addPoint(dup, &code, &err), "端口被占用时拒绝新增");
    check(!err.empty(), "给出可读原因：" + err);
    checkEq(gw.inject("p-legacy", uavJson("hot-1", 1, ts), "10.0.0.7:1000"), 1,
            "端口冲突不影响既有接入点");

    // 移除热增的接入点
    check(gw.removePoint("p-hot", &code, &err), "热删接入点成功");
    checkEq(gw.inject("p-hot", std::string("zz", 2), "10.0.0.8:1000"), -1,
            "移除后注入返回 -1（接入点不存在）");
    check(!gw.removePoint("p-hot", &code, &err), "重复移除返回 false");
    checkEq(code, 2002, "错误码 2002（资源不存在）");
}

/// S1 · 中立 hub 客户端能观察广播（不依赖 Drogon）。
void testHubBroadcast(Gateway& gw, const std::shared_ptr<Captured>&) {
    g_case = "S1/hub 广播出口";
    class Counter final : public IHubClient {
    public:
        explicit Counter(std::shared_ptr<std::vector<std::string>> v) : v_(std::move(v)) {}
        void onMessage(const std::string& payload) override {
            std::lock_guard<std::mutex> lk(mtx_);
            v_->push_back(payload);
        }
    private:
        std::shared_ptr<std::vector<std::string>> v_;
        std::mutex mtx_;
    };
    auto seen = std::make_shared<std::vector<std::string>>();
    auto client = std::make_shared<Counter>(seen);
    EventHub::instance().addClient(client);

    checkEq(EventHub::instance().clientCount(), static_cast<std::size_t>(1),
            "客户端已登记");
    const auto before = EventHub::instance().broadcastCount();
    EventHub::instance().broadcast("test.event", {{"a", 1}, {"b", "x"}});
    checkEq(EventHub::instance().broadcastCount(), before + 1, "广播计数 +1");
    checkEq(seen->size(), static_cast<std::size_t>(1), "中立客户端收到 1 条");
    if (!seen->empty()) {
        const auto j = nlohmann::json::parse((*seen)[0]);
        checkEq(j.value("type", std::string()), std::string("test.event"), "信封 type 正确");
        checkEq(j.value("data", nlohmann::json::object()).value("a", 0), 1, "信封 data 正确");
        check(j.contains("ts") && j["ts"].is_number(), "信封带 epoch 毫秒 ts");
    }
    EventHub::instance().removeClient(client);
    checkEq(EventHub::instance().clientCount(), static_cast<std::size_t>(0), "注销后为 0");
    (void)gw;
}

/// 纯逻辑自测：有界台账的淘汰语义（不依赖 threads/网络）。
void testLedgerEvictionUnit() {
    g_case = "S3/有界台账淘汰（纯逻辑）";
    IngestConfig cfg;
    cfg.ledgerLimit = 5;
    cfg.healthWindowSec = 60;
    cfg.heartbeatPeriodMs = 1000;
    hlt::HealthTracker tracker(cfg);

    const Millis base = nowMs();
    std::vector<std::string> pushed;
    for (int i = 1; i <= 12; ++i) {
        const std::string id = "u-" + std::to_string(i);
        pushed.push_back(id);
        NormalizedEvent ev;
        ev.deviceId = id;
        ev.deviceType = "uav";
        ev.kind = "uav.pos";
        ev.recvAt = base + i * 10;   // 递增，保证"最旧"可判定
        ev.ts = ev.recvAt;
        tracker.push(ev);
    }
    checkEq(tracker.size(), static_cast<std::size_t>(5), "注入 12 条后台账只留 5 条");
    std::vector<DeviceInfo> infos = tracker.list(nowMs());
    checkEq(infos.size(), static_cast<std::size_t>(5), "list 返回 5 条");
    // list() 按 deviceId 排序（u-10 排在 u-8 前面），因此这里按集合判定
    std::vector<std::string> ids;
    for (const auto& d : infos) ids.push_back(d.deviceId);
    auto hasId = [&](const char* id) {
        for (const auto& s : ids) {
            if (s == id) return true;
        }
        return false;
    };
    check(hasId("u-8") && hasId("u-9") && hasId("u-10") && hasId("u-11") && hasId("u-12"),
          "保留的正是最新的 5 条（u-8..u-12）");
    check(!hasId("u-1") && !hasId("u-7"), "最早写入的已被淘汰（u-1 / u-7 都不在）");
    check(tracker.evictedTotal() >= 7, "淘汰计数被记录");

    // 上限调小：立即收缩
    tracker.setLedgerLimit(2);
    checkEq(tracker.size(), static_cast<std::size_t>(2), "调小上限后台账立即收缩到 2");
}

struct Case {
    const char* name;
    void (*fn)(Gateway&, const std::shared_ptr<Captured>&);
};

}  // namespace

int main(int argc, char** argv) {
    bool listOnly = false;
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--list") == 0) listOnly = true;
    }

    const std::vector<Case> cases = {
        {"S1/hub 广播出口",                     testHubBroadcast},
        {"S2/ING-PRS-05 既有 4 类 kind 兼容",   testLegacyCompat},
        {"S2/ING-NRM-02/03 归一化降级",         testNormalizeDegrade},
        {"S2/ING-PRS-03 原始透传",              testRawPassthrough},
        {"S2/ING-PRS-02/04 解析器注册与异常隔离", testParserIsolation},
        {"S3/ING-FAN-02 单帧合并",              testMergeWindow},
        {"S3/ING-HLT-03 丢包率精确统计",        testPacketLoss},
        {"S3/ING-HLT-04 乱序统计与不可精确统计", testOutOfOrderAndNoSeq},
        {"S3/契约§4.2 seq 回绕按新会话",        testSeqWrap},
        {"S3/ING-HLT-01/02/06 离线判定与恢复上线", testOfflineOnline},
        {"S3/ING-NFR-05 有界台账淘汰",          testBoundedLedger},
        {"S3/ING-ACC-06 + ING-FAN-01 指标与队列", testMetricsAndQueue},
        {"S3/ING-ACC-03/04 热增删与故障隔离",   testHotAddRemove},
    };

    if (listOnly) {
        for (const auto& c : cases) std::cout << c.name << std::endl;
        return 0;
    }

    std::cout << "=== device-ingest " << version() << " · 自测（"
              << (hub_enabled() ? "hub=ON" : "hub=OFF") << "）===" << std::endl;

    auto cap = std::make_shared<Captured>();
    auto sink = std::make_shared<TestSink>(cap);

    // 独立实例：不污染 Gateway::instance()
    Gateway gw;
    gw.registerParser(std::make_shared<ThrowingParser>());

    IngestConfig cfg = testConfig(45800, 100);
    if (!gw.start(cfg, sink, nullptr, nullptr)) {
        std::cerr << "接入层启动失败" << std::endl;
        return 2;
    }
    std::cout << "接入层已启动：接入点 " << gw.listPoints().size()
              << " 个，解析器 " << gw.parsers().size() << " 个" << std::endl;

    for (const auto& c : cases) {
        const int before = g_failed;
        g_case = c.name;
        c.fn(gw, cap);
        std::cout << (g_failed == before ? "  [通过] " : "  [失败] ") << c.name << std::endl;
    }

    // 不依赖 Gateway 的纯逻辑用例
    {
        const int before = g_failed;
        testLedgerEvictionUnit();
        std::cout << (g_failed == before ? "  [通过] " : "  [失败] ")
                  << g_case << std::endl;
    }

    gw.stop();

    std::cout << "----------------------------------------" << std::endl;
    std::cout << "断言 " << g_checks << " 项，失败 " << g_failed << " 项" << std::endl;
    for (const auto& f : g_failures) std::cout << "  ✗ " << f << std::endl;
    return g_failed == 0 ? 0 : 1;
}
