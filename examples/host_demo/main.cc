// examples/host_demo · 最小宿主（设计方案 §3 目录结构 / 目标 G2）
//
// 它证明的是**目标 G2**：新仓可独立运行一个最小宿主，不连主仓数据库——
//   运行后能用工具发 UDP，看到事件被打印/广播。
//
// 这个文件同时是"宿主怎么接模块"的活样板，共四步：
//   ① 实现 ISink（数据出口）
//   ② 实现 ILogSink（日志出口，替代现状直接写 event_log 表那条隐线）
//   ③ 组装 IngestConfig（可来自 config.json / 环境变量 / 命令行）
//   ④ Gateway::instance().start(cfg, sink, log, source)
//
// 用法：
//   host_demo                          # 用内置演示配置（回环 3 个接入点）
//   host_demo --config config.json     # 读配置文件（契约 §3 结构）
//   host_demo --port 45454 --seconds 0 # 只跑一个接入点，0 = 一直跑到 Ctrl+C
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <nlohmann/json.hpp>

// Windows 的 <objbase.h> 会把 `interface` 定义成 `struct` 宏（为兼容 COM 的老代码）。
// 本模块的公开头里有名为 `interface` 的配置字段，宿主若先包含了 windows.h
// （很多宿主都会），下面这行能防止它被宏替换成语法垃圾。
#if defined(_MSC_VER)
#pragma push_macro("interface")
#pragma push_macro("small")
#undef interface
#undef small
#endif
#include "device_ingest/gateway.h"
#include "device_ingest/hub.h"
#include "device_ingest/version.h"
#if defined(_MSC_VER)
#pragma pop_macro("small")
#pragma pop_macro("interface")
#endif

using namespace device_ingest;

namespace {

std::atomic<long long> g_events{0};
std::atomic<long long> g_health{0};
std::atomic<long long> g_alerts{0};
std::atomic<long long> g_hubMessages{0};

/// ① 数据出口：模块把归一化事件交给宿主。
/// 主仓的落点会是 backend/src/ingest_bridge/Sink.cc（转 EventHub::broadcast + 落库）。
class ConsoleSink final : public ISink {
public:
    void onEvent(const IngestEvent& ev) override { ++g_events; }

    void onBatch(const std::vector<IngestEvent>& evs) override {
        for (const auto& ev : evs) {
            ++g_events;
            std::cout << "[" << ev.type << "] " << ev.data.dump() << std::endl;
        }
    }

    void onDeviceHealth(const DeviceHealthEvent& ev) override {
        ++g_health;
        const char* what = ev.kind == DeviceHealthEvent::Kind::Online  ? "上线"
                         : ev.kind == DeviceHealthEvent::Kind::Offline ? "离线"
                                                                       : "统计";
        std::cout << "[health/" << what << "] " << ev.toJson().dump() << std::endl;
    }

    void onCommandState(const CommandStateEvent& ev) override {
        std::cout << "[command] " << ev.cmdId << " -> " << toString(ev.state) << std::endl;
    }

    void onAlert(const AlertEvent& ev) override {
        ++g_alerts;
        std::cout << "[alert] " << ev.toJson().dump() << std::endl;
    }
};

/// ② 日志出口：模块内**不出现任何 SQL**，落库由宿主在这里实现。
class ConsoleLog final : public ILogSink {
public:
    explicit ConsoleLog(bool verbose) : verbose_(verbose) {}
    void log(const std::string& category, const nlohmann::json& payload) override {
        if (!verbose_) return;
        std::cout << "[log/" << category << "] " << payload.dump() << std::endl;
    }
    void commandAudit(const CommandRecord& rec) override {
        std::cout << "[audit] " << rec.toJson().dump() << std::endl;
    }

private:
    bool verbose_ = false;
};

/// hub 的中立客户端：不引入任何 WS 框架也能看到广播内容。
class CountingHubClient final : public IHubClient {
public:
    void onMessage(const std::string& payload) override {
        ++g_hubMessages;
        if (g_hubMessages <= 5) std::cout << "[hub] " << payload << std::endl;
    }
};

/// 内置演示配置：三个接入点并行（ING-ACC-01 的最小可复现形态）。
/// 端口全部落在高端口区间，避开 8080（HTTP/WS）与 8090（AI 桥）。
std::string demoConfigJson(int basePort, bool withRaw, bool emitNormalized) {
    nlohmann::json cfg;
    cfg["ingest"]["enabled"] = true;
    cfg["ingest"]["mergeWindowMs"] = 100;
    cfg["ingest"]["defaultTimeoutMultiple"] = 3;
    cfg["ingest"]["healthWindowSec"] = 60;
    cfg["ingest"]["heartbeatPeriodMs"] = 1000;
    // 默认 false：归一化事件只经 ISink 交给宿主（契约里新设备类型的事件名由接入点 topic 声明，
    // 不是"所有事件都往 WS 推"）。--emit-normalized 打开后额外广播一份，便于观察链路。
    cfg["ingest"]["emitRawEvents"] = emitNormalized;

    nlohmann::json pts = nlohmann::json::array();

    // 接入点 1：既有 4 类 kind（兼容解析器），收到什么 kind 就发什么事件名
    pts.push_back({
        {"id", "ingest-legacy"},
        {"group", ""},
        {"port", basePort},
        {"parserId", "legacy.kind.v1"},
        {"deviceType", "legacy"},
        {"topic", ""},
    });

    // 接入点 2：协议未定的设备（原始透传，ING-PRS-03）
    if (withRaw) {
        pts.push_back({
            {"id", "ingest-raw"},
            {"group", ""},
            {"port", basePort + 1},
            {"parserId", "raw.passthrough"},
            {"deviceType", "unknown"},
            {"topic", "telemetry.raw"},
        });
    }

    // 接入点 3：新设备类型示例（同一解析器，显式声明 topic → 前端无需改代码即可收到）
    pts.push_back({
        {"id", "ingest-uav-b"},
        {"group", ""},
        {"port", basePort + 2},
        {"parserId", "legacy.kind.v1"},
        {"deviceType", "uav"},
        {"topic", "telemetry.uav.pos"},
    });

    cfg["ingest"]["points"] = pts;
    return cfg.dump(2);
}

void printBanner(const IngestConfig& cfg) {
    std::cout << "=== device-ingest " << version()
              << " · host_demo（最小宿主）===" << std::endl;
    std::cout << "hub 层编译开关 : " << (hub_enabled() ? "ON（Drogon）" : "OFF（纯采集模式）")
              << std::endl;
    std::cout << "合并窗口       : " << cfg.mergeWindowMs << " ms（0 = 逐包直发）" << std::endl;
    std::cout << "失联倍数       : " << cfg.defaultTimeoutMultiple
              << " × " << cfg.heartbeatPeriodMs << " ms" << std::endl;
    std::cout << "归一事件广播   : " << (cfg.emitRawEvents ? "开" : "关（只经 ISink）")
              << std::endl;
    std::cout << "接入点         : " << cfg.points.size() << " 个" << std::endl;
    for (const auto& p : cfg.points) {
        std::cout << "   - " << p.id << "  :" << p.port
                  << "  解析器=" << p.parserId
                  << "  事件名=" << (p.topic.empty() ? "(由 kind 决定)" : p.topic)
                  << std::endl;
    }
    std::cout << "----------------------------------------" << std::endl;
}

}  // namespace

int main(int argc, char** argv) {
    std::string configPath;
    std::string reportPath;
    int  basePort = 45500;
    int  seconds = 0;          // 0 = 一直跑
    bool verbose = false;
    bool noRaw = false;
    bool emitNormalized = false;

    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto next = [&](int& out) { if (i + 1 < argc) out = std::atoi(argv[++i]); };
        if (a == "--config" && i + 1 < argc)      configPath = argv[++i];
        else if (a == "--report" && i + 1 < argc) reportPath = argv[++i];
        else if (a == "--port")                   next(basePort);
        else if (a == "--seconds")                next(seconds);
        else if (a == "--verbose")                verbose = true;
        else if (a == "--no-raw")                 noRaw = true;
        else if (a == "--emit-normalized")        emitNormalized = true;
        else if (a == "--help" || a == "-h") {
            std::cout << "用法: host_demo [--config 文件] [--port 起始端口] "
                         "[--seconds 运行秒数] [--verbose] [--no-raw] "
                         "[--emit-normalized] [--report 报告文件]" << std::endl;
            return 0;
        }
    }

    IngestConfig cfg;
    if (!configPath.empty()) {
        std::string err;
        if (!config::loadFile(configPath, cfg, err)) {
            std::cerr << "配置读取失败: " << err << std::endl;
            return 2;
        }
    } else {
        std::string err;
        if (!config::fromJsonString(demoConfigJson(basePort, !noRaw, emitNormalized), cfg, err)) {
            std::cerr << "内置演示配置非法: " << err << std::endl;
            return 2;
        }
    }
    config::applyEnvOverrides(cfg);
    if (emitNormalized) cfg.emitRawEvents = true;   // 显式覆盖配置文件里的取值

    printBanner(cfg);

    // 中立客户端：让 host_demo 也能验证"广播这一路"是通的（S1 验收用）
    auto counter = std::make_shared<CountingHubClient>();
    EventHub::instance().addClient(counter);

    auto sink = std::make_shared<ConsoleSink>();
    auto log  = std::make_shared<ConsoleLog>(verbose);

    // ④ 启动（设计方案 §7 门面）
    if (!Gateway::instance().start(cfg, sink, log, nullptr)) {
        // 启动失败有两种：端口全被占用（运维问题），或当前阶段数据面尚未接入。
        // 后者是正常的阶段状态（S0 骨架期），不是错误退出。
        const GatewayStatus st = Gateway::instance().status();
        std::cout << "接入层未启动："
                  << (st.parsers == 0 ? "当前构建为骨架阶段（数据面未接入）"
                                      : "没有任何接入点成功启动（查上面的 [alert] 与 [log/acc]）")
                  << std::endl;
        return st.parsers == 0 ? 0 : 3;
    }

    std::cout << "接入层已启动。用 tools/device_sim 发一包试试：" << std::endl;
    std::cout << "  device_sim --port " << cfg.points.front().port
              << " --kind uav.pos --devices 3 --hz 1 --seconds 5" << std::endl;
    std::cout << "----------------------------------------" << std::endl;

    const auto t0 = std::chrono::steady_clock::now();
    while (true) {
        if (seconds > 0) {
            const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                                     std::chrono::steady_clock::now() - t0).count();
            if (elapsed >= seconds) break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    // 状态快照：验收时直接读这段输出
    const GatewayStatus st = Gateway::instance().status();
    const GatewayHealth gh = Gateway::instance().gatewayHealth();
    std::cout << "----------------------------------------" << std::endl;
    std::cout << "状态: " << st.toJson().dump() << std::endl;
    std::cout << "总览: " << gh.toJson().dump() << std::endl;
    std::cout << "事件 " << g_events.load()
              << " · 健康 " << g_health.load()
              << " · 告警 " << g_alerts.load()
              << " · 广播 " << EventHub::instance().broadcastCount()
              << "（中立客户端收到 " << g_hubMessages.load() << "）" << std::endl;

    // 台账逐条打印（ING-HLT-05）
    const auto devices = Gateway::instance().listDevices();
    std::cout << "设备台账（" << devices.size() << " 条）:" << std::endl;
    for (const auto& d : devices) {
        std::cout << "  " << d.toJson(nowMs()).dump() << std::endl;
    }

    // 机器可读报告：给 scripts/acceptance.ps1 之类的验收脚本读（人不必看它）
    if (!reportPath.empty()) {
        nlohmann::json report;
        report["version"]   = version();
        report["hubEnabled"] = hub_enabled();
        report["status"]    = st.toJson();
        report["health"]    = gh.toJson();
        report["sinkEvents"] = g_events.load();
        report["hubBroadcasts"] = EventHub::instance().broadcastCount();

        nlohmann::json pts = nlohmann::json::array();
        for (const auto& p : Gateway::instance().listPoints()) pts.push_back(p);
        report["points"] = pts;

        nlohmann::json devs = nlohmann::json::array();
        for (const auto& d : devices) {
            nlohmann::json item = d.toJson(nowMs());
            DeviceStats ds;
            if (Gateway::instance().deviceHealth(d.deviceId, ds)) {
                item["stats"] = ds.toJson();   // 丢包率/乱序/是否可精确统计
            }
            devs.push_back(std::move(item));
        }
        report["devices"] = devs;

        std::ofstream out(reportPath, std::ios::binary | std::ios::trunc);
        if (out) {
            out << report.dump(2) << std::endl;
            std::cout << "报告已写入: " << reportPath << std::endl;
        } else {
            std::cerr << "报告写入失败: " << reportPath << std::endl;
        }
    }

    Gateway::instance().stop();
    EventHub::instance().removeClient(counter);
    return 0;
}
