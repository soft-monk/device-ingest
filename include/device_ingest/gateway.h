// device-ingest · Gateway：模块门面（宿主唯一入口）
//
// 设计方案 §7 对外接口（模块公开面，逐项）：
//   门面      Gateway::instance()：start / stop / reload / status / inject(测试注入)
//   接入点    addPoint / removePoint / listPoints / pointMetrics
//   解析器    ParserRegistry::add / list（经 parsers() 取用）
//   设备      listDevices / deviceHealth / gatewayHealth
//   指令      sendCommand / commandState / listCommands / retryCommand / commandLogs
//   广播      EventHub::instance()（签名与现状一致）
//
// 保留单例形态（设计方案 §11）：主仓 main.cc:224 的
//   Telemetry::instance().start(...)  →  Gateway::instance().start(cfg, sink, logSink, src)
// 是一次直接替换，不需要改调用风格。
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "device_ingest/config.h"
#include "device_ingest/device_source.h"
#include "device_ingest/parser.h"
#include "device_ingest/sink.h"
#include "device_ingest/types.h"

namespace device_ingest {

/// 门面运行状态快照。
struct GatewayStatus {
    bool        running = false;
    bool        enabled = true;      ///< 配置里的 ingest.enabled
    std::size_t points = 0;
    std::size_t pointsRunning = 0;
    std::size_t devices = 0;
    std::size_t online = 0;
    std::size_t parsers = 0;
    std::uint64_t packets = 0;
    std::uint64_t events = 0;
    std::uint64_t dropped = 0;
    std::uint64_t parseFailed = 0;
    bool        hubEnabled = false;  ///< 是否编译了 WS 广播层
    std::string version;

    nlohmann::json toJson() const;
};

/// 指令下发请求（契约 §4.3 下发请求体）。
struct CommandRequest {
    std::string targetDeviceId;  ///< 二选一：单播到设备
    std::string targetIngestId;  ///< 二选一：组播到某接入点
    std::string type;            ///< 指令类型（设备指令集定义，待确认项）
    nlohmann::json payload = nlohmann::json::object();
    int         timeoutMs = 0;   ///< 0 = 用全局 cmdTimeoutMs
    int         maxRetries = -1; ///< <0 = 用全局 cmdMaxRetries
    std::string actor;           ///< 谁发的（写入操作日志）
};

/// 指令操作日志条目（契约 §4.3 /logs；ING-CMD-06）。
struct CommandLogEntry {
    Millis         at = 0;
    std::string    actor;
    std::string    cmdId;
    std::string    deviceId;
    std::string    action;         ///< create | send | retry | ack | fail | timeout | drop
    std::string    payloadDigest;  ///< 载荷摘要（不落全文）
    std::string    result;
    nlohmann::json toJson() const;
};

/**
 * 外设接入层门面。
 *
 * 线程模型：start() 之后模块内部有
 *   - 每接入点 1 个接收线程（acc）
 *   - 1 个离线巡检线程（hlt）
 *   - 1 个队列消费线程（fan，做单帧合并与分发）
 *   - 1 个指令超时巡检线程（cmd，S4 起启用）
 * 所有公开查询方法都可在任意线程调用。
 */
class Gateway {
public:
    /// 进程级单例（与现状 Telemetry::instance() 对应，便于主仓直接替换）。
    static Gateway& instance();

    /// 独立实例（测试/多接入层并行用）。不注册内置解析器之外的任何东西。
    Gateway();
    ~Gateway();
    Gateway(const Gateway&) = delete;
    Gateway& operator=(const Gateway&) = delete;

    // ---------------------------------------------------------------- 生命周期
    /// 启动：按配置拉起接入点、健康巡检、队列消费。
    /// sink 可为空（此时只广播到 hub、只维护台账）；logSink / source 可留空。
    /// cfg.points 为空时自动回落单接入点（config::fallbackPoint）。
    /// 返回 false 表示整体未启动（cfg.enabled == false，或没有任何接入点成功启动）。
    bool start(const IngestConfig& cfg,
               std::shared_ptr<ISink>    sink = nullptr,
               std::shared_ptr<ILogSink> logSink = nullptr,
               std::shared_ptr<IDeviceSource> source = nullptr);

    /// 停机：停接收线程、排空队列、落台账。可重复调用。
    void stop();

    /// 运行中重载配置：**增量**处理接入点（新增/移除/重启变化的），既有接入点不丢包。
    /// 返回 false 时 error 给可读原因，且已生效的部分不回滚（与契约 §9 降级矩阵一致）。
    bool reload(const IngestConfig& cfg, std::string* error = nullptr);

    bool running() const;
    const IngestConfig& config() const;

    // ---------------------------------------------------------------- 接入点（ING-ACC-02/03）
    /// 热增接入点（运行中生效）。失败返回错误码与可读原因（契约 §8）。
    bool addPoint(const PointConfig& point, int* code = nullptr, std::string* error = nullptr);
    /// 热删接入点；删除后不再收包且不影响其它接入点。
    bool removePoint(const std::string& id, int* code = nullptr, std::string* error = nullptr);
    /// 接入点配置 + 运行状态（契约 §4.1 GET /points）。
    std::vector<nlohmann::json> listPoints() const;
    /// 接入点级指标（ING-ACC-06）；id 不存在返回空对象。
    nlohmann::json pointMetrics(const std::string& id) const;
    /// 全部接入点指标快照。
    std::vector<PointMetrics> allPointMetrics() const;

    // ---------------------------------------------------------------- 测试注入（保留既有 injectPacket 语义）
    /// 单包入口：把一段字节当作"从 peer 收到"喂进解析→归一→分发链路，
    /// 不经过网络。用于单测与工具（设计方案 §2 末尾 / §11 inject 行）。
    /// 返回产出的归一化事件数；-1 = 指定接入点不存在。
    int inject(const std::string& ingestId, const std::string& bytes,
               const std::string& peer = "127.0.0.1:0");

    // ---------------------------------------------------------------- 解析器（ING-PRS-02）
    ParserRegistry& parsers();

    /// 注册一个解析器并广播到全部接入点（编译期静态注册的运行时等价物）。
    bool registerParser(ParserPtr parser);

    // ---------------------------------------------------------------- 设备健康（ING-HLT-01~06）
    std::vector<DeviceInfo> listDevices() const;
    /// 单设备台账；不存在返回 false。
    bool deviceInfo(const std::string& deviceId, DeviceInfo& out) const;
    /// 单设备统计（契约 §4.2 /devices/{id}/health）；不存在返回 false。
    bool deviceHealth(const std::string& deviceId, DeviceStats& out) const;
    /// 接入层总览（契约 §4.2 /health）。
    GatewayHealth gatewayHealth() const;
    /// 门面状态快照。
    GatewayStatus status() const;

    // ---------------------------------------------------------------- 指令（ING-CMD-01~06，S4 完整实现）
    /// 下发指令，返回 cmdId；失败时 cmdId 为空且 code/error 给原因（契约 §8）。
    std::string sendCommand(const CommandRequest& req, int* code = nullptr,
                            std::string* error = nullptr);
    bool commandState(const std::string& cmdId, CommandRecord& out) const;
    std::vector<CommandRecord> listCommands(const std::string& deviceId = std::string(),
                                            CommandState* stateFilter = nullptr,
                                            std::size_t limit = 100) const;
    bool retryCommand(const std::string& cmdId);
    std::vector<CommandLogEntry> commandLogs(const std::string& deviceId = std::string(),
                                             std::size_t limit = 100) const;

    /// 清空台账 / 队列 / 指标（测试复位用；不影响运行中的接入点）。
    void resetState();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

/// 便捷函数：解析一段配置文本并启动进程级单例。
/// 给 examples/host_demo 与压测工具用，宿主可以完全不用它。
bool startFromConfigText(const std::string& configText,
                         std::shared_ptr<ISink> sink = nullptr,
                         std::shared_ptr<ILogSink> logSink = nullptr,
                         std::shared_ptr<IDeviceSource> source = nullptr,
                         std::string* error = nullptr);

}  // namespace device_ingest
