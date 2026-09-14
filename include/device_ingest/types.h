// device-ingest · 对外数据类型（归一化对象、台账、事件）
//
// 契约依据：《外设接入契约》§2 归一化对象、§4.2 台账、§5 事件名、§6 指令回执。
//
// 一条硬规矩（契约 §2）：**归一化对象是接入层对外的唯一数据形状**。
// 字段缺失按契约降级，不报错；非法值只丢该字段并计数，事件仍上行。
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

namespace device_ingest {

/// 时间戳口径：epoch 毫秒（整数）。
using Millis = std::int64_t;

/// 当前墙钟（epoch 毫秒）。全模块唯一的取时入口，便于测试替换。
Millis nowMs();

/// 与契约 §2 的字段名一一对应，避免各处手写字符串写错。
namespace field {
inline constexpr const char* kDeviceId   = "deviceId";
inline constexpr const char* kDeviceType = "deviceType";
inline constexpr const char* kKind       = "kind";
inline constexpr const char* kLng        = "lng";
inline constexpr const char* kLat        = "lat";
inline constexpr const char* kAlt        = "alt";
inline constexpr const char* kHeading    = "heading";
inline constexpr const char* kSpeed      = "speed";
inline constexpr const char* kBattery    = "battery";
inline constexpr const char* kSeq        = "seq";
inline constexpr const char* kTs         = "ts";
inline constexpr const char* kTsSource   = "tsSource";
inline constexpr const char* kRecvAt     = "recvAt";
inline constexpr const char* kQuality    = "quality";
inline constexpr const char* kSource     = "source";
inline constexpr const char* kRaw        = "raw";
inline constexpr const char* kIngestId   = "ingestId";
inline constexpr const char* kPeer       = "peer";
inline constexpr const char* kBytes      = "bytes";
}  // namespace field

/// `tsSource` 取值（契约 §2 / ING-NRM-03）。
enum class TsSource { Device, Server };

inline const char* toString(TsSource s) {
    return s == TsSource::Device ? "device" : "server";
}

/// 解析器的输出：**还没归一化**的业务字段。
/// 解析器 MUST 无状态、可重入（ING-PRS-01），因此这里只放数据、不放解析器状态。
struct ParsedEvent {
    std::string    deviceId;   ///< 空 = 解析器没给，归一化时用 source.peer 兜底
    std::string    kind;       ///< 事件种类，如 "uav.pos"（契约 §5）
    nlohmann::json fields = nlohmann::json::object();  ///< 业务字段（lng/lat/alt/...）
    Millis         ts  = 0;    ///< 设备时间戳；0 = 缺失，归一化时用到达时刻补齐
    long long      seq = -1;   ///< 设备序号；-1 = 缺失（丢包/乱序转"不可精确统计"）
    nlohmann::json quality;    ///< 可选，质量附加字段
    /// 可选：十六进制摘要。仅"原始透传"或显式开启溯源时出现（契约 §2 `raw`）。
    nlohmann::json raw;
    bool           rawPassthrough = false;  ///< true = 原始透传事件（契约 §7 raw.passthrough）
};

/// 溯源信息（契约 §2 `source` / ING-NRM-04）。
struct EventSource {
    std::string ingestId;  ///< 接入点 id
    std::string peer;      ///< 来源 "ip:port"
    std::size_t bytes = 0; ///< 原始包长度
};

/// 归一化对象（契约 §2）。**接入层对外唯一的数据形状。**
struct NormalizedEvent {
    std::string deviceId;              ///< MUST；缺失时用 source.peer 兜底并计数
    std::string deviceType;            ///< MUST，接入点默认设备类型
    std::string kind;                  ///< MUST，事件种类
    bool        hasLng = false;        ///< 非法/越界 → 该字段整块缺失（不报错）
    bool        hasLat = false;
    double      lng = 0.0;
    double      lat = 0.0;
    bool        hasAlt = false;        double alt = 0.0;
    bool        hasHeading = false;    double heading = 0.0;   ///< 0–360 度
    bool        hasSpeed = false;      double speed = 0.0;     ///< m/s
    bool        hasBattery = false;    int    battery = 0;     ///< 0–100
    bool        hasSeq = false;        long long seq = -1;     ///< 无 seq → 不可精确统计
    Millis      ts = 0;                ///< MUST，epoch 毫秒
    TsSource    tsSource = TsSource::Server;  ///< MUST，"device" | "server"
    Millis      recvAt = 0;            ///< MUST，服务端到达时刻
    nlohmann::json quality;            ///< 可选
    EventSource source;                ///< MUST，溯源信息
    nlohmann::json raw;                ///< 可选，仅原始透传或显式开启溯源时出现

    /// 按契约 §2 序列化。**只输出契约里有的字段**，缺什么就不出什么键。
    nlohmann::json toJson() const;
};

/// 归一化事件的投递单元：事件名（来自接入点 topic）+ 数据。
/// 这是 ISink::onEvent / onBatch 的参数形态（设计方案 §3.2 / §6）。
struct IngestEvent {
    std::string    type;    ///< WS 事件名，如 "telemetry.uav.pos"
    std::string    ingestId;
    nlohmann::json data;    ///< 契约 §5 的 data 关键字段（必要时含归一字段）
    NormalizedEvent normalized;  ///< 归一对象本体（供宿主台账/落库用，不发给前端）
};

/// 设备台账条目（契约 §4.2 / ING-HLT-05）。
struct DeviceInfo {
    std::string deviceId;
    std::string deviceType;
    Millis      firstSeen = 0;
    Millis      lastSeen = 0;
    bool        online = false;
    double      offlineSec = 0.0;
    bool        hasLastPos = false;
    double      lastLng = 0.0;
    double      lastLat = 0.0;
    bool        hasLastAlt = false;
    double      lastAlt = 0.0;
    std::string ingestId;      ///< 最近一次上报的接入点
    std::string peer;          ///< 最近一次来源地址
    bool        hasSeq = false;
    long long   lastSeq = -1;
    int         packets = 0;

    nlohmann::json toJson(Millis now) const;
};

/// 设备健康事件（契约 §5 device.online / device.offline / node.state / device.stats）。
struct DeviceHealthEvent {
    enum class Kind { Online, Offline, Stats };
    Kind        kind = Kind::Online;
    std::string deviceId;
    std::string deviceType;
    bool        online = false;
    double      offlineSec = 0.0;   ///< Online 时=离线时长；Offline 时=已失联时长
    Millis      lastSeen = 0;
    double      timeoutSec = 0.0;
    std::string reason;             ///< 如 "timeout"
    // device.stats 用（契约 §5）
    bool        seqPrecise = true;
    double      packetLossRate = 0.0;
    double      outOfOrderRate = 0.0;
    int         windowSec = 60;

    nlohmann::json toJson() const;
};

/// 丢包/乱序统计结果（契约 §4.2 / ING-HLT-03·04）。
/// seqPrecise=false 时 packetLossRate / outOfOrderRate 为 null，
/// 并在 reason 写明原因——**禁止给估计值冒充精确值**。
struct DeviceStats {
    std::string deviceId;
    bool        online = false;
    bool        seqPrecise = true;
    bool        hasLossRate = false;   ///< false → 序列化为 null
    double      packetLossRate = 0.0;  ///< 0..1
    long long   outOfOrderCount = 0;
    bool        hasOutOfOrderRate = false;
    double      outOfOrderRate = 0.0;
    long long   received = 0;
    long long   expected = 0;          ///< maxSeq - minSeq + 1
    long long   minSeq = 0;
    long long   maxSeq = 0;
    int         windowSec = 60;
    double      timeoutSec = 0.0;
    std::string reason;                ///< 不可精确统计的原因（人可读）

    nlohmann::json toJson() const;
};

/// 指令状态（契约 §4.3 状态机）。
enum class CommandState { Pending, Sent, Acked, Failed, Timeout };

inline const char* toString(CommandState s) {
    switch (s) {
        case CommandState::Pending: return "pending";
        case CommandState::Sent:    return "sent";
        case CommandState::Acked:   return "acked";
        case CommandState::Failed:  return "failed";
        case CommandState::Timeout: return "timeout";
    }
    return "pending";
}

/// 指令记录（契约 §4.3 / ING-CMD-05·06）。
struct CommandRecord {
    std::string  cmdId;
    std::string  deviceId;      ///< 单播目标（与 ingestId 二选一）
    std::string  ingestId;      ///< 组播目标（接入点）
    std::string  type;          ///< 指令类型（设备指令集定义，待确认项）
    nlohmann::json payload = nlohmann::json::object();
    std::string  actor;         ///< 谁发的（写入操作日志）
    CommandState state = CommandState::Pending;
    int          retries = 0;
    int          maxRetries = 2;
    int          timeoutMs = 1000;
    std::string  target;        ///< 实际发送地址 "ip:port"（可读）
    Millis       createdAt = 0;
    Millis       sentAt = 0;
    Millis       ackedAt = 0;
    Millis       finishedAt = 0;
    std::string  detail;        ///< 可读原因 / 回执 detail

    /// 时间线（契约 §4.3）：每次发送/重传/回执都追加一条。
    struct TimelineEntry {
        Millis      at = 0;
        std::string event;   ///< created | sent | retry | acked | failed | timeout
        std::string detail;
    };
    std::vector<TimelineEntry> timeline;

    nlohmann::json toJson() const;
};

/// 指令状态变化事件（契约 §5 command.state）。
struct CommandStateEvent {
    std::string  cmdId;
    std::string  deviceId;
    CommandState state = CommandState::Pending;
    int          retries = 0;
    std::string  detail;
};

/// 告警事件（契约 §5 alert，接入点故障、丢包超阈值、无主回执用此）。
struct AlertEvent {
    enum class Level { Info, Warn, Critical };
    Level       level = Level::Warn;
    std::string code;    ///< 机器可读，如 "point_bind_failed"
    std::string title;
    std::string text;
    std::string ingestId;

    nlohmann::json toJson() const;
};

/// 接入点运行指标（契约 §4.1 / ING-ACC-06）。
struct PointMetrics {
    std::string id;
    bool        running = false;
    std::uint64_t packets = 0;      ///< 到达包数
    std::uint64_t bytes = 0;        ///< 字节数
    std::uint64_t dropped = 0;      ///< 丢弃数（超长截断 + 空包）
    std::uint64_t truncated = 0;    ///< 超 8 KB 被截断
    std::uint64_t empty = 0;        ///< 空包
    std::uint64_t parseFailed = 0;  ///< 解析失败数
    std::uint64_t events = 0;       ///< 产出的归一化事件数
    Millis      lastRecvAt = 0;
    std::string lastPeer;
    std::string lastError;          ///< 不可读原因（bind/join/未注册解析器）

    nlohmann::json toJson() const;
};

/// 接入层总览（契约 §4.2 /api/v1/ingest/health）。
struct GatewayHealth {
    std::size_t points = 0;
    std::size_t pointsRunning = 0;
    std::size_t devices = 0;
    std::size_t online = 0;
    std::uint64_t packets = 0;
    std::uint64_t events = 0;
    std::uint64_t dropped = 0;
    std::uint64_t parseFailed = 0;
    double      packetsPerSec = 0.0;
    std::uint64_t queueDepth = 0;
    std::uint64_t queueDropped = 0;

    nlohmann::json toJson() const;
};

}  // namespace device_ingest
