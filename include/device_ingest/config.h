// device-ingest · 接入点与接入层配置（纯结构体，无外部依赖）
//
// 契约依据：《外设接入契约》§3 接入点配置
//   - 全部来自配置文件/环境变量，模块内**不硬编码**任何地址与端口（ING-ACC-02）；
//   - 未配置 points 时回落既有单接入点（契约 §3 降级约定 / §9 降级矩阵）。
//
// 本文件只描述"形状"；把 JSON / 环境变量翻译成这个结构是宿主的事
// （模块提供 config::loadFile / config::fromJson 作为便利实现，宿主可不用）。
#pragma once

#include <string>
#include <vector>

#include <nlohmann/json.hpp>

namespace device_ingest {

/// 一条接入点 = 一个「组播地址:端口」+ 一个解析器 + 一个目标 topic。
/// 硬约束：**一个端口只归一个接入点**（需求专篇 R6）。
struct PointConfig {
    std::string id;          ///< MUST 唯一，如 "ingest-uav-a"
    std::string group;       ///< 组播地址；空 = 收单播
    int         port = 0;    ///< MUST > 0
    std::string iface;       ///< 可选，绑定网卡地址（多网卡环境建议显式指定）
    std::string parserId;    ///< MUST 已注册的解析器，如 "uav.json.v1"
    std::string deviceType;  ///< 该接入点的默认设备类型，如 "uav"
    std::string topic;       ///< 归一后事件名（契约 §5）
    bool        enabled = true;
    int         timeoutMultiple = 0;  ///< 0 = 用全局 defaultTimeoutMultiple

    /// 单条接入点是否自洽。返回空字符串 = 合法；否则为可读原因（契约 §8 code=2001）。
    std::string validate() const;
};

/// 接入层全局配置。
struct IngestConfig {
    bool   enabled = true;              ///< false → 模块整体不启动（契约 §8 code=5001）
    int    mergeWindowMs = 100;         ///< ING-FAN-02；0 = 逐包直发
    int    defaultTimeoutMultiple = 3;  ///< ING-HLT-01，失联 = 周期 × 倍数
    int    healthWindowSec = 60;        ///< ING-HLT-03 丢包/乱序统计窗口
    int    heartbeatPeriodMs = 1000;    ///< 设备标称上报周期（用于换算超时）
    int    offlineCheckIntervalMs = 200;///< 离线巡检粒度（ING-HLT-01 判定粒度 ≤ 1 s）
    int    cmdTimeoutMs = 1000;         ///< ING-CMD-04 指令回执超时
    int    cmdMaxRetries = 2;           ///< ING-CMD-04 最大重传次数
    int    statsPushIntervalMs = 5000;  ///< device.stats 上行周期（契约 §5）
    bool   emitRawEvents = false;       ///< 是否把归一事件也发到 hub（默认只走 ISink.onEvent）

    std::size_t queueLimit = 20000;         ///< ING-NFR-05 进程内队列上限
    std::size_t ledgerLimit = 20000;        ///< ING-NFR-05 设备台账上限
    std::size_t commandLogLimit = 10000;    ///< ING-NFR-05 指令日志上限

    std::vector<PointConfig> points;    ///< 空 = 回落单接入点（config::fallbackPoint）

    /// 全局自洽性检查；返回可读原因列表（空 = 合法）。
    std::vector<std::string> validate() const;
};

namespace config {

/// 未配置 points 时，按既有环境变量语义回落为**单个**接入点：
///   group/port/enabled 取 MAPAPP_UDP_GROUP / MAPAPP_UDP_PORT / MAPAPP_UDP_ENABLED
///   默认 "239.10.10.10" / 45454 / true（契约 §1 既有组播必须继续可用）。
/// 解析器固定为内置 legacy_kind（既有 4 类 kind 分派），保证行为与改造前一致。
PointConfig fallbackPoint();

/// 把 JSON 对象翻译成 IngestConfig（契约 §3 的字段名）。
/// 顶层既接受 {"ingest": {...}} 也接受直接的 {...}。
/// 返回 false 时 error 给出可读原因；缺字段一律取默认值，不报错。
bool fromJson(const nlohmann::json& value, IngestConfig& out, std::string& error);
bool fromJsonString(const std::string& text, IngestConfig& out, std::string& error);

/// 读文件并调用 fromJsonString；文件不存在返回 false。
bool loadFile(const std::string& path, IngestConfig& out, std::string& error);

/// 用环境变量覆盖已填好的配置（MAPAPP_UDP_* / DEVICE_INGEST_*）。缺省变量不变更任何值。
void applyEnvOverrides(IngestConfig& cfg);

}  // namespace config
}  // namespace device_ingest
