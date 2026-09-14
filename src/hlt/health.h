// hlt/health.h · 设备台账 + 心跳超时 + 丢包/乱序精确统计（ING-HLT-01 ~ 06）
//
// 序号精度前提（评审已确认）：设备报文带 seq 与 ts，因此丢包率与乱序为
// **精确统计**；无 seq 的设备 MUST 标注"不可精确统计"，**不给估计值**（决策 D6）。
#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "device_ingest/config.h"
#include "device_ingest/types.h"
#include "util/bounded.h"

namespace device_ingest {
namespace hlt {

/// 单设备台账条目（ING-HLT-05）。
struct DeviceRecord {
    std::string deviceId;
    std::string deviceType;
    Millis      firstSeen = 0;
    Millis      lastSeen = 0;
    bool        online = false;
    bool        everOffline = false;
    Millis      offlineSince = 0;      ///< 最近一次判离线的时刻
    double      lastOfflineSec = 0.0;  ///< 最近一段离线时长（恢复上线时上报）
    std::string ingestId;
    std::string peer;
    bool        hasLastPos = false;
    double      lastLng = 0.0;
    double      lastLat = 0.0;
    bool        hasLastAlt = false;
    double      lastAlt = 0.0;
    long long   packets = 0;
    double      timeoutSec = 3.0;      ///< 本设备适用的失联阈值（周期 × 倍数）

    // ---------------- 序号与统计窗口
    bool        hasSeq = false;
    long long   lastSeq = -1;
    long long   minSeq = 0;            ///< 当前窗口内最小 seq
    long long   maxSeq = 0;            ///< 当前窗口内最大 seq
    long long   received = 0;          ///< 当前窗口内实收包数（带 seq 的）
    long long   outOfOrder = 0;        ///< 当前窗口内乱序次数
    bool        seqWrapped = false;    ///< 窗口内发生过 seq 回绕/重置（按新会话处理）
    std::int64_t windowIndex = -1;     ///< 当前统计窗口编号，变更即滚窗

    DeviceInfo toInfo(Millis now) const;
    DeviceStats toStats(const IngestConfig& cfg, Millis now) const;
};

/// 台账 + 健康判定 + 统计。
///
/// 线程安全：push() 由接收线程调用；check() 由巡检线程调用；
/// 查询由任意线程调用；内部一把锁，临界区都很短。
class HealthTracker {
public:
    /// push() 的结果：调用方据此决定要不要发 device.online（ING-HLT-02/06）。
    ///
    /// 为什么要有这个返回值：`push` 只改状态、不发事件（它不认识 sink），
    /// 而"新设备上线"与"离线设备恢复"都必须上行一条 device.online。
    /// 早期版本只返回 bool，导致**上线事件从来没被生成过**——由自测抓出。
    struct PushResult {
        bool        isNewDevice = false;   ///< 台账里第一次出现
        bool        cameOnline = false;    ///< 上线跃迁（新设备或离线恢复）
        double      offlineSec = 0.0;      ///< 恢复时携带的离线时长
        std::string deviceType;
        Millis      lastSeen = 0;
    };

    explicit HealthTracker(const IngestConfig& cfg);

    void setConfig(const IngestConfig& cfg);
    void setLedgerLimit(std::size_t limit);

    /// 消费一条归一事件。
    PushResult push(const NormalizedEvent& ev);

    /// 周期巡检：判离线。把本次新判离线的设备追加到离线集合。
    void check(Millis now, std::vector<DeviceRecord>& wentOffline);

    /// 自上个统计窗口以来需要推 device.stats 的设备（低频，默认 5 s 一次）。
    /// 由调用方决定推送节奏；这里只负责"窗口到期"这一事实。
    std::vector<DeviceStats> dueStats(Millis now);

    bool find(const std::string& deviceId, DeviceRecord& out) const;
    std::vector<DeviceInfo> list(Millis now) const;
    bool stats(const std::string& deviceId, DeviceStats& out) const;
    std::size_t size() const;
    std::size_t onlineCount(Millis now) const;
    std::uint64_t evictedTotal() const;
    void clear();

private:
    mutable std::mutex mtx_;
    IngestConfig cfg_;
    std::unordered_map<std::string, DeviceRecord> devices_;
    std::size_t ledgerLimit_ = 20000;
    std::uint64_t evicted_ = 0;
    Millis lastStatsPush_ = 0;
    std::unordered_map<std::string, Millis> lastStatsAt_;
};

}  // namespace hlt
}  // namespace device_ingest
