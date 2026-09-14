// acc/point.h · 单接入点接收（设计方案 §11：udpLoop → acc::Point）
//
// 迁移映射：net/Telemetry.cc:83-145 udpLoop → 本文件
//   改动性质 = **搬 + 参数化**：每接入点一个实例，去掉全局 socket 成员。
//
// 硬约束：**一个端口只归一个接入点**（需求专篇 R6）。同端口冲突在启动时报可读原因。
// 故障隔离（ING-ACC-04）：bind 失败 / join 失败只影响本接入点，其它接入点继续工作。
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "device_ingest/config.h"
#include "device_ingest/types.h"

namespace device_ingest {
namespace acc {

/// 收到一个原始包时的交付回调。
/// 参数：原始字节、来源 "ip:port"、收到时刻。
/// 回调在**本接入点的接收线程**上执行，因此 MUST 快速返回、不得阻塞
/// （ING-FAN-01：接收线程不因下游阻塞）。解析→归一→入队正好满足这一点。
using PacketHandler = std::function<void(const char* data, std::size_t len,
                                         const std::string& peer, Millis recvAt)>;

/// 单接入点：绑定端口 → 加入组播 → 循环收包 → 交回调。
class Point {
public:
    explicit Point(PointConfig cfg);
    ~Point();

    Point(const Point&) = delete;
    Point& operator=(const Point&) = delete;

    /// 启动接收线程。返回 false 时 error 给可读原因（契约 §8 code=3002/3003）。
    /// 注意：即使 bind 成功但 join 组播失败，也返回 true（与现状行为一致：
    /// 单播仍可用），失败原因留在 metrics().lastError 里。
    bool start(PacketHandler handler, std::string* error);

    /// 停接收线程（幂等）。热删接入点时调用，不阻塞其它接入点。
    void stop();

    bool running() const { return running_.load(); }
    const PointConfig& config() const { return cfg_; }
    std::string id() const { return cfg_.id; }

    /// 指标快照（ING-ACC-06）。
    PointMetrics metrics() const;

    /// 内部计数口（解析/归一由 Gateway 完成后回填）。
    /// countPacket 供 Gateway::inject 使用：注入也是"到达"，
    /// 否则无网环境下接入点指标（ING-ACC-06）永远读不到数据。
    void countPacket(std::size_t len, const std::string& peer, Millis recvAt);
    void countEvent();
    void countParseFailed();
    void mergeNormalizeStats(const void* statsJson);
    void setLastError(const std::string& error);

    /// 发送一个 UDP 数据报（指令下发用，ING-CMD-02）。
    bool send(const std::string& ip, int port, const std::string& payload,
              std::string* error) const;

private:
    void loop();

    PointConfig cfg_;
    std::thread thread_;
    std::atomic<bool> running_{false};
    std::atomic<bool> quit_{false};

    // 以下字段在接收线程写、其它线程读 → 用一把锁保护
    mutable std::mutex mtx_;
    std::uint64_t packets_ = 0;
    std::uint64_t bytes_ = 0;
    std::uint64_t dropped_ = 0;
    std::uint64_t truncated_ = 0;
    std::uint64_t empty_ = 0;
    std::uint64_t parseFailed_ = 0;
    std::uint64_t events_ = 0;
    Millis        lastRecvAt_ = 0;
    std::string   lastPeer_;
    std::string   lastError_;
    PacketHandler handler_;

#if defined(_WIN32)
    std::atomic<std::uint64_t> sock_{~0ULL};
#else
    std::atomic<int> sock_{-1};
#endif
};

}  // namespace acc
}  // namespace device_ingest
