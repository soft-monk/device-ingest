// fan/queue.h · 进程内队列（生产者 → 队列 → 消费者）
//
// ING-FAN-01（MUST）：归一化事件进**进程内队列**，由独立消费侧合并后广播；
// 接收线程 MUST NOT 因广播阻塞。
// 决策 D2：不引 Redis / NATS / Kafka，但接口按"生产者→队列→消费者"形态设计，
// 留跨进程升级口（本文件是唯一需要替换的地方）。
#pragma once

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <vector>

#include "device_ingest/types.h"

namespace device_ingest {
namespace fan {

/// 有界阻塞队列。容量超限时**淘汰最旧一条并计数**（ING-NFR-05），
/// 绝不阻塞生产者——这条是"接收线程不因广播阻塞"的底线。
class Queue {
public:
    explicit Queue(std::size_t limit = 20000) : limit_(limit) {}

    void setLimit(std::size_t limit);
    std::size_t limit() const;
    std::size_t depth() const;
    std::uint64_t pushedTotal() const;
    std::uint64_t droppedTotal() const;   ///< 因超限被淘汰的条数（超限计数告警口径）

    /// 入队（生产者调用，不阻塞）。返回 false = 本次入队触发了最旧淘汰。
    bool push(IngestEvent ev);

    /// 取一条（最多等 timeoutMs；closed 后即使队列空也立即返回 false）。
    bool pop(IngestEvent& out, int timeoutMs = 50);

    /// 关闭：唤醒所有等待者，pop 在队列空时立即返回 false。
    void close();
    bool closed() const;
    /// 重新打开（reload 用）。
    void reopen();

    /// 清空并复位计数（测试复位用）。
    void clear();

private:
    mutable std::mutex      mtx_;
    std::condition_variable cv_;
    std::deque<IngestEvent> items_;
    std::size_t             limit_ = 20000;
    bool                    closed_ = false;
    std::uint64_t           pushed_ = 0;
    std::uint64_t           dropped_ = 0;
};

}  // namespace fan
}  // namespace device_ingest
