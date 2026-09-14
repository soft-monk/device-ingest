// util/bounded.h · 有界容器
//
// ING-NFR-05（MUST）：队列长度、设备台账条数、指令日志条数、各统计窗口
// 均为**有界容器**，超限按**最旧淘汰**；提供占用查询。
// 口径对齐 map-2d core/resources.ts 的既有做法。
#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace device_ingest {

/// 有界 FIFO：容量满时淘汰最旧一条，并累计淘汰计数（不静默丢数据）。
template <typename T>
class BoundedRing {
public:
    explicit BoundedRing(std::size_t limit = 0) : limit_(limit) {}

    void setLimit(std::size_t limit) {
        std::lock_guard<std::mutex> lk(mtx_);
        limit_ = limit;
        trimLocked();
    }
    std::size_t limit() const {
        std::lock_guard<std::mutex> lk(mtx_);
        return limit_;
    }
    std::size_t size() const {
        std::lock_guard<std::mutex> lk(mtx_);
        return items_.size();
    }
    /// 因超限被淘汰的累计条数（ING-NFR-05 的"超限计数告警"口径）。
    std::uint64_t evictedTotal() const {
        std::lock_guard<std::mutex> lk(mtx_);
        return evicted_;
    }

    /// 入队。返回 true = 未触发淘汰；false = 本此入队淘汰了最旧一条。
    bool push(T item) {
        std::lock_guard<std::mutex> lk(mtx_);
        bool evictedNow = false;
        if (limit_ > 0 && items_.size() >= limit_) {
            items_.pop_front();
            ++evicted_;
            evictedNow = true;
        }
        items_.push_back(std::move(item));
        return !evictedNow;
    }

    /// 按谓词取第一条匹配项（从新到旧）。
    template <typename Pred>
    bool findIf(Pred pred, T& out) const {
        std::lock_guard<std::mutex> lk(mtx_);
        for (auto it = items_.rbegin(); it != items_.rend(); ++it) {
            if (pred(*it)) { out = *it; return true; }
        }
        return false;
    }

    /// 快照（旧→新）。limit 为 0 表示全部。
    std::vector<T> snapshot(std::size_t limit = 0) const {
        std::lock_guard<std::mutex> lk(mtx_);
        std::vector<T> out;
        const std::size_t n = items_.size();
        std::size_t begin = 0;
        if (limit > 0 && n > limit) begin = n - limit;
        out.reserve(n - begin);
        for (std::size_t i = begin; i < n; ++i) out.push_back(items_[i]);
        return out;
    }

    /// 快照 + 变换，避免调用方在锁外再拷一遍。
    template <typename Fn>
    auto map(Fn fn) const -> std::vector<decltype(fn(std::declval<const T&>()))> {
        std::lock_guard<std::mutex> lk(mtx_);
        std::vector<decltype(fn(std::declval<const T&>()))> out;
        out.reserve(items_.size());
        for (const auto& it : items_) out.push_back(fn(it));
        return out;
    }

    void clear() {
        std::lock_guard<std::mutex> lk(mtx_);
        items_.clear();
    }

private:
    void trimLocked() {
        while (limit_ > 0 && items_.size() > limit_) {
            items_.pop_front();
            ++evicted_;
        }
    }

    mutable std::mutex mtx_;
    std::deque<T>       items_;
    std::size_t         limit_ = 0;   ///< 0 = 不限制
    std::uint64_t       evicted_ = 0;
};

/// 有界映射表：条目超限时淘汰**最久未更新**的一条（台账用）。
/// 台账不能简单按插入顺序淘汰——老设备可能一直在线，淘汰它等于丢设备。
template <typename V>
class BoundedMap {
public:
    explicit BoundedMap(std::size_t limit = 0) : limit_(limit) {}

    void setLimit(std::size_t limit) {
        std::lock_guard<std::mutex> lk(mtx_);
        limit_ = limit;
        // 上限调小时按迭代顺序淘汰到不超限
        // （upsert 路径会按"最久未更新"淘汰，这里只做收缩）
        while (limit_ > 0 && map_.size() > limit_) {
            map_.erase(map_.begin());
            ++evicted_;
        }
    }
    std::size_t limit() const {
        std::lock_guard<std::mutex> lk(mtx_);
        return limit_;
    }
    std::size_t size() const {
        std::lock_guard<std::mutex> lk(mtx_);
        return map_.size();
    }
    std::uint64_t evictedTotal() const {
        std::lock_guard<std::mutex> lk(mtx_);
        return evicted_;
    }

    /// 就地更新或插入；`stampOf` 给出"最近活跃时刻"，用于超限淘汰判定。
    template <typename Fn, typename StampFn>
    void upsert(const std::string& key, Fn fn, StampFn stampOf) {
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = map_.find(key);
        if (it == map_.end()) {
            V v{};
            fn(v);
            map_.emplace(key, std::move(v));
            if (limit_ > 0 && map_.size() > limit_) evictOldestLocked(stampOf);
        } else {
            fn(it->second);
        }
    }

    bool get(const std::string& key, V& out) const {
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = map_.find(key);
        if (it == map_.end()) return false;
        out = it->second;
        return true;
    }

    template <typename Fn>
    void forEach(Fn fn) const {
        std::lock_guard<std::mutex> lk(mtx_);
        for (const auto& kv : map_) fn(kv.second);
    }

    template <typename Fn>
    void mutate(const std::string& key, Fn fn) {
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = map_.find(key);
        if (it != map_.end()) fn(it->second);
    }

    /// 就地遍历并允许回调修改（离线巡检用）。
    template <typename Fn>
    void mutateAll(Fn fn) {
        std::lock_guard<std::mutex> lk(mtx_);
        for (auto& kv : map_) fn(kv.second);
    }

    void clear() {
        std::lock_guard<std::mutex> lk(mtx_);
        map_.clear();
    }

private:
    template <typename StampFn>
    void evictOldestLocked(StampFn stampOf) {
        auto victim = map_.begin();
        bool first = true;
        for (auto it = map_.begin(); it != map_.end(); ++it) {
            if (first) { victim = it; first = false; continue; }
            if (stampOf(it->second) < stampOf(victim->second)) victim = it;
        }
        if (victim != map_.end()) {
            map_.erase(victim);
            ++evicted_;
        }
    }

    mutable std::mutex mtx_;
    std::unordered_map<std::string, V> map_;
    std::size_t   limit_ = 0;
    std::uint64_t evicted_ = 0;
};

}  // namespace device_ingest
