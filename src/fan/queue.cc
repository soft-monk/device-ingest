// queue.cc
#include "fan/queue.h"

#include <chrono>

namespace device_ingest {
namespace fan {

void Queue::setLimit(std::size_t limit) {
    std::lock_guard<std::mutex> lk(mtx_);
    limit_ = limit;
    while (limit_ > 0 && items_.size() > limit_) {
        items_.pop_front();
        ++dropped_;
    }
}

std::size_t Queue::limit() const {
    std::lock_guard<std::mutex> lk(mtx_);
    return limit_;
}

std::size_t Queue::depth() const {
    std::lock_guard<std::mutex> lk(mtx_);
    return items_.size();
}

std::uint64_t Queue::pushedTotal() const {
    std::lock_guard<std::mutex> lk(mtx_);
    return pushed_;
}

std::uint64_t Queue::droppedTotal() const {
    std::lock_guard<std::mutex> lk(mtx_);
    return dropped_;
}

bool Queue::push(IngestEvent ev) {
    bool evicted = false;
    {
        std::lock_guard<std::mutex> lk(mtx_);
        if (limit_ > 0 && items_.size() >= limit_) {
            items_.pop_front();
            ++dropped_;
            evicted = true;
        }
        items_.push_back(std::move(ev));
        ++pushed_;
    }
    cv_.notify_one();
    return !evicted;
}

bool Queue::pop(IngestEvent& out, int timeoutMs) {
    std::unique_lock<std::mutex> lk(mtx_);
    if (items_.empty() && !closed_) {
        cv_.wait_for(lk, std::chrono::milliseconds(timeoutMs),
                     [this] { return !items_.empty() || closed_; });
    }
    if (items_.empty()) return false;
    out = std::move(items_.front());
    items_.pop_front();
    return true;
}

void Queue::close() {
    {
        std::lock_guard<std::mutex> lk(mtx_);
        closed_ = true;
    }
    cv_.notify_all();
}

bool Queue::closed() const {
    std::lock_guard<std::mutex> lk(mtx_);
    return closed_;
}

void Queue::reopen() {
    std::lock_guard<std::mutex> lk(mtx_);
    closed_ = false;
}

void Queue::clear() {
    std::lock_guard<std::mutex> lk(mtx_);
    items_.clear();
    pushed_ = 0;
    dropped_ = 0;
}

}  // namespace fan
}  // namespace device_ingest
