// manager.cc
#include "acc/manager.h"

#include <algorithm>

namespace device_ingest {
namespace acc {

Manager::~Manager() { stopAll(); }

bool Manager::add(const PointConfig& cfg,
                  const std::function<PacketHandler(const PointConfig&)>& handlerFor,
                  std::string* error) {
    if (cfg.id.empty()) {
        if (error) *error = "接入点缺 id";
        return false;
    }
    const std::string verr = cfg.validate();
    if (!verr.empty()) {
        if (error) *error = verr;
        return false;
    }

    std::lock_guard<std::mutex> lk(mtx_);
    if (points_.find(cfg.id) != points_.end()) {
        if (error) *error = "接入点 id 已存在: " + cfg.id;
        return false;
    }
    // 硬约束：一个端口只归一个接入点（需求专篇 R6）
    for (const auto& kv : points_) {
        if (kv.second && kv.second->config().port == cfg.port) {
            if (error) {
                *error = "端口 " + std::to_string(cfg.port) + " 已被接入点 " +
                         kv.first + " 占用（一个端口只归一个接入点）";
            }
            return false;
        }
    }
    if (!cfg.enabled) {
        // 配置里声明了但 enabled=false：登记配置但不启动线程
        auto p = std::unique_ptr<Point>(new Point(cfg));
        p->setLastError("配置 disabled，未启动");
        points_.emplace(cfg.id, std::move(p));
        return true;
    }

    auto point = std::unique_ptr<Point>(new Point(cfg));
    std::string startError;
    PacketHandler handler = handlerFor ? handlerFor(cfg) : PacketHandler();
    if (!point->start(std::move(handler), &startError)) {
        if (error) *error = startError;
        return false;   // 不留下半启动状态
    }
    points_.emplace(cfg.id, std::move(point));
    return true;
}

bool Manager::remove(const std::string& id) {
    std::unique_ptr<Point> victim;
    {
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = points_.find(id);
        if (it == points_.end()) return false;
        victim = std::move(it->second);
        points_.erase(it);
    }
    // 在锁外停线程：stop() 会 join，持锁 join 会阻塞其它接入点的查询
    if (victim) victim->stop();
    return true;
}

Point* Manager::get(const std::string& id) const {
    std::lock_guard<std::mutex> lk(mtx_);
    auto it = points_.find(id);
    return it == points_.end() ? nullptr : it->second.get();
}

bool Manager::has(const std::string& id) const {
    std::lock_guard<std::mutex> lk(mtx_);
    return points_.find(id) != points_.end();
}

std::vector<PointConfig> Manager::configs() const {
    std::lock_guard<std::mutex> lk(mtx_);
    std::vector<PointConfig> out;
    out.reserve(points_.size());
    for (const auto& kv : points_) {
        if (kv.second) out.push_back(kv.second->config());
    }
    return out;
}

std::vector<PointMetrics> Manager::metrics() const {
    // 先在锁内取指针，再在锁外取指标：避免 metrics() 内部加锁与容器锁嵌套
    std::vector<Point*> snapshot;
    {
        std::lock_guard<std::mutex> lk(mtx_);
        snapshot.reserve(points_.size());
        for (const auto& kv : points_) {
            if (kv.second) snapshot.push_back(kv.second.get());
        }
    }
    std::vector<PointMetrics> out;
    out.reserve(snapshot.size());
    for (Point* p : snapshot) out.push_back(p->metrics());
    return out;
}

std::size_t Manager::runningCount() const {
    std::lock_guard<std::mutex> lk(mtx_);
    std::size_t n = 0;
    for (const auto& kv : points_) {
        if (kv.second && kv.second->running()) ++n;
    }
    return n;
}

std::size_t Manager::size() const {
    std::lock_guard<std::mutex> lk(mtx_);
    return points_.size();
}

void Manager::stopAll() {
    std::vector<std::unique_ptr<Point>> victims;
    {
        std::lock_guard<std::mutex> lk(mtx_);
        for (auto& kv : points_) victims.push_back(std::move(kv.second));
        points_.clear();
    }
    for (auto& p : victims) {
        if (p) p->stop();
    }
}

}  // namespace acc
}  // namespace device_ingest
