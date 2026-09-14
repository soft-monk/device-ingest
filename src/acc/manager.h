// acc/manager.h · 多接入点管理（ING-ACC-01/03/04/06）
//
// 需求要点：
//   ING-ACC-01 多接入点并行接收，MUST 支持 ≥ 8 个接入点，互不串台
//   ING-ACC-03 运行时热增删：不重启进程即可加入/移出一个接入点
//   ING-ACC-04 故障隔离：某接入点失败，其它接入点继续工作
//   ING-ACC-06 接入点级运行指标可查
#pragma once

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "acc/point.h"
#include "device_ingest/config.h"
#include "device_ingest/types.h"

namespace device_ingest {
namespace acc {

/// 接入点容器。每个接入点一个接收线程；增删互不影响。
class Manager {
public:
    Manager() = default;
    ~Manager();

    Manager(const Manager&) = delete;
    Manager& operator=(const Manager&) = delete;

    /// 新增并启动一个接入点。
    /// @param handlerFor 由调用方按接入点生成回调（Gateway 在这里接上解析→归一→入队）
    /// 返回 false 时 error 给可读原因；失败**不留下半启动状态**。
    bool add(const PointConfig& cfg,
             const std::function<PacketHandler(const PointConfig&)>& handlerFor,
             std::string* error = nullptr);

    /// 移除并停止接入点；返回是否存在。
    bool remove(const std::string& id);

    /// 已存在的接入点（不加启动）。
    Point* get(const std::string& id) const;
    bool has(const std::string& id) const;

    /// 配置快照（按 id 排序，输出稳定）。
    std::vector<PointConfig> configs() const;
    /// 指标快照（按 id 排序）。
    std::vector<PointMetrics> metrics() const;
    /// 运行中的接入点数。
    std::size_t runningCount() const;
    std::size_t size() const;

    /// 全部停止（幂等）。
    void stopAll();

private:
    mutable std::mutex mtx_;
    std::map<std::string, std::unique_ptr<Point>> points_;
};

}  // namespace acc
}  // namespace device_ingest
