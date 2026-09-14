// device-ingest · 反向接口（宿主实现侧）
//
// 设计方案 §3.2：模块把结果交给宿主，宿主实现这两个（可留空的）接口。
//   - ISink    ：数据出口（归一事件 / 批次 / 设备健康 / 指令状态 / 告警）
//   - ILogSink ：日志出口（**替代现状直接写 event_log 表那条隐线**）
//
// 为什么要有 ILogSink：现状 EventHub::logEvent() 内部直接写 SQLite
// （EventHub.cc:68-70），把广播与落库缝在一起。抽仓后广播只广播，
// 落库由宿主经本接口实现——模块内**不出现任何 SQL**（设计方案 G4）。
#pragma once

#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "device_ingest/types.h"

namespace device_ingest {

/// 数据出口。所有方法都可能被**消费线程**调用，实现方需自行保证线程安全；
/// 实现应尽快返回（不要在里面做长阻塞），否则会拖慢单帧合并节奏。
class ISink {
public:
    virtual ~ISink() = default;

    /// 每个归一化事件。默认窗（mergeWindowMs>0）下该事件**已按窗合并**，
    /// 即同一设备在窗内的多次上报只留最后一次。
    virtual void onEvent(const IngestEvent& ev) = 0;

    /// 单帧合并后的整批事件。默认实现逐条转调 onEvent。
    ///
    /// ⚠️ 语义约定：`onBatch` 与 `onEvent` **不会同时**被调用——
    /// 配了合并窗（mergeWindowMs > 0，默认）时只调 onBatch；配 0 时只调 onEvent。
    /// 因此**两个都重写**才能收到全部数据；只想处理单条时，
    /// 重写 onEvent 即可（默认 onBatch 会转调过去），或反过来只重写 onBatch。
    ///
    /// 这样设计的原因：早期版本"先 onBatch 再逐条 onEvent"，
    /// 宿主会收到两份数据（自测把它抓了出来）。
    virtual void onBatch(const std::vector<IngestEvent>& evs) {
        for (const auto& e : evs) onEvent(e);
    }

    /// 设备上线 / 离线 / 周期统计（对应 WS device.online / device.offline / device.stats）。
    virtual void onDeviceHealth(const DeviceHealthEvent& ev) { (void)ev; }

    /// 指令状态变化（对应 WS command.state；含重传与超时）。
    virtual void onCommandState(const CommandStateEvent& ev) { (void)ev; }

    /// 告警：接入点 bind/join 失败、解析器故障、无主回执、队列超限等。
    virtual void onAlert(const AlertEvent& ev) { (void)ev; }
};

/// 空实现：宿主只关心其中一两个回调时用它做基类，省掉一堆空方法。
class NullSink : public ISink {
public:
    void onEvent(const IngestEvent& ev) override { (void)ev; }
    void onBatch(const std::vector<IngestEvent>& evs) override { (void)evs; }
};

/// 日志出口（可留空）。模块自身不落库、不写文件。
class ILogSink {
public:
    virtual ~ILogSink() = default;

    /// 结构化日志：category 形如 "acc" / "prs" / "nrm" / "hlt" / "cmd" / "fan" / "hub"。
    virtual void log(const std::string& category, const nlohmann::json& payload) = 0;

    /// 指令操作日志（ING-CMD-06）：谁 / 何时 / 向谁 / 发了什么 / 结果如何。
    virtual void commandAudit(const CommandRecord& rec) = 0;
};

/// 空实现：不落任何日志。纯采集或测试场景直接用。
class NullLogSink : public ILogSink {
public:
    void log(const std::string& category, const nlohmann::json& payload) override {
        (void)category; (void)payload;
    }
    void commandAudit(const CommandRecord& rec) override { (void)rec; }
};

}  // namespace device_ingest
