// nrm/normalize.h · 归一化（ING-NRM-01 ~ 04）
#pragma once

#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "device_ingest/config.h"
#include "device_ingest/types.h"

namespace device_ingest {
namespace nrm {

/// 归一化过程中的丢弃/兜底计数（ING-NRM-02 的"计数 +1"口径）。
/// 由调用方（acc 接入点）持有并累加到 PointMetrics。
struct NormalizeStats {
    std::uint64_t coordDroppedLng = 0;   ///< 经度越界或非数字
    std::uint64_t coordDroppedLat = 0;   ///< 纬度越界或非数字
    std::uint64_t headingFixed    = 0;   ///< 航向越界被归一化到 [0,360)
    std::uint64_t headingDropped  = 0;   ///< 航向非数字被丢弃
    std::uint64_t batteryDropped  = 0;   ///< 电量越界被丢弃
    std::uint64_t tsFromServer    = 0;   ///< ts 缺失用到达时刻补齐
    std::uint64_t deviceIdFallback = 0;  ///< deviceId 缺失用 peer 兜底
    std::uint64_t seqMissing      = 0;   ///< 无 seq → 丢包/乱序不可精确统计

    void merge(const NormalizeStats& other);
    nlohmann::json toJson() const;
};

/// 归一化：ParsedEvent + 接入点上下文 → NormalizedEvent。
///
/// @param pe        解析器输出（还没归一化的业务字段）
/// @param point     所属接入点配置（提供 deviceType / ingestId / topic）
/// @param peer      来源 "ip:port"
/// @param bytes     原始包长度
/// @param recvAt    服务端到达时刻（epoch 毫秒）
/// @param stats     计数出口，可为空
NormalizedEvent normalize(const ParsedEvent& pe,
                          const PointConfig& point,
                          const std::string& peer,
                          std::size_t bytes,
                          Millis recvAt,
                          NormalizeStats* stats);

/// 由 `kind` 与接入点 topic 推导 WS 事件名（契约 §5 末段）。
///
/// 规则（顺序即优先级）：
///   1. 既有 4 类 kind → 固定事件名（兼容承诺 ING-PRS-05，**不可改名**）；
///   2. 接入点显式配了 topic → 用 topic（新设备类型加端口即生效，不用改前端）；
///   3. 其余 → "telemetry." + kind；kind 为空 → "telemetry.event"。
std::string resolveEventName(const std::string& kind, const std::string& topic);

/// 把归一对象转成"发给前端的事件 data"。
///
/// @param legacyCompat true = 该 kind 属既有 4 类：**保留解析器给出的全部原字段**
///                      （字段只增不改），再补齐归一字段。
///                     false = 新设备类型：输出标准归一字段。
nlohmann::json toEventData(const NormalizedEvent& ev, const ParsedEvent& pe,
                           bool legacyCompat);

}  // namespace nrm
}  // namespace device_ingest
