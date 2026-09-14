// fan/merge.h · 单帧合并（ING-FAN-02）
#pragma once

#include <vector>

#include "device_ingest/types.h"

namespace device_ingest {
namespace fan {

/// 单帧合并：同一时间窗内**同设备**的多次上报合并为**最后一次**再分发。
///
/// 合并键是 `deviceId`（不是 kind）：契约 §2 保证 deviceId 在接入层内稳定唯一，
/// 一台设备一个窗内只该占一个位置。
///
/// 顺序：按**首次出现**的顺序保留，位置取最后一次的值——
/// 这样"谁先动"的时序语义不变，只是把同一设备的多帧压成一帧。
///
/// @param batch 一个时间窗内入队的事件（可能含同设备多条）
/// @return 合并后的事件；无重复时原样返回（不复制）
std::vector<IngestEvent> mergeBatch(const std::vector<IngestEvent>& batch);

/// 合并后的条数（不构造结果，供日志/指标用）。
std::size_t mergedCount(const std::vector<IngestEvent>& batch);

}  // namespace fan
}  // namespace device_ingest
