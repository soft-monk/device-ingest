// device-ingest · 版本与构建开关（唯一自描述处）
//
// 宿主与接入方通过这里判断"手上这份模块是什么版本、带不带 hub 层"，
// 而不是去猜 CMake 变量（设计方案 §13 待确认 3）。
#pragma once

// DEVICE_INGEST_VERSION 由 CMake 注入（target_compile_definitions）。
// 单独用编译器手工编译源码时给个兜底值，保证头文件可独立被包含。
#ifndef DEVICE_INGEST_VERSION
#define DEVICE_INGEST_VERSION "0.1.0-dev"
#endif

namespace device_ingest {

/// 模块语义版本，形如 "0.1.0"。
inline const char* version() { return DEVICE_INGEST_VERSION; }

/// 是否编译了 WS 广播层（hub）。
/// false = 纯采集模式：模块只做"接收 → 解析 → 归一 → 队列 → 回调"，
/// 不依赖任何 Web 框架（设计方案 §8.1）。
constexpr bool hub_enabled() {
#if defined(DEVICE_INGEST_HAS_HUB)
    return true;
#else
    return false;
#endif
}

}  // namespace device_ingest
