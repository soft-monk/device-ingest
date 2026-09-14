// util/clock.h · 时间工具
//
// 设计方案 §11：现状 core/Repo::nowMs()（主仓一行工具函数）搬到这里，主仓不再提供。
//
// ⚠️ 文件名的由来（本仓踩过的坑）：它最初叫 util/time.h，与 CRT 的 <time.h> 同名——
//    一旦 src/util 进入 include 路径，<ctime> 内部的 `#include <time.h>` 就会命中它，
//    把 <chrono>/<filesystem> 整条链打断（MSVC 报 "chrono 不是类或命名空间名称"）。
//    因此改名 clock.h，且 src/util 不再加入 include 路径
//    （src 内部一律用全路径 `#include "util/clock.h"`）。
//
// nowMs() 的**声明**放在公开头 device_ingest/types.h（它是契约里的时间口径），
// 定义在 clock.cc —— 全模块唯一取时入口，便于将来注入假时钟做确定性测试。
#pragma once

#include <chrono>
#include <cstdint>
#include <string>

#include "device_ingest/types.h"

namespace device_ingest {

/// 单调时钟毫秒，用于线程节拍/超时（不受系统时间回拨影响）。
inline std::int64_t steadyMs() {
    return static_cast<std::int64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

/// 本地时间串 `YYYY-MM-DD HH:mm:ss`（契约 §1 人类可读时间口径）。
std::string formatLocal(Millis ms);

}  // namespace device_ingest
