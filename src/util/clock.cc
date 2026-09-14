// util/clock.cc · 时间工具实现（参见 util/clock.h 的命名说明）
#include "util/clock.h"

#include <chrono>
#include <cstdio>
#include <ctime>

namespace device_ingest {

Millis nowMs() {
    // epoch 毫秒（契约 §2 时间口径）。全模块唯一取时入口。
    return static_cast<Millis>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

std::string formatLocal(Millis ms) {
    const std::time_t secs = static_cast<std::time_t>(ms / 1000);
    std::tm tm{};
#if defined(_WIN32)
    if (localtime_s(&tm, &secs) != 0) return std::string();
#else
    if (localtime_r(&secs, &tm) == nullptr) return std::string();
#endif
    char buf[32] = {0};
    std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d",
                  tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                  tm.tm_hour, tm.tm_min, tm.tm_sec);
    return std::string(buf);
}

}  // namespace device_ingest
