// validate.cc · 字段级校验实现
//
// 契约 §2 降级约定（MUST）逐条落地：
//   坐标缺失/非数字/越界 → 丢弃 lng/lat 字段，事件仍上行，越界计数 +1
//   ts 缺失或非数字      → 用服务端 recvAt 填充 ts，并置 tsSource:"server"
//   seq 缺失             → 不填 seq，该设备丢包/乱序标"不可精确统计"
//   未知字段             → 原样忽略，不进归一对象（不报错）
#include "nrm/validate.h"

#include <cmath>
#include <cstdio>

namespace device_ingest {
namespace nrm {

Maybe<double> asDouble(const nlohmann::json& j, const char* key) {
    Maybe<double> out;
    auto it = j.find(key);
    if (it == j.end() || it->is_null()) return out;
    double v = 0.0;
    if (it->is_number()) {
        v = it->get<double>();
    } else if (it->is_string()) {
        // 报文里数字写成字符串是常见情况（"lng":"116.4"）；能解析就接受，否则视为缺失
        try {
            std::size_t used = 0;
            const std::string s = it->get<std::string>();
            if (s.empty()) return out;
            v = std::stod(s, &used);
            if (used != s.size()) return out;   // "116.4abc" 之类不接受
        } catch (...) {
            return out;
        }
    } else {
        return out;   // 布尔/数组/对象一律视为缺失
    }
    if (!std::isfinite(v)) return out;          // NaN / Inf 视为缺失
    out.has = true;
    out.value = v;
    return out;
}

Maybe<long long> asInt(const nlohmann::json& j, const char* key) {
    Maybe<long long> out;
    auto it = j.find(key);
    if (it == j.end() || it->is_null()) return out;
    if (it->is_number_integer() || it->is_number_unsigned()) {
        out.has = true;
        out.value = it->get<long long>();
        return out;
    }
    if (it->is_number_float()) {
        const double d = it->get<double>();
        if (!std::isfinite(d)) return out;
        if (std::floor(d) != d) return out;     // 非整数不接受（seq 必须是整数）
        if (d < -9.2e18 || d > 9.2e18) return out;
        out.has = true;
        out.value = static_cast<long long>(d);
        return out;
    }
    if (it->is_string()) {
        try {
            std::size_t used = 0;
            const std::string s = it->get<std::string>();
            if (s.empty()) return out;
            const long long v = std::stoll(s, &used);
            if (used != s.size()) return out;
            out.has = true;
            out.value = v;
        } catch (...) {
        }
    }
    return out;
}

Maybe<std::string> asString(const nlohmann::json& j, const char* key) {
    Maybe<std::string> out;
    auto it = j.find(key);
    if (it == j.end() || it->is_null()) return out;
    if (it->is_string()) {
        out.has = true;
        out.value = it->get<std::string>();
    }
    return out;
}

bool validLng(double v) { return std::isfinite(v) && v >= -180.0 && v <= 180.0; }
bool validLat(double v) { return std::isfinite(v) && v >= -90.0  && v <= 90.0;  }

bool normalizeHeading(double v, double& out) {
    if (!std::isfinite(v)) return false;
    double h = std::fmod(v, 360.0);
    if (h < 0.0) h += 360.0;
    out = h;
    return true;
}

bool validBattery(long long v) { return v >= 0 && v <= 100; }

std::string hexDigest(const char* data, std::size_t len, std::size_t maxBytes) {
    static const char* kHex = "0123456789abcdef";
    const std::size_t n = len < maxBytes ? len : maxBytes;
    std::string out;
    out.reserve(n * 2 + 3);
    for (std::size_t i = 0; i < n; ++i) {
        const unsigned char c = static_cast<unsigned char>(data[i]);
        out.push_back(kHex[c >> 4]);
        out.push_back(kHex[c & 0x0F]);
    }
    if (len > n) out += "...";
    return out;
}

}  // namespace nrm
}  // namespace device_ingest
