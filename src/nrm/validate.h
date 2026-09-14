// nrm/validate.h · 字段级校验（ING-NRM-02 / ING-NRM-03）
#pragma once

#include <cstddef>
#include <string>

#include <nlohmann/json.hpp>

#include "device_ingest/types.h"

namespace device_ingest {
namespace nrm {

/// 取值结果：多一个"有没有取到"的布尔量，避免用 0 冒充"缺失"。
template <typename T>
struct Maybe {
    bool has = false;
    T    value{};
};

/// JSON → double。非数字（字符串/布尔/对象/NaN/Inf）一律视为缺失（不报错）。
Maybe<double> asDouble(const nlohmann::json& j, const char* key);
/// JSON → long long。非整数类型但可精确表示时接受（如 10241.0）。
Maybe<long long> asInt(const nlohmann::json& j, const char* key);
/// JSON → 字符串（仅接受 JSON 字符串）。
Maybe<std::string> asString(const nlohmann::json& j, const char* key);

/// WGS84 经度合法性：|lng| ≤ 180。
bool validLng(double v);
/// WGS84 纬度合法性：|lat| ≤ 90。
bool validLat(double v);
/// 航向归一化到 [0,360)：负值 +360、超界取模；非有限值返回 false。
bool normalizeHeading(double v, double& out);
/// 电量合法性：0..100（按契约取整）。
bool validBattery(long long v);

/// 十六进制摘要（契约 §2 raw）：最多 maxBytes 字节，超出加 "..." 标注。
std::string hexDigest(const char* data, std::size_t len, std::size_t maxBytes = 64);

}  // namespace nrm
}  // namespace device_ingest
