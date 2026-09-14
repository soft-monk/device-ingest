// config.cc · 配置解析与环境变量回落
//
// 契约依据：《外设接入契约》§3 接入点配置 + §9 降级矩阵。
// 关键约束（ING-ACC-02 MUST）：接入点、设备类型、解析器绑定、超时倍数
// **全部来自配置**，模块内不硬编码；未配置时回落既有单接入点行为。
#include "device_ingest/config.h"

#include <cctype>
#include <cstdlib>
#include <fstream>
#include <sstream>

namespace device_ingest {

// ---------------------------------------------------------------- 校验
std::string PointConfig::validate() const {
    if (id.empty())        return "接入点缺 id";
    if (port <= 0)         return "接入点 " + id + " 缺 port";
    if (port > 65535)      return "接入点 " + id + " 的 port 超出 1..65535";
    if (parserId.empty())  return "接入点 " + id + " 缺 parserId";
    if (!group.empty() && group != "0.0.0.0" && group.find(':') != std::string::npos) {
        return "接入点 " + id + " 的 group 不是 IPv4 地址: " + group;
    }
    return std::string();
}

std::vector<std::string> IngestConfig::validate() const {
    std::vector<std::string> errs;
    std::vector<std::string> seenIds;
    std::vector<int>         seenPorts;
    for (const auto& p : points) {
        const std::string e = p.validate();
        if (!e.empty()) errs.push_back(e);
        for (const auto& id : seenIds) {
            if (id == p.id) { errs.push_back("接入点 id 重复: " + p.id); break; }
        }
        seenIds.push_back(p.id);
        // 硬约束：一个端口只归一个接入点（需求专篇 R6）
        for (int port : seenPorts) {
            if (port == p.port) {
                errs.push_back("端口 " + std::to_string(p.port) +
                               " 被多个接入点占用（一个端口只归一个接入点）");
                break;
            }
        }
        seenPorts.push_back(p.port);
    }
    if (mergeWindowMs < 0)            errs.push_back("mergeWindowMs 不能为负");
    if (healthWindowSec <= 0)         errs.push_back("healthWindowSec 必须为正");
    if (heartbeatPeriodMs <= 0)       errs.push_back("heartbeatPeriodMs 必须为正");
    if (defaultTimeoutMultiple <= 0)  errs.push_back("defaultTimeoutMultiple 必须为正");
    if (cmdTimeoutMs <= 0)            errs.push_back("cmdTimeoutMs 必须为正");
    if (cmdMaxRetries < 0)            errs.push_back("cmdMaxRetries 不能为负");
    return errs;
}

namespace config {

namespace {

/// 逐字段取值；缺失一律取默认，不报错（契约 §2 降级约定精神）。
template <typename T>
void getIf(const nlohmann::json& j, const char* key, T& out) {
    auto it = j.find(key);
    if (it == j.end() || it->is_null()) return;
    try {
        out = it->get<T>();
    } catch (...) {
        // 类型不符 → 保留默认值（配置容错优先于报错）
    }
}

PointConfig pointFromJson(const nlohmann::json& j) {
    PointConfig p;
    getIf(j, "id", p.id);
    getIf(j, "group", p.group);
    getIf(j, "port", p.port);
    getIf(j, "iface", p.iface);
    getIf(j, "parserId", p.parserId);
    getIf(j, "deviceType", p.deviceType);
    getIf(j, "topic", p.topic);
    getIf(j, "enabled", p.enabled);
    getIf(j, "timeoutMultiple", p.timeoutMultiple);
    return p;
}

const char* envOrNull(const char* name) {
    const char* v = std::getenv(name);
    if (v == nullptr || *v == '\0') return nullptr;
    return v;
}

bool envInt(const char* name, int& out) {
    const char* v = envOrNull(name);
    if (v == nullptr) return false;
    try {
        out = std::stoi(v);
        return true;
    } catch (...) {
        return false;
    }
}

bool envBool(const char* name, bool& out) {
    const char* v = envOrNull(name);
    if (v == nullptr) return false;
    std::string s(v);
    for (auto& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    if (s == "1" || s == "true" || s == "yes" || s == "on")  { out = true;  return true; }
    if (s == "0" || s == "false" || s == "no" || s == "off") { out = false; return true; }
    return false;
}

bool envStr(const char* name, std::string& out) {
    const char* v = envOrNull(name);
    if (v == nullptr) return false;
    out = v;
    return true;
}

}  // namespace

PointConfig fallbackPoint() {
    // 既有环境变量语义：MAPAPP_UDP_GROUP / MAPAPP_UDP_PORT / MAPAPP_UDP_ENABLED
    // 默认值与主仓 core/Config.h 完全一致（239.10.10.10 / 45454 / true）。
    PointConfig p;
    p.id         = "ingest-legacy";
    p.group      = "239.10.10.10";
    p.port       = 45454;
    p.parserId   = "legacy.kind.v1";
    p.deviceType = "legacy";
    p.topic      = "";   // 空 = 由解析器给出的 kind 决定事件名（既有 4 类 kind）
    p.enabled    = true;
    envStr("MAPAPP_UDP_GROUP", p.group);
    envInt("MAPAPP_UDP_PORT", p.port);
    envBool("MAPAPP_UDP_ENABLED", p.enabled);
    return p;
}

bool fromJson(const nlohmann::json& value, IngestConfig& out, std::string& error) {
    error.clear();
    if (!value.is_object()) {
        error = "配置根节点必须是 JSON 对象";
        return false;
    }
    const nlohmann::json* root = &value;
    auto it = value.find("ingest");
    if (it != value.end() && it->is_object()) root = &(*it);

    getIf(*root, "enabled", out.enabled);
    getIf(*root, "mergeWindowMs", out.mergeWindowMs);
    getIf(*root, "defaultTimeoutMultiple", out.defaultTimeoutMultiple);
    getIf(*root, "healthWindowSec", out.healthWindowSec);
    getIf(*root, "heartbeatPeriodMs", out.heartbeatPeriodMs);
    getIf(*root, "offlineCheckIntervalMs", out.offlineCheckIntervalMs);
    getIf(*root, "cmdTimeoutMs", out.cmdTimeoutMs);
    getIf(*root, "cmdMaxRetries", out.cmdMaxRetries);
    getIf(*root, "statsPushIntervalMs", out.statsPushIntervalMs);
    getIf(*root, "emitRawEvents", out.emitRawEvents);

    // 有界上限（ING-NFR-05）：JSON 用无符号语义，负数视为非法并保留默认。
    std::size_t sz = 0;
    sz = out.queueLimit;       getIf(*root, "queueLimit", sz);       out.queueLimit = sz;
    sz = out.ledgerLimit;      getIf(*root, "ledgerLimit", sz);      out.ledgerLimit = sz;
    sz = out.commandLogLimit;  getIf(*root, "commandLogLimit", sz);  out.commandLogLimit = sz;

    auto pit = root->find("points");
    if (pit != root->end() && pit->is_array()) {
        out.points.clear();
        for (const auto& pj : *pit) {
            if (!pj.is_object()) continue;
            out.points.push_back(pointFromJson(pj));
        }
    }

    const auto errs = out.validate();
    if (!errs.empty()) {
        std::ostringstream oss;
        for (std::size_t i = 0; i < errs.size(); ++i) {
            if (i) oss << "; ";
            oss << errs[i];
        }
        error = oss.str();
        return false;
    }
    return true;
}

bool fromJsonString(const std::string& text, IngestConfig& out, std::string& error) {
    error.clear();
    nlohmann::json j;
    try {
        j = nlohmann::json::parse(text);
    } catch (const std::exception& ex) {
        error = std::string("配置不是合法 JSON: ") + ex.what();
        return false;
    }
    return fromJson(j, out, error);
}

bool loadFile(const std::string& path, IngestConfig& out, std::string& error) {
    error.clear();
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        error = "配置文件打不开: " + path;
        return false;
    }
    std::ostringstream oss;
    oss << in.rdbuf();
    if (!fromJsonString(oss.str(), out, error)) {
        error = path + ": " + error;
        return false;
    }
    return true;
}

void applyEnvOverrides(IngestConfig& cfg) {
    // 全局段：DEVICE_INGEST_* 覆盖（不覆盖 points，points 只走配置文件/热增）
    envInt("DEVICE_INGEST_MERGE_WINDOW_MS", cfg.mergeWindowMs);
    envInt("DEVICE_INGEST_TIMEOUT_MULTIPLE", cfg.defaultTimeoutMultiple);
    envInt("DEVICE_INGEST_HEALTH_WINDOW_SEC", cfg.healthWindowSec);
    envInt("DEVICE_INGEST_HEARTBEAT_PERIOD_MS", cfg.heartbeatPeriodMs);
    envInt("DEVICE_INGEST_CMD_TIMEOUT_MS", cfg.cmdTimeoutMs);
    envInt("DEVICE_INGEST_CMD_MAX_RETRIES", cfg.cmdMaxRetries);
    envBool("DEVICE_INGEST_ENABLED", cfg.enabled);
    int q = static_cast<int>(cfg.queueLimit);
    if (envInt("DEVICE_INGEST_QUEUE_LIMIT", q) && q > 0) cfg.queueLimit = static_cast<std::size_t>(q);

    // 未配置 points 时回落既有单接入点（契约 §3 / §9 降级矩阵第 1 行）
    if (cfg.points.empty()) {
        cfg.points.push_back(fallbackPoint());
    } else {
        // 已配置 points 时，既有 MAPAPP_UDP_* 仍然生效于"同端口那条"：
        // 若某条接入点的端口与 MAPAPP_UDP_PORT 相同，则用环境变量覆盖其 group/enabled。
        int envPort = 0;
        if (envInt("MAPAPP_UDP_PORT", envPort)) {
            for (auto& p : cfg.points) {
                if (p.port == envPort) {
                    envStr("MAPAPP_UDP_GROUP", p.group);
                    envBool("MAPAPP_UDP_ENABLED", p.enabled);
                }
            }
        }
    }
}

}  // namespace config
}  // namespace device_ingest
