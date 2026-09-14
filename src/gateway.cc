// gateway.cc · 门面实现（S0：骨架）
//
// S0 阶段的范围（设计方案 §9 迁移顺序）：
//   「建仓骨架：目录 + CMakeLists.txt + 空库 + README，**不接主仓**」
//   验收：新仓独立 cmake 构建通过；examples/host_demo 可运行。
//
// 因此本阶段把**公开面先立起来**（配置结构、归一化对象、反向接口、门面签名都定了），
// 实现留最小：start() 明确拒绝启动并写日志，各查询返回空值。
// 数据面（acc 接收 → prs 解析 → nrm 归一 → hlt 健康 → fan 合并）在 S1–S3 逐步接上，
// 每一步都是"可构建 + 可运行 + 可验收"的独立状态。
#include "device_ingest/gateway.h"

#include <mutex>

#include "device_ingest/hub.h"
#include "device_ingest/version.h"
#include "util/clock.h"

namespace device_ingest {

namespace {

/// 契约 §8 错误码（S0 只用得到"参数非法/资源不存在/未启用"三类）
enum Code {
    kOk             = 0,
    kParamInvalid   = 2001,
    kNotFound       = 2002,
    kCmdFailed      = 4001,
    kIngestDisabled = 5001,
};

}  // namespace

struct Gateway::Impl {
    explicit Impl(Gateway* owner) : self(owner) {}

    Gateway* self = nullptr;

    mutable std::mutex lifeMtx;
    mutable std::mutex cfgMtx;
    IngestConfig       cfg;
    bool               running = false;

    /// 已注册的解析器数（S0 无实现，恒为 0）
    std::size_t parserCount = 0;

    std::shared_ptr<ISink>         sink;
    std::shared_ptr<ILogSink>      logSink;
    std::shared_ptr<IDeviceSource> source;

    IngestConfig cfgCopy() const {
        std::lock_guard<std::mutex> lk(cfgMtx);
        return cfg;
    }

    void log(const std::string& category, const nlohmann::json& payload) const {
        auto sinkLocal = logSink;
        if (!sinkLocal) return;
        try { sinkLocal->log(category, payload); } catch (...) {}
    }
};

// ---------------------------------------------------------------- Gateway 公开面
Gateway::Gateway() : impl_(new Impl(this)) {}
Gateway::~Gateway() { stop(); }

Gateway& Gateway::instance() {
    static Gateway g;
    return g;
}

bool Gateway::start(const IngestConfig& cfgIn,
                    std::shared_ptr<ISink>    sink,
                    std::shared_ptr<ILogSink> logSink,
                    std::shared_ptr<IDeviceSource> source) {
    std::lock_guard<std::mutex> lk(impl_->lifeMtx);

    IngestConfig cfg = cfgIn;
    if (cfg.points.empty()) cfg.points.push_back(config::fallbackPoint());
    {
        std::lock_guard<std::mutex> cfgLk(impl_->cfgMtx);
        impl_->cfg = cfg;
    }
    impl_->sink    = std::move(sink);
    impl_->logSink = std::move(logSink);
    impl_->source  = std::move(source);

    if (!cfg.enabled) {
        impl_->log("acc", {{"error", "ingest.enabled=false，接入层未启用"},
                           {"code", kIngestDisabled}});
        return false;
    }

    // S0：数据面尚未接入，明确拒绝启动（而不是假装在跑）
    impl_->log("acc",
               {{"event", "start_rejected"},
                {"reason", "S0 骨架阶段：数据面（acc/prs/nrm/hlt/fan）尚未接入"},
                {"points", cfg.points.size()},
                {"version", version()}});
    return false;
}

void Gateway::stop() {
    std::lock_guard<std::mutex> lk(impl_->lifeMtx);
    impl_->running = false;
}

bool Gateway::running() const { return impl_->running; }

const IngestConfig& Gateway::config() const { return impl_->cfg; }

bool Gateway::reload(const IngestConfig& cfgIn, std::string* error) {
    std::lock_guard<std::mutex> lk(impl_->lifeMtx);
    IngestConfig cfg = cfgIn;
    if (cfg.points.empty()) cfg.points.push_back(config::fallbackPoint());
    {
        std::lock_guard<std::mutex> cfgLk(impl_->cfgMtx);
        impl_->cfg = cfg;
    }
    if (error) *error = "S0 骨架阶段：reload 只更新配置，接入点管理尚未接入";
    return true;
}

// ---------------------------------------------------------------- 接入点
bool Gateway::addPoint(const PointConfig& point, int* code, std::string* error) {
    const std::string verr = point.validate();
    if (!verr.empty()) {
        if (code) *code = kParamInvalid;
        if (error) *error = verr;
        return false;
    }
    if (code) *code = kNotFound;
    if (error) *error = "S0 骨架阶段：接入点管理（acc）尚未接入";
    return false;
}

bool Gateway::removePoint(const std::string& id, int* code, std::string* error) {
    (void)id;
    if (code) *code = kNotFound;
    if (error) *error = "S0 骨架阶段：接入点管理（acc）尚未接入";
    return false;
}

std::vector<nlohmann::json> Gateway::listPoints() const { return {}; }

nlohmann::json Gateway::pointMetrics(const std::string& id) const {
    (void)id;
    return nlohmann::json::object();
}

std::vector<PointMetrics> Gateway::allPointMetrics() const { return {}; }

// ---------------------------------------------------------------- 测试注入
int Gateway::inject(const std::string& ingestId, const std::string& bytes,
                    const std::string& peer) {
    (void)ingestId; (void)bytes; (void)peer;
    return -1;   // 无接入点可注入
}

// ---------------------------------------------------------------- 解析器
ParserRegistry& Gateway::parsers() { return ParserRegistry::builtin(); }

bool Gateway::registerParser(ParserPtr parser) {
    (void)parser;
    return false;   // S2 起接入
}

// ---------------------------------------------------------------- 设备健康
std::vector<DeviceInfo> Gateway::listDevices() const { return {}; }

bool Gateway::deviceInfo(const std::string& deviceId, DeviceInfo& out) const {
    (void)deviceId; (void)out;
    return false;
}

bool Gateway::deviceHealth(const std::string& deviceId, DeviceStats& out) const {
    (void)deviceId; (void)out;
    return false;
}

GatewayHealth Gateway::gatewayHealth() const { return GatewayHealth{}; }

GatewayStatus Gateway::status() const {
    GatewayStatus s;
    s.running    = impl_->running;
    s.enabled    = impl_->cfgCopy().enabled;
    s.parsers    = impl_->parserCount;
    s.hubEnabled = hub_enabled();
    s.version    = version();
    return s;
}

nlohmann::json GatewayStatus::toJson() const {
    return nlohmann::json{
        {"running",       running},
        {"enabled",       enabled},
        {"points",        points},
        {"pointsRunning", pointsRunning},
        {"devices",       devices},
        {"online",        online},
        {"parsers",       parsers},
        {"packets",       packets},
        {"events",        events},
        {"dropped",       dropped},
        {"parseFailed",   parseFailed},
        {"hubEnabled",    hubEnabled},
        {"version",       version},
    };
}

// ---------------------------------------------------------------- 指令（S4）
std::string Gateway::sendCommand(const CommandRequest& req, int* code, std::string* error) {
    if (req.targetDeviceId.empty() && req.targetIngestId.empty()) {
        if (code) *code = kParamInvalid;
        if (error) *error = "target 必须给 deviceId 或 ingestId 之一";
        return std::string();
    }
    if (code) *code = kCmdFailed;
    if (error) {
        *error = "指令下发在 S4 阶段实现（设备指令集尚未定稿，见需求专篇 §9 阻塞项 2）";
    }
    return std::string();
}

bool Gateway::commandState(const std::string& cmdId, CommandRecord& out) const {
    (void)cmdId; (void)out;
    return false;
}

std::vector<CommandRecord> Gateway::listCommands(const std::string& deviceId,
                                                 CommandState* stateFilter,
                                                 std::size_t limit) const {
    (void)deviceId; (void)stateFilter; (void)limit;
    return {};
}

bool Gateway::retryCommand(const std::string& cmdId) {
    (void)cmdId;
    return false;
}

std::vector<CommandLogEntry> Gateway::commandLogs(const std::string& deviceId,
                                                  std::size_t limit) const {
    (void)deviceId; (void)limit;
    return {};
}

nlohmann::json CommandLogEntry::toJson() const {
    return nlohmann::json{
        {"at", at}, {"actor", actor}, {"cmdId", cmdId}, {"deviceId", deviceId},
        {"action", action}, {"payloadDigest", payloadDigest}, {"result", result},
    };
}

// ---------------------------------------------------------------- 复位
void Gateway::resetState() {}

// ---------------------------------------------------------------- 便捷启动
bool startFromConfigText(const std::string& configText,
                         std::shared_ptr<ISink> sink,
                         std::shared_ptr<ILogSink> logSink,
                         std::shared_ptr<IDeviceSource> source,
                         std::string* error) {
    IngestConfig cfg;
    std::string err;
    if (!configText.empty()) {
        if (!config::fromJsonString(configText, cfg, err)) {
            if (error) *error = err;
            return false;
        }
    }
    if (cfg.points.empty()) cfg.points.push_back(config::fallbackPoint());
    if (!Gateway::instance().start(cfg, std::move(sink), std::move(logSink), std::move(source))) {
        if (error) *error = err.empty() ? std::string("接入层启动失败") : err;
        return false;
    }
    return true;
}

}  // namespace device_ingest
