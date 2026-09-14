// gateway.cc · 门面实现（S2：解析器体系 + 归一化）
//
// 设计方案 §6 数据流（本文件就是那张图的代码形态，已接通其中标 ✅ 的环节）：
//
//   外设 ──UDP──▶ acc::Point（每接入点一个接收线程）            ✅ S1
//                      │ 原始字节 + peer + recvAt
//                      ▼
//               prs::ParserRegistry → IParser::parse()         ✅ S2
//                      │ ParsedEvent[]（解析器无状态；异常在此隔离）
//                      ▼
//               nrm::normalize()                                ✅ S2
//                      │ NormalizedEvent
//                      ├──▶ hlt::HealthTracker（台账/心跳/丢包/乱序）⏳ S3
//                      └──▶ fan::Queue（进程内队列）               ✅ S1
//                               │ 单帧合并（mergeWindowMs）        ✅ S1
//                               ▼
//                        ISink::onBatch / EventHub::broadcast    ✅ S1
#include "device_ingest/gateway.h"

#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <sstream>
#include <thread>

#include "acc/manager.h"
#include "device_ingest/hub.h"
#include "device_ingest/version.h"
#include "fan/merge.h"
#include "fan/queue.h"
#include "nrm/normalize.h"
#include "prs/registry.h"
#include "util/clock.h"

namespace device_ingest {

namespace {

/// 契约 §8 错误码
enum Code {
    kOk              = 0,
    kParamInvalid    = 2001,
    kNotFound        = 2002,
    kParserMissing   = 3001,
    kPortUnavailable = 3002,
    kJoinFailed      = 3003,
    kParseFailed     = 3004,
    kCmdFailed       = 4001,
    kCmdTimeout      = 4002,
    kIngestDisabled  = 5001,
};

}  // namespace

// ---------------------------------------------------------------- Impl
struct Gateway::Impl {
    explicit Impl(Gateway* owner) : self(owner) {}

    Gateway* self = nullptr;

    mutable std::mutex  lifeMtx;          // 保护 start/stop/reload 的互斥
    mutable std::mutex  cfgMtx;           // 保护 cfg
    IngestConfig        cfg;
    std::atomic<bool>   running{false};

    ParserRegistry      parsers;
    std::unique_ptr<acc::Manager> manager;
    std::unique_ptr<fan::Queue>   queue;   // hlt 在 S3 接入

    std::shared_ptr<ISink>         sink;
    std::shared_ptr<ILogSink>      logSink;
    std::shared_ptr<IDeviceSource> source;

    /// 是否把归一事件也广播到 hub。
    /// 默认 false：新设备类型的归一事件只经 ISink 交给宿主（契约里没有这些事件名）；
    /// 置 true 时按 `telemetry.<kind>` / 接入点 topic 广播，便于宿主或验收工具观察结果。
    std::atomic<bool> emitNormalized{false};

    std::thread consumerThread;
    std::atomic<bool> stopFlag{false};

    std::atomic<std::uint64_t> totalEvents{0};
    std::atomic<std::uint64_t> totalParseFailed{0};

    // 带宽观测（packetsPerSec）
    mutable std::mutex bwMtx;
    std::uint64_t bwLastTotal = 0;
    std::int64_t  bwLastAt = 0;
    double        packetsPerSec = 0.0;

    // ---------------------------------------------------------------- 小工具
    IngestConfig cfgCopy() const {
        std::lock_guard<std::mutex> lk(cfgMtx);
        return cfg;
    }

    void log(const std::string& category, const nlohmann::json& payload) const {
        auto sinkLocal = logSink;   // 拷贝一份，避免并发替换时读到半个
        if (!sinkLocal) return;
        try { sinkLocal->log(category, payload); } catch (...) {}
    }

    void alert(AlertEvent::Level level, const std::string& code,
               const std::string& title, const std::string& text,
               const std::string& ingestId = std::string()) {
        AlertEvent ev;
        ev.level = level;
        ev.code = code;
        ev.title = title;
        ev.text = text;
        ev.ingestId = ingestId;
        auto sinkLocal = sink;
        if (sinkLocal) {
            try { sinkLocal->onAlert(ev); } catch (...) {}
        }
        EventHub::instance().broadcast("alert", ev.toJson());
    }

    // ---------------------------------------------------------------- 收包 → 解析 → 归一 → 入队
    void handlePacket(const PointConfig& point, const char* data, std::size_t len,
                      const std::string& peer, Millis recvAt) {
        if (len == 0) return;

        ParserPtr parser = parsers.find(point.parserId);
        acc::Point* pt = manager ? manager->get(point.id) : nullptr;
        if (!parser) {
            if (pt) pt->countParseFailed();
            totalParseFailed.fetch_add(1);
            return;
        }

        std::vector<ParsedEvent> parsed;
        bool ok = false;
        try {
            // ING-PRS-04：解析器抛异常只影响该接入点，不中断接收线程、不影响其它接入点
            ok = parser->parse(data, len, peer, parsed);
        } catch (const std::exception& ex) {
            ok = false;
            log("prs", {{"ingestId", point.id}, {"parserId", point.parserId},
                        {"error", std::string("解析器抛异常: ") + ex.what()}});
            alert(AlertEvent::Level::Warn, "parser_exception",
                  "解析器异常",
                  std::string("解析器 ") + point.parserId + " 抛异常: " + ex.what(),
                  point.id);
        } catch (...) {
            ok = false;
            log("prs", {{"ingestId", point.id}, {"parserId", point.parserId},
                        {"error", "解析器抛出未知异常"}});
        }
        if (!ok || parsed.empty()) {
            if (!ok) {
                if (pt) pt->countParseFailed();
                totalParseFailed.fetch_add(1);
            }
            return;
        }

        const std::size_t n = parsed.size();
        for (std::size_t i = 0; i < n; ++i) {
            nrm::NormalizeStats nstats;
            NormalizedEvent ev = nrm::normalize(parsed[i], point, peer, len, recvAt, &nstats);
            if (nstats.tsFromServer > 0 || nstats.deviceIdFallback > 0 ||
                nstats.coordDroppedLng > 0 || nstats.coordDroppedLat > 0) {
                log("nrm", {{"ingestId", point.id}, {"deviceId", ev.deviceId},
                            {"stats", nstats.toJson()}});
            }

            const bool legacy = isLegacyKind(ev.kind);
            IngestEvent item;
            item.type       = nrm::resolveEventName(ev.kind, point.topic);
            item.ingestId   = point.id;
            item.data       = nrm::toEventData(ev, parsed[i], legacy);
            item.normalized = ev;

            if (queue) queue->push(std::move(item));
            // 接入点级事件计数（ING-ACC-06）
            if (pt) pt->countEvent();
            totalEvents.fetch_add(1);
        }
    }

    // ---------------------------------------------------------------- 一批事件的统一出口
    /// 数据出口在这里收口：
    ///   - 宿主：只调 `onBatch`（**不再逐条 onEvent**，否则宿主会收到两份数据——
    ///     `ISink::onBatch` 的默认实现本身就是逐条转调 onEvent）；
    ///   - 广播：`emitNormalized` 打开时按事件名广播（信封 {type,data,ts} 不变）。
    void deliver(const std::vector<IngestEvent>& evs) {
        if (evs.empty()) return;
        auto sinkLocal = sink;
        if (sinkLocal) {
            try { sinkLocal->onBatch(evs); } catch (...) {}
        }
        if (emitNormalized.load()) {
            for (const auto& ev : evs) {
                EventHub::instance().broadcast(ev.type, ev.data);
            }
        }
    }

    // ---------------------------------------------------------------- 队列消费线程（单帧合并）
    void consumerLoop() {
        while (!stopFlag.load()) {
            const IngestConfig c = cfgCopy();
            const int window = c.mergeWindowMs;

            if (window <= 0) {
                // 配 0 = 逐包直发（ING-FAN-02）
                IngestEvent ev;
                if (!queue->pop(ev, 100)) {
                    if (queue->closed()) break;
                    continue;
                }
                deliver({ev});
                continue;
            }

            // 窗内先排空队列，再整体派发。
            // 关键：排空**不重置**窗口终点——否则高频流会把窗无限延长（等于没合并）。
            // 也不因"暂时空"提前收工，避免一次突发被切成几批（合并比会虚高）。
            std::vector<IngestEvent> batch;
            IngestEvent first;
            if (!queue->pop(first, 100)) {
                if (queue->closed()) break;
                continue;
            }
            batch.push_back(std::move(first));

            const std::int64_t deadline = steadyMs() + window;
            while (steadyMs() < deadline && !stopFlag.load()) {
                IngestEvent ev;
                if (queue->pop(ev, 5)) batch.push_back(std::move(ev));
            }
            if (batch.empty()) continue;

            const std::size_t rawCount = batch.size();
            std::vector<IngestEvent> merged = fan::mergeBatch(batch);
            deliver(merged);

            // 合并比（merged == batch 表示窗内无同设备重复）
            log("fan", {{"batch", rawCount}, {"merged", merged.size()},
                        {"windowMs", window}});
        }
        // 停机前把队列排空，避免丢最后一批
        IngestEvent ev;
        std::vector<IngestEvent> tail;
        while (queue->pop(ev, 0)) tail.push_back(std::move(ev));
        if (!tail.empty()) deliver(fan::mergeBatch(tail));
    }

    // ---------------------------------------------------------------- 启动 / 停止
    /// 收包回调工厂：把"接入点配置 + 字节 + 来源"接进处理链路。
    /// 回调在接收线程上执行，因此这里不做任何阻塞操作（ING-FAN-01）。
    std::function<acc::PacketHandler(const PointConfig&)> packetHandlerFactory() {
        return [this](const PointConfig& pc) {
            return [this, pc](const char* data, std::size_t len,
                              const std::string& peer, Millis recvAt) {
                handlePacket(pc, data, len, peer, recvAt);
            };
        };
    }

    bool startPoints(const IngestConfig& c, std::string* error) {
        bool anyStarted = false;
        std::ostringstream failures;
        for (const auto& p : c.points) {
            if (!parsers.has(p.parserId)) {
                // ING-PRS-02 / 契约 §8 code=3001：拒绝该接入点启动，不影响其它接入点
                const std::string msg = "接入点 " + p.id + " 引用了未注册的解析器: " + p.parserId;
                failures << msg << "; ";
                alert(AlertEvent::Level::Critical, "parser_not_registered",
                      "解析器未注册", msg, p.id);
                log("acc", {{"ingestId", p.id}, {"error", msg}, {"code", kParserMissing}});
                continue;
            }
            std::string addErr;
            const bool ok = manager->add(p, packetHandlerFactory(), &addErr);
            if (ok) {
                anyStarted = true;
                const PointMetrics m = manager->get(p.id) ? manager->get(p.id)->metrics()
                                                          : PointMetrics();
                if (!m.lastError.empty()) {
                    alert(AlertEvent::Level::Warn, "point_degraded",
                          "接入点降级", m.lastError, p.id);
                }
                log("acc", {{"ingestId", p.id}, {"port", p.port}, {"group", p.group},
                            {"topic", p.topic}, {"running", m.running}});
            } else {
                failures << "接入点 " << p.id << " 启动失败: " << addErr << "; ";
                alert(AlertEvent::Level::Critical, "point_bind_failed",
                      "接入点启动失败", addErr, p.id);
                log("acc", {{"ingestId", p.id}, {"error", addErr}, {"code", kPortUnavailable}});
            }
        }
        if (!anyStarted) {
            if (error) {
                *error = failures.str().empty() ? std::string("没有任何接入点启动成功")
                                                : failures.str();
            }
            return false;
        }
        return true;
    }
};

// ---------------------------------------------------------------- Gateway 公开面
Gateway::Gateway() : impl_(new Impl(this)) {
    // 每个 Gateway 实例持有自己的注册表，并预置内置解析器。
    // 必须是**实例成员**而不是 static 副本：多实例（测试、多接入层）之间不能互相污染。
    impl_->parsers.addBuiltins();
}

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
    if (impl_->running.load()) return true;

    IngestConfig cfg = cfgIn;
    if (cfg.points.empty()) cfg.points.push_back(config::fallbackPoint());
    if (!cfg.enabled) {
        impl_->log("acc", {{"error", "ingest.enabled=false，接入层未启用"},
                           {"code", kIngestDisabled}});
        return false;
    }

    {
        std::lock_guard<std::mutex> cfgLk(impl_->cfgMtx);
        impl_->cfg = cfg;
    }
    impl_->sink     = std::move(sink);
    impl_->logSink  = std::move(logSink);
    impl_->source   = std::move(source);
    impl_->emitNormalized.store(cfg.emitRawEvents);

    impl_->manager.reset(new acc::Manager());
    impl_->queue.reset(new fan::Queue(cfg.queueLimit));

    impl_->totalEvents.store(0);
    impl_->totalParseFailed.store(0);
    impl_->stopFlag.store(false);
    impl_->running.store(true);

    std::string err;
    if (!impl_->startPoints(cfg, &err)) {
        impl_->running.store(false);
        impl_->manager->stopAll();
        impl_->manager.reset();
        impl_->queue.reset();
        impl_->log("acc", {{"error", err}, {"code", kPortUnavailable}});
        return false;
    }

    impl_->consumerThread = std::thread([this] { impl_->consumerLoop(); });
    impl_->log("acc", {{"event", "started"}, {"points", impl_->manager->size()},
                       {"mergeWindowMs", cfg.mergeWindowMs},
                       {"hubEnabled", hub_enabled()}});
    return true;
}

void Gateway::stop() {
    std::lock_guard<std::mutex> lk(impl_->lifeMtx);
    if (!impl_->running.exchange(false)) return;

    // 顺序：先停接收（不再产生新事件）→ 再停巡检（S3）→ 最后排空队列
    if (impl_->manager) impl_->manager->stopAll();
    impl_->stopFlag.store(true);
    if (impl_->queue) impl_->queue->close();
    if (impl_->consumerThread.joinable()) impl_->consumerThread.join();

    impl_->manager.reset();
    impl_->queue.reset();
    impl_->log("acc", {{"event", "stopped"}});
}

bool Gateway::running() const { return impl_->running.load(); }

const IngestConfig& Gateway::config() const { return impl_->cfg; }

bool Gateway::reload(const IngestConfig& cfgIn, std::string* error) {
    std::lock_guard<std::mutex> lk(impl_->lifeMtx);
    IngestConfig cfg = cfgIn;
    if (cfg.points.empty()) cfg.points.push_back(config::fallbackPoint());
    if (!cfg.enabled) {
        if (error) *error = "ingest.enabled=false，reload 拒绝执行（请先 stop）";
        return false;
    }

    {
        std::lock_guard<std::mutex> cfgLk(impl_->cfgMtx);
        impl_->cfg = cfg;
    }
    impl_->emitNormalized.store(cfg.emitRawEvents);
    if (impl_->queue) impl_->queue->setLimit(cfg.queueLimit);

    if (!impl_->running.load()) return true;   // 未运行：只更新配置

    // 增量收敛接入点：**既有接入点不重启**（ING-NFR-03：增删不中断既有接入点收包）
    const std::vector<PointConfig> current = impl_->manager->configs();
    for (const auto& oldP : current) {
        bool stillThere = false;
        for (const auto& newP : cfg.points) {
            if (newP.id == oldP.id) { stillThere = true; break; }
        }
        if (!stillThere) {
            impl_->manager->remove(oldP.id);
            impl_->log("acc", {{"event", "point_removed"}, {"ingestId", oldP.id}});
        }
    }
    for (const auto& newP : cfg.points) {
        const acc::Point* existing = impl_->manager->get(newP.id);
        if (existing == nullptr) {
            if (!impl_->parsers.has(newP.parserId)) {
                impl_->log("acc", {{"error", "解析器未注册"}, {"ingestId", newP.id},
                                   {"parserId", newP.parserId}, {"code", kParserMissing}});
                if (error) *error = "接入点 " + newP.id + " 的解析器未注册: " + newP.parserId;
                continue;
            }
            std::string addErr;
            impl_->manager->add(newP, impl_->packetHandlerFactory(), &addErr);
            impl_->log("acc", {{"event", "point_added"}, {"ingestId", newP.id},
                               {"error", addErr}});
        } else {
            const PointConfig oldCfg = existing->config();
            const bool changed = oldCfg.port != newP.port || oldCfg.group != newP.group ||
                                 oldCfg.iface != newP.iface ||
                                 oldCfg.parserId != newP.parserId ||
                                 oldCfg.enabled != newP.enabled;
            if (changed) {
                // 端口/组播/解析器变了：重启这一条，其它接入点不受影响
                impl_->manager->remove(newP.id);
                std::string addErr;
                impl_->manager->add(newP, impl_->packetHandlerFactory(), &addErr);
                impl_->log("acc", {{"event", "point_restarted"}, {"ingestId", newP.id},
                                   {"error", addErr}});
            }
        }
    }
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
    if (!impl_->parsers.has(point.parserId)) {
        if (code) *code = kParserMissing;
        if (error) *error = "解析器未注册: " + point.parserId;
        return false;
    }
    std::string addErr;
    const bool ok = impl_->manager->add(point, impl_->packetHandlerFactory(), &addErr);
    if (!ok) {
        if (code) *code = kPortUnavailable;
        if (error) *error = addErr;
        return false;
    }
    {
        // 热增后写回配置，保证 listPoints()/status() 与运行态一致
        std::lock_guard<std::mutex> cfgLk(impl_->cfgMtx);
        bool found = false;
        for (auto& p : impl_->cfg.points) {
            if (p.id == point.id) { p = point; found = true; break; }
        }
        if (!found) impl_->cfg.points.push_back(point);
    }
    impl_->log("acc", {{"event", "point_added"}, {"ingestId", point.id}, {"port", point.port}});
    if (code) *code = kOk;
    return true;
}

bool Gateway::removePoint(const std::string& id, int* code, std::string* error) {
    if (!impl_->manager->remove(id)) {
        if (code) *code = kNotFound;
        if (error) *error = "接入点不存在: " + id;
        return false;
    }
    {
        std::lock_guard<std::mutex> cfgLk(impl_->cfgMtx);
        auto& pts = impl_->cfg.points;
        for (auto it = pts.begin(); it != pts.end(); ++it) {
            if (it->id == id) { pts.erase(it); break; }
        }
    }
    impl_->log("acc", {{"event", "point_removed"}, {"ingestId", id}});
    if (code) *code = kOk;
    return true;
}

std::vector<nlohmann::json> Gateway::listPoints() const {
    // 配置 + 运行状态合并输出（契约 §4.1 GET /points）。
    //
    // 以**配置**为基准遍历：解析器未注册、端口被占、bind 失败等原因导致"没起来的"
    // 接入点同样要出现在清单里（running=false + lastError 给可读原因）——
    // 否则运维看不到它，只能去翻日志（契约 §9 降级矩阵要求"该接入点标 running:false + 可读原因"）。
    std::vector<nlohmann::json> out;
    const std::vector<PointMetrics> metrics = impl_->manager->metrics();
    const IngestConfig cfg = impl_->cfgCopy();

    auto metricsOf = [&](const std::string& id) -> const PointMetrics* {
        for (const auto& m : metrics) {
            if (m.id == id) return &m;
        }
        return nullptr;
    };

    std::vector<std::string> emitted;
    auto emit = [&](const PointConfig& pc) {
        const PointMetrics* m = metricsOf(pc.id);
        nlohmann::json j{
            {"id",          pc.id},
            {"enabled",     pc.enabled},
            {"group",       pc.group},
            {"port",        pc.port},
            {"iface",       pc.iface},
            {"parserId",    pc.parserId},
            {"deviceType",  pc.deviceType},
            {"topic",       pc.topic},
            // 事件名解析结果：既有 4 类由 kind 决定；其余取 topic；空则 telemetry.<kind>
            {"eventName",   pc.topic.empty() ? std::string("(由 kind 决定)") : pc.topic},
            {"running",     false},
            {"lastRecvAt",  static_cast<Millis>(0)},
            {"packets",     static_cast<std::uint64_t>(0)},
            {"parseFailed", static_cast<std::uint64_t>(0)},
            {"lastError",   std::string()},
        };
        nlohmann::json pm = nlohmann::json::object();
        if (m != nullptr) {
            j["running"]     = m->running;
            j["lastRecvAt"]  = m->lastRecvAt;
            j["packets"]     = m->packets;
            j["parseFailed"] = m->parseFailed;
            j["lastError"]   = m->lastError;
            pm = m->toJson();
        } else {
            j["lastError"] = pc.enabled ? std::string("未启动（解析器未注册或启动失败）")
                                        : std::string("配置为 disabled");
        }
        j["metrics"] = std::move(pm);
        out.push_back(std::move(j));
        emitted.push_back(pc.id);
    };

    for (const auto& pc : cfg.points) emit(pc);
    // 运行中但已不在配置里的接入点（热增删的中间态）也要可见
    for (const auto& m : metrics) {
        bool seen = false;
        for (const auto& id : emitted) {
            if (id == m.id) { seen = true; break; }
        }
        if (!seen) {
            PointConfig pc;
            if (acc::Point* p = impl_->manager->get(m.id)) pc = p->config();
            pc.id = m.id;
            emit(pc);
        }
    }
    return out;
}

nlohmann::json Gateway::pointMetrics(const std::string& id) const {
    for (const auto& m : impl_->manager->metrics()) {
        if (m.id == id) return m.toJson();
    }
    return nlohmann::json::object();
}

std::vector<PointMetrics> Gateway::allPointMetrics() const {
    return impl_->manager->metrics();
}

// ---------------------------------------------------------------- 测试注入
int Gateway::inject(const std::string& ingestId, const std::string& bytes,
                    const std::string& peer) {
    // inject 不经过网络，但走**完全相同**的处理链路
    PointConfig point;
    acc::Point* livePoint = impl_->manager->get(ingestId);
    if (livePoint != nullptr) {
        point = livePoint->config();
    } else {
        // 未启动时也允许注入：用配置里的接入点定义（便于无网络单测）
        std::lock_guard<std::mutex> cfgLk(impl_->cfgMtx);
        bool found = false;
        for (const auto& p : impl_->cfg.points) {
            if (p.id == ingestId) { point = p; found = true; break; }
        }
        if (!found) return -1;
    }

    // 注入也是"到达"：接入点指标要看得见（ING-ACC-06），否则无网环境下指标永远是 0。
    // recvAt 只取一次：接入点指标与事件对象的到达时刻必须一致（排障时对得上）。
    const Millis recvAt = nowMs();
    if (livePoint != nullptr) {
        livePoint->countPacket(bytes.size(), peer, recvAt);
    }

    const std::size_t before = impl_->totalEvents.load();
    impl_->handlePacket(point, bytes.data(), bytes.size(), peer, recvAt);
    const std::size_t after = impl_->totalEvents.load();
    return static_cast<int>(after - before);
}

// ---------------------------------------------------------------- 解析器
ParserRegistry& Gateway::parsers() { return impl_->parsers; }

bool Gateway::registerParser(ParserPtr parser) {
    return impl_->parsers.add(std::move(parser));
}

// ---------------------------------------------------------------- 设备健康（S3 接入）
std::vector<DeviceInfo> Gateway::listDevices() const { return {}; }

bool Gateway::deviceInfo(const std::string& deviceId, DeviceInfo& out) const {
    (void)deviceId; (void)out;
    return false;
}

bool Gateway::deviceHealth(const std::string& deviceId, DeviceStats& out) const {
    (void)deviceId; (void)out;
    return false;
}

GatewayHealth Gateway::gatewayHealth() const {
    GatewayHealth h;
    const std::vector<PointMetrics> metrics = impl_->manager ? impl_->manager->metrics()
                                                             : std::vector<PointMetrics>();
    h.points = metrics.size();
    for (const auto& m : metrics) {
        if (m.running) ++h.pointsRunning;
        h.packets     += m.packets;
        h.events      += m.events;
        h.dropped     += m.dropped;
        h.parseFailed += m.parseFailed;
    }
    if (impl_->queue) {
        h.queueDepth   = impl_->queue->depth();
        h.queueDropped = impl_->queue->droppedTotal();
    }
    // 带宽观测：用两次采样之间的增量算，避免除零与抖动
    const std::uint64_t total = h.packets;
    const std::int64_t  now   = steadyMs();
    {
        std::lock_guard<std::mutex> lk(impl_->bwMtx);
        if (impl_->bwLastAt == 0) {
            impl_->bwLastAt = now;
            impl_->bwLastTotal = total;
        } else if (now - impl_->bwLastAt >= 1000) {
            const double dt = static_cast<double>(now - impl_->bwLastAt) / 1000.0;
            impl_->packetsPerSec = static_cast<double>(total - impl_->bwLastTotal) / dt;
            impl_->bwLastAt = now;
            impl_->bwLastTotal = total;
        }
        h.packetsPerSec = impl_->packetsPerSec;
    }
    return h;
}

GatewayStatus Gateway::status() const {
    GatewayStatus s;
    s.running = impl_->running.load();
    const IngestConfig c = impl_->cfgCopy();
    s.enabled = c.enabled;
    const std::vector<PointMetrics> metrics = impl_->manager ? impl_->manager->metrics()
                                                             : std::vector<PointMetrics>();
    s.points = metrics.size();
    for (const auto& m : metrics) {
        if (m.running) ++s.pointsRunning;
        s.packets     += m.packets;
        s.events      += m.events;
        s.dropped     += m.dropped;
        s.parseFailed += m.parseFailed;
    }
    s.parsers    = impl_->parsers.size();
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
void Gateway::resetState() {
    if (impl_->queue) impl_->queue->clear();
    impl_->totalEvents.store(0);
    impl_->totalParseFailed.store(0);
}

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
