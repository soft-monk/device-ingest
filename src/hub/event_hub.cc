// event_hub.cc · WS 广播出口
//
// 设计方案 §11：core/EventHub.h/.cc（72+32 行）**搬**到这里，
// 唯一的改动是把 logEvent（现状 EventHub.cc:68-70 直接写 SQLite）移出为 ILogSink。
// 本文件内**不出现任何 SQL**（设计方案 G4）。
#include "device_ingest/hub.h"

#include <algorithm>

#if defined(DEVICE_INGEST_HAS_HUB)
#include <drogon/WebSocketConnection.h>
#endif

#include "util/clock.h"

namespace device_ingest {

EventHub& EventHub::instance() {
    static EventHub hub;
    return hub;
}

// ---------------------------------------------------------------- 框架中立面
void EventHub::addClient(const IHubClientPtr& client) {
    if (!client) return;
    std::lock_guard<std::mutex> lk(mtx_);
    clients_.push_back(client);
}

void EventHub::removeClient(const IHubClientPtr& client) {
    std::lock_guard<std::mutex> lk(mtx_);
    clients_.erase(std::remove(clients_.begin(), clients_.end(), client), clients_.end());
}

std::size_t EventHub::clientCount() {
    std::lock_guard<std::mutex> lk(mtx_);
    std::size_t n = clients_.size();
#if defined(DEVICE_INGEST_HAS_HUB)
    n += wsClients_.size();
#endif
    return n;
}

std::string EventHub::envelope(const std::string& type, const nlohmann::json& data) {
    nlohmann::json msg;
    msg["type"] = type;
    msg["data"] = data;
    // 时间戳口径与现状 EventHub::sendTo/broadcast 完全一致（epoch 毫秒）
    msg["ts"] = nowMs();
    return msg.dump();
}

void EventHub::broadcast(const std::string& type, const nlohmann::json& data) {
    const std::string payload = envelope(type, data);

    // 先取快照再发送：发送期间允许客户端增删（与现状做法一致）
    std::vector<IHubClientPtr> snapshot;
#if defined(DEVICE_INGEST_HAS_HUB)
    std::vector<std::shared_ptr<drogon::WebSocketConnection>> wsSnapshot;
#endif
    {
        std::lock_guard<std::mutex> lk(mtx_);
        snapshot = clients_;
#if defined(DEVICE_INGEST_HAS_HUB)
        wsSnapshot = wsClients_;
#endif
        ++broadcastCount_;
    }
    for (auto& c : snapshot) {
        if (c) {
            // 单个客户端抛异常不得中断整轮广播（ING-PRS-04 的隔离精神）
            try { c->onMessage(payload); } catch (...) {}
        }
    }
#if defined(DEVICE_INGEST_HAS_HUB)
    for (auto& c : wsSnapshot) {
        if (c) c->send(payload);
    }
#endif
}

std::uint64_t EventHub::broadcastCount() const {
    std::lock_guard<std::mutex> lk(mtx_);
    return broadcastCount_;
}

void EventHub::clearClients() {
    std::lock_guard<std::mutex> lk(mtx_);
    clients_.clear();
#if defined(DEVICE_INGEST_HAS_HUB)
    wsClients_.clear();
#endif
}

// ---------------------------------------------------------------- Drogon 面（签名与现状一致）
#if defined(DEVICE_INGEST_HAS_HUB)
void EventHub::add(const std::shared_ptr<drogon::WebSocketConnection>& conn) {
    if (!conn) return;
    std::lock_guard<std::mutex> lk(mtx_);
    wsClients_.push_back(conn);
}

void EventHub::remove(const std::shared_ptr<drogon::WebSocketConnection>& conn) {
    std::lock_guard<std::mutex> lk(mtx_);
    wsClients_.erase(std::remove(wsClients_.begin(), wsClients_.end(), conn), wsClients_.end());
}

void EventHub::sendTo(const std::shared_ptr<drogon::WebSocketConnection>& conn,
                      const std::string& type, const nlohmann::json& data) {
    if (!conn) return;
    conn->send(envelope(type, data));
}
#endif

}  // namespace device_ingest
