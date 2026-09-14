// device-ingest · EventHub：WS 广播出口（随模块走）
//
// 设计方案 §2 耦合①：现状 core/EventHub.{h,cc} 的广播能力属模块能力
// （含连接管理），不是业务，因此**随模块走**；主仓改为
// `#include <device_ingest/hub.h>`（配合 SR2 的转发头过渡）。
//
// 两种构建形态：
//   - DEVICE_INGEST_WITH_HUB=ON  ：hub 依赖 Drogon，接口与现状**签名一致**，
//                                 主仓 WsGateway.cc 无需改动（设计方案 §7 广播行）；
//   - DEVICE_INGEST_WITH_HUB=OFF ：hub 不含任何 Web 框架，只保留
//                                 "客户端计数 + 事件回调"两个中立能力，
//                                 可编译到纯采集机（设计方案 §8.1）。
//
// 另外提供**与 Web 框架无关**的中立客户端面（IHubClient / addClient），
// 供本仓的验收工具与自测直接观察广播内容——不引入 Drogon 也能验证 S1/S2/S3。
#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

// ⚠️ 前置声明必须放在**全局命名空间**。写在 namespace device_ingest 里面会变成
//    `device_ingest::drogon::WebSocketConnection`——那不是 drogon 的类型，
//    症状是编译 hub 层时报"使用了未定义类型"（C2027）。
//    这个错误长期潜伏：在打开 DEVICE_INGEST_WITH_HUB 之前，这段代码从未被真正编译过。
#if defined(DEVICE_INGEST_HAS_HUB)
namespace drogon { class WebSocketConnection; }
#endif

namespace device_ingest {

/// 框架中立的广播客户端。
/// 宿主用别的 WS 框架（或本仓工具想直接看广播内容）时实现它即可。
class IHubClient {
public:
    virtual ~IHubClient() = default;
    /// 收到一条完整 WS 信封 `{"type":...,"data":...,"ts":...}` 的序列化文本。
    virtual void onMessage(const std::string& payload) = 0;
};

using IHubClientPtr = std::shared_ptr<IHubClient>;

/// WS 广播中枢。单通道 JSON 事件 `{type,data,ts}` 广播至所有在线客户端；
/// 一次发布、多端订阅；并发安全（与现状 core/EventHub 行为一致）。
class EventHub {
public:
    static EventHub& instance();

    // ---------------- 与现状签名一致（DEVICE_INGEST_WITH_HUB=ON 时可用）----------------
#if defined(DEVICE_INGEST_HAS_HUB)
    void add(const std::shared_ptr<drogon::WebSocketConnection>& conn);
    void remove(const std::shared_ptr<drogon::WebSocketConnection>& conn);
    static void sendTo(const std::shared_ptr<drogon::WebSocketConnection>& conn,
                       const std::string& type, const nlohmann::json& data);
#endif

    // ---------------- 框架中立面（两种构建都有）----------------
    void addClient(const IHubClientPtr& client);
    void removeClient(const IHubClientPtr& client);
    /// 在线客户端数（含 Drogon 侧与中立侧）。
    std::size_t clientCount();

    /// 广播事件：data 为对象；内部自动附 `ts`（epoch 毫秒）。无客户端时安静返回。
    void broadcast(const std::string& type, const nlohmann::json& data);

    /// 组装信封（不发送），便于宿主自己转发或测试断言。
    static std::string envelope(const std::string& type, const nlohmann::json& data);

    /// 广播总条数（可观测性；不随客户端增删清零）。
    std::uint64_t broadcastCount() const;

    /// 清空全部客户端（进程退出/测试复位用）。
    void clearClients();

private:
    EventHub() = default;
    mutable std::mutex mtx_;
#if defined(DEVICE_INGEST_HAS_HUB)
    std::vector<std::shared_ptr<drogon::WebSocketConnection>> wsClients_;
#endif
    std::vector<IHubClientPtr> clients_;
    std::uint64_t broadcastCount_ = 0;
};

}  // namespace device_ingest
