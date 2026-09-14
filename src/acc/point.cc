// point.cc · 单接入点接收实现
#include "acc/point.h"

#include <chrono>
#include <cstring>
#include <string>
#include <vector>

#if defined(_WIN32)
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
using socket_t = SOCKET;
#define CLOSE_SOCKET closesocket
#define BAD_SOCKET INVALID_SOCKET
#define SOCK_ERR WSAGetLastError()
#define WOULD_BLOCK(e) ((e) == WSAETIMEDOUT || (e) == WSAEWOULDBLOCK)
#else
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
using socket_t = int;
#define CLOSE_SOCKET close
#define BAD_SOCKET (-1)
#define SOCK_ERR errno
#define WOULD_BLOCK(e) ((e) == EAGAIN || (e) == EWOULDBLOCK || (e) == EINTR)
#endif

#include "util/clock.h"
#include "util/net.h"

namespace device_ingest {
namespace acc {

namespace {

/// 契约 §1：单包 ≤ 8 KB（UTF-8）。给 1 字节余量，便于识别"超过 8 KB"。
constexpr std::size_t kMaxPacket = 8 * 1024;
constexpr std::size_t kBufferSize = kMaxPacket + 1;

/// 收包轮询粒度：既保证 stop() 及时生效，又不让 CPU 空转。
constexpr int kSelectTimeoutMs = 200;

std::string peerString(const sockaddr_in& from) {
    char ip[INET_ADDRSTRLEN] = {0};
    if (inet_ntop(AF_INET, &from.sin_addr, ip, sizeof(ip)) == nullptr) {
        return std::string("0.0.0.0:") + std::to_string(ntohs(from.sin_port));
    }
    return std::string(ip) + ":" + std::to_string(ntohs(from.sin_port));
}

}  // namespace

Point::Point(PointConfig cfg) : cfg_(std::move(cfg)) {}

Point::~Point() { stop(); }

void Point::setLastError(const std::string& error) {
    std::lock_guard<std::mutex> lk(mtx_);
    lastError_ = error;
}

bool Point::start(PacketHandler handler, std::string* error) {
    if (running_.load()) return true;
    if (cfg_.port <= 0 || cfg_.port > 65535) {
        const std::string e = "端口非法: " + std::to_string(cfg_.port);
        setLastError(e);
        if (error) *error = e;
        return false;
    }
    if (!net::startup()) {
        const std::string e = "网络库初始化失败（Windows 上为 WSAStartup）";
        setLastError(e);
        if (error) *error = e;
        return false;
    }

    {
        std::lock_guard<std::mutex> lk(mtx_);
        handler_ = std::move(handler);
        lastError_.clear();
    }
    quit_.store(false);
    running_.store(true);
    thread_ = std::thread(&Point::loop, this);
    return true;
}

void Point::stop() {
    if (!running_.exchange(false)) {
        if (thread_.joinable()) thread_.join();
        return;
    }
    quit_.store(true);
    // 唤醒 select：给监听 socket 发一个空包（不会进入处理分支）。
    // 即使不发，select 超时（200 ms）也会让循环退出，这只是让停机更快。
    const auto raw = sock_.load();
    if (raw != static_cast<decltype(raw)>(BAD_SOCKET) && raw != 0 && raw != ~0ULL) {
        socket_t s = static_cast<socket_t>(raw);
        sockaddr_in self{};
        self.sin_family = AF_INET;
        self.sin_port = htons(static_cast<unsigned short>(cfg_.port));
        self.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        (void)::sendto(s, "", 0, 0, reinterpret_cast<sockaddr*>(&self), sizeof(self));
    }
    if (thread_.joinable()) thread_.join();
    net::cleanup();
}

void Point::loop() {
    socket_t s = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (s == BAD_SOCKET) {
        setLastError("socket() 失败（错误码 " +
                     std::to_string(static_cast<long long>(SOCK_ERR)) + "）");
        running_.store(false);
        return;
    }

    int reuse = 1;
    (void)setsockopt(s, SOL_SOCKET, SO_REUSEADDR,
                     reinterpret_cast<const char*>(&reuse), sizeof(reuse));

    // 绑定：组播地址不是本地地址，必须绑 INADDR_ANY:port
    // （与现状 Telemetry.cc:101-110 一致）
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<unsigned short>(cfg_.port));
    addr.sin_addr.s_addr = INADDR_ANY;
    if (bind(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        setLastError("bind 失败，端口 " + std::to_string(cfg_.port) +
                     "（错误码 " + std::to_string(static_cast<long long>(SOCK_ERR)) +
                     "）。该端口可能已被占用；其它接入点不受影响。");
        CLOSE_SOCKET(s);
        running_.store(false);
        return;
    }

    // 组播加入（ING-ACC-04：join 失败只影响本接入点）
    if (!cfg_.group.empty() && net::isMulticast(cfg_.group)) {
        unsigned int groupAddr = 0;
        unsigned int ifaceAddr = htonl(INADDR_ANY);
        (void)net::parseIpv4(cfg_.group, groupAddr);
        if (!cfg_.iface.empty() && !net::parseIpv4(cfg_.iface, ifaceAddr)) {
            setLastError("iface 不是合法 IPv4 地址: " + cfg_.iface + "（已回落到默认网卡）");
        }
        ip_mreq mreq{};
        mreq.imr_multiaddr.s_addr = groupAddr;
        mreq.imr_interface.s_addr = ifaceAddr;
        if (setsockopt(s, IPPROTO_IP, IP_ADD_MEMBERSHIP,
                       reinterpret_cast<const char*>(&mreq), sizeof(mreq)) != 0) {
            setLastError("join 组播 " + cfg_.group + " 失败（错误码 " +
                         std::to_string(static_cast<long long>(SOCK_ERR)) +
                         "）。多网卡环境请显式配置 iface；单播仍可用。");
        }
#if defined(_WIN32)
        // 同机自收自发：本仓 device_sim 的单机验收场景依赖它
        DWORD loop = 1;
        (void)setsockopt(s, IPPROTO_IP, IP_MULTICAST_LOOP,
                         reinterpret_cast<const char*>(&loop), sizeof(loop));
#else
        unsigned char loop = 1;
        (void)setsockopt(s, IPPROTO_IP, IP_MULTICAST_LOOP,
                         reinterpret_cast<const char*>(&loop), sizeof(loop));
#endif
    }

    sock_.store(static_cast<decltype(sock_.load())>(s));

    std::vector<char> buf(kBufferSize, 0);
    while (!quit_.load()) {
        fd_set fds;
        FD_ZERO(&fds);
        FD_SET(s, &fds);
        timeval tv{};
        tv.tv_sec = 0;
        tv.tv_usec = kSelectTimeoutMs * 1000;
        const int rc = select(static_cast<int>(s + 1), &fds, nullptr, nullptr, &tv);
        if (rc <= 0) continue;   // 超时/被中断 → 回到循环顶检查 quit_

        sockaddr_in from{};
        socklen_t fromLen = sizeof(from);
        const int n = recvfrom(s, buf.data(), static_cast<int>(kBufferSize), 0,
                               reinterpret_cast<sockaddr*>(&from), &fromLen);
        const Millis recvAt = nowMs();

        if (n <= 0) {
            // n == 0：stop() 的唤醒空包，或对端发的空包
            //（ING-ACC-05：空包丢弃并计数；接收循环 MUST NOT 中断）
            std::lock_guard<std::mutex> lk(mtx_);
            ++empty_;
            ++dropped_;
            continue;
        }

        const std::size_t len = static_cast<std::size_t>(n);
        const std::string peer = peerString(from);

        PacketHandler handler;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            ++packets_;
            bytes_ += len;
            lastRecvAt_ = recvAt;
            lastPeer_ = peer;
            // 契约 §1 / ING-ACC-05：超过 8 KB 的包截断并计数告警
            if (len > kMaxPacket) {
                ++truncated_;
                ++dropped_;
            }
            handler = handler_;
        }

        if (handler) {
            // 回调在接收线程上执行：解析→归一→入队，全部非阻塞（ING-FAN-01）
            handler(buf.data(), len, peer, recvAt);
        }
    }

    sock_.store(static_cast<decltype(sock_.load())>(BAD_SOCKET));
    CLOSE_SOCKET(s);
    running_.store(false);
}

PointMetrics Point::metrics() const {
    std::lock_guard<std::mutex> lk(mtx_);
    PointMetrics m;
    m.id          = cfg_.id;
    m.running     = running_.load();
    m.packets     = packets_;
    m.bytes       = bytes_;
    m.dropped     = dropped_;
    m.truncated   = truncated_;
    m.empty       = empty_;
    m.parseFailed = parseFailed_;
    m.events      = events_;
    m.lastRecvAt  = lastRecvAt_;
    m.lastPeer    = lastPeer_;
    m.lastError   = lastError_;
    return m;
}

void Point::countPacket(std::size_t len, const std::string& peer, Millis recvAt) {
    std::lock_guard<std::mutex> lk(mtx_);
    ++packets_;
    bytes_ += len;
    lastRecvAt_ = recvAt;
    lastPeer_ = peer;
    // 契约 §1 / ING-ACC-05：超过 8 KB 的包截断并计数告警
    if (len > kMaxPacket) {
        ++truncated_;
        ++dropped_;
    }
}

void Point::countEvent() {
    std::lock_guard<std::mutex> lk(mtx_);
    ++events_;
}

void Point::countParseFailed() {
    std::lock_guard<std::mutex> lk(mtx_);
    ++parseFailed_;
}

void Point::mergeNormalizeStats(const void* statsJson) {
    // 归一化计数目前只用于日志/诊断；保留接口避免将来改动公开面。
    (void)statsJson;
}

bool Point::send(const std::string& ip, int port, const std::string& payload,
                 std::string* error) const {
    return net::sendUdp(ip, port, payload, cfg_.iface, error);
}

}  // namespace acc
}  // namespace device_ingest
