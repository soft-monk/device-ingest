// util/net.cc
#include "net.h"

#include <atomic>
#include <cstdio>
#include <cstring>

#if defined(_WIN32)
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
using socket_t = SOCKET;
#define CLOSE_SOCKET closesocket
#define BAD_SOCKET INVALID_SOCKET
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
using socket_t = int;
#define CLOSE_SOCKET close
#define BAD_SOCKET (-1)
#endif

namespace device_ingest {
namespace net {

namespace {
std::atomic<int> g_wsaRefs{0};
bool g_wsaOk = true;
}  // namespace

bool startup() {
#if defined(_WIN32)
    if (g_wsaRefs.fetch_add(1) == 0) {
        WSADATA wsa{};
        if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
            g_wsaOk = false;
            return false;
        }
        g_wsaOk = true;
    }
    return g_wsaOk;
#else
    g_wsaRefs.fetch_add(1);
    return true;
#endif
}

void cleanup() {
#if defined(_WIN32)
    if (g_wsaRefs.fetch_sub(1) == 1) {
        WSACleanup();
    }
#else
    g_wsaRefs.fetch_sub(1);
#endif
}

bool splitPeer(const std::string& peer, std::string& ip, int& port) {
    const auto pos = peer.rfind(':');
    if (pos == std::string::npos || pos == 0) {
        ip = peer;
        return false;
    }
    ip = peer.substr(0, pos);
    const std::string portStr = peer.substr(pos + 1);
    if (portStr.empty()) return false;
    int v = 0;
    for (char c : portStr) {
        if (c < '0' || c > '9') return false;
        v = v * 10 + (c - '0');
        if (v > 65535) return false;
    }
    port = v;
    return true;
}

std::string normalizePeer(const std::string& peer, int defaultPort) {
    std::string ip;
    int port = defaultPort;
    if (splitPeer(peer, ip, port)) {
        return ip + ":" + std::to_string(port);
    }
    if (defaultPort > 0) {
        return peer + ":" + std::to_string(defaultPort);
    }
    return peer;
}

bool isMulticast(const std::string& ip) {
    unsigned int addr = 0;
    if (!parseIpv4(ip, addr)) return false;
    const std::uint32_t host = ntohl(addr);
    return (host & 0xF0000000u) == 0xE0000000u;  // 224.0.0.0/4
}

bool parseIpv4(const std::string& ip, unsigned int& out) {
    if (ip.empty()) return false;
    in_addr a{};
    if (inet_pton(AF_INET, ip.c_str(), &a) != 1) return false;
    out = a.s_addr;
    return true;
}

bool resolveIpv4(const std::string& host, unsigned int& out) {
    if (parseIpv4(host, out)) return true;   // 绝大多数情况：点分十进制字面量
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    addrinfo* res = nullptr;
    if (::getaddrinfo(host.c_str(), nullptr, &hints, &res) != 0 || res == nullptr) {
        return false;
    }
    bool ok = false;
    for (addrinfo* p = res; p != nullptr; p = p->ai_next) {
        if (p->ai_family == AF_INET && p->ai_addr != nullptr) {
            out = reinterpret_cast<sockaddr_in*>(p->ai_addr)->sin_addr.s_addr;
            ok = true;
            break;
        }
    }
    ::freeaddrinfo(res);
    return ok;
}

bool sendUdp(const std::string& ip, int port, const std::string& payload,
             const std::string& iface, std::string* error) {
    if (ip.empty() || port <= 0 || port > 65535) {
        if (error) *error = "目标地址或端口非法: " + ip + ":" + std::to_string(port);
        return false;
    }
    unsigned int dstAddr = 0;
    if (!resolveIpv4(ip, dstAddr)) {
        if (error) *error = "目标地址解析失败: " + ip;
        return false;
    }
    if (!startup()) {
        if (error) *error = "网络库初始化失败（Windows 上为 WSAStartup）";
        return false;
    }
    socket_t s = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (s == BAD_SOCKET) {
        if (error) *error = "socket() 失败";
        cleanup();
        return false;
    }

    // 指定网卡（多网卡/多播场景由配置显式声明；空 = 由系统选路）
    if (!iface.empty()) {
        unsigned int srcAddr = 0;
        if (parseIpv4(iface, srcAddr)) {
            sockaddr_in local{};
            local.sin_family = AF_INET;
            local.sin_port = htons(0);
            local.sin_addr.s_addr = srcAddr;
            (void)::bind(s, reinterpret_cast<sockaddr*>(&local), sizeof(local));
        }
    }

    sockaddr_in dst{};
    dst.sin_family = AF_INET;
    dst.sin_port = htons(static_cast<unsigned short>(port));
    dst.sin_addr.s_addr = dstAddr;

    const int sent = ::sendto(s, payload.data(), static_cast<int>(payload.size()), 0,
                              reinterpret_cast<sockaddr*>(&dst), sizeof(dst));
    CLOSE_SOCKET(s);
    cleanup();
    if (sent != static_cast<int>(payload.size())) {
        if (error) {
#if defined(_WIN32)
            *error = "sendto 失败，WSA 错误码 " + std::to_string(WSAGetLastError());
#else
            *error = std::string("sendto 失败: ") + std::strerror(errno);
#endif
        }
        return false;
    }
    return true;
}

}  // namespace net
}  // namespace device_ingest
