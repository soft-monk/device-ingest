// util/net.h · 网络小工具（地址解析、UDP 收发）
//
// 设计方案 §11：现状 Telemetry.cc 的 udpLoop 直接内嵌 winsock/posix 分支，
// 抽仓后把平台差异**收进本文件**，acc 层只认这里的几个函数。
//
// 平台：Windows（winsock2）与非 Windows（BSD socket）。C++17，无第三方网络库。
#pragma once

#include <cstdint>
#include <string>

namespace device_ingest {
namespace net {

/// 进程级 WSA 初始化（Windows 需要；非 Windows 为空操作）。
/// 可重复调用，内部引用计数。返回 false = 初始化失败。
bool startup();
void cleanup();

/// "ip:port" 或 "ip"（用 defaultPort 补全）→ 已解析的可读形式 "ip:port"。
/// 解析失败返回原串。
std::string normalizePeer(const std::string& peer, int defaultPort = 0);

/// 拆分 "ip:port"；无端口时 port 保持不变。
bool splitPeer(const std::string& peer, std::string& ip, int& port);

/// 发送一个 UDP 数据报（单播直接给 ip:port；组播由调用方给组播地址+端口）。
/// 返回 true = 已发出至少 1 字节。error 给可读原因。
bool sendUdp(const std::string& ip, int port, const std::string& payload,
             const std::string& iface, std::string* error);

/// 解析并校验点分十进制 IPv4；合法且为组播地址（224.0.0.0/4）返回 true。
bool isMulticast(const std::string& ip);

/// 点分十进制 IPv4 → 网络字节序的 32 位地址。
///
/// 用它而不是 `inet_addr()`：后者在 Windows 上已弃用（告警 C4996），而且
/// 无法区分 "0.0.0.0" 与解析失败（两者都返回 INADDR_NONE）。
/// @param out 网络字节序地址（可直接赋给 sockaddr_in::sin_addr.s_addr）
/// @return false = 不是合法 IPv4 字面量
bool parseIpv4(const std::string& ip, unsigned int& out);

/// 组播/发送用：把主机名或点分十进制解析成网络字节序 IPv4（失败返回 false）。
bool resolveIpv4(const std::string& host, unsigned int& out);

}  // namespace net
}  // namespace device_ingest
