// tools/device_sim · 设备模拟器（ING-NFR-06 / 契约 §10）
//
// 存在意义：**真实外设不在场时，用它复现全部验收场景**。
// 支持多协议包、台数与频率、丢包/乱序/停机注入——需求专篇 §4 的每条验收都能用它跑。
//
// 用法示例（对应契约 §10 场景表）：
//   发一包给 host_demo 看事件：
//     device_sim --port 45500 --kind uav.pos --devices 3 --hz 1 --seconds 5
//   丢包精确统计（期望 packetLossRate ≈ 0.10）：
//     device_sim --port 45500 --devices 1 --hz 50 --drop-rate 10 --seconds 10
//   乱序精确统计：
//     device_sim --port 45500 --devices 1 --hz 50 --shuffle-rate 20 --seconds 10
//   离线判定（3 s 内应收到 device.offline）：
//     device_sim --port 45500 --devices 3 --hz 1 --stop-at 5 --seconds 20
//   新设备类型（原始透传）：
//     device_sim --port 45501 --kind raw --size 200 --devices 2 --hz 1 --seconds 5
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <nlohmann/json.hpp>

#include "device_ingest/version.h"

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

namespace {

double wrapSin(double t, double base, double amp, double period, double phase = 0.0) {
    return base + amp * std::sin((t / period) * 2.0 * 3.14159265358979323846 + phase);
}

struct Options {
    std::string host = "127.0.0.1";
    int         port = 45500;
    std::string kind = "uav.pos";
    int         devices = 1;
    double      hz = 1.0;
    int         seconds = 5;
    int         size = 200;          ///< 报文目标大小（不足时补齐 padding）
    double      dropRate = 0.0;      ///< 0..100，丢包注入（seq 跳过）
    double      shuffleRate = 0.0;   ///< 0..100，乱序注入概率
    double      stopAt = -1.0;       ///< >0：该秒后停发（造离线）
    double      restartAt = -1.0;    ///< >0：该秒后恢复（造上线）
    std::string deviceIdPrefix = "sim";
    bool        verbose = false;
    int         seed = 20260912;
};

/// 一台模拟设备的状态。
struct DeviceState {
    std::string id;
    long long   seq = 0;
    double      t = 0.0;
    double      baseLng = 116.3974;
    double      baseLat = 39.9093;
    bool        stopped = false;
};

nlohmann::json buildUav(DeviceState& d, long long ts) {
    nlohmann::json e;
    e["kind"]    = "uav.pos";
    e["uavId"]   = d.id;
    e["groupId"] = "grp-1";
    e["type"]    = "optical";
    e["lng"]     = std::round((d.baseLng + 0.006 * std::sin(d.t)) * 1e6) / 1e6;
    e["lat"]     = std::round((d.baseLat + 0.005 * std::cos(d.t * 0.8)) * 1e6) / 1e6;
    e["alt"]     = std::round(wrapSin(d.t, 900, 140, 7.0));
    e["heading"] = std::fmod(d.t * 40.0, 360.0);
    e["speed"]   = std::round(wrapSin(d.t, 22, 4, 5.0) * 10) / 10;
    e["battery"] = static_cast<int>(std::max(20.0, 95.0 - std::fmod(d.t * 0.25, 60.0)));
    e["ts"]      = ts;
    e["seq"]     = d.seq;
    return e;
}

nlohmann::json buildLink(DeviceState& d, long long ts) {
    nlohmann::json e;
    e["kind"]          = "link.quality";
    e["linkId"]        = d.id;
    e["from"]          = "node-a";
    e["to"]            = "node-b";
    e["signal"]        = "strong";
    e["bandwidthMbps"] = std::round((82.0 + 3.0 * std::sin(d.t / 9.0)) * 10) / 10;
    e["latencyMs"]     = std::round((38.0 + 4.0 * std::cos(d.t / 7.0)) * 10) / 10;
    e["lossRate"]      = 0.3;
    e["coverageKm2"]   = 126.0;
    e["meshProgress"]  = 78;
    e["state"]         = "green";
    e["ts"]            = ts;
    e["seq"]           = d.seq;
    return e;
}

nlohmann::json buildTarget(DeviceState& d, long long ts) {
    nlohmann::json e;
    e["kind"]     = "target.state";
    e["targetId"] = d.id;
    e["targetNo"] = "T-001";
    e["threat"]   = "high";
    e["status"]   = "red";
    e["lng"]      = d.baseLng + 0.01;
    e["lat"]      = d.baseLat - 0.01;
    e["ts"]       = ts;
    e["seq"]      = d.seq;
    return e;
}

nlohmann::json buildNode(DeviceState& d, long long ts) {
    nlohmann::json e;
    e["kind"]   = "node.state";
    e["nodeId"] = d.id;
    e["name"]   = d.id;
    e["online"] = true;
    e["ts"]     = ts;
    e["seq"]    = d.seq;
    return e;
}

/// 报文补齐到目标大小：`--size 200` 时更接近真实外设的包长。
/// 报文本身就超过目标大小时不做任何事（不截断真实字段）。
void padTo(nlohmann::json& j, int targetSize) {
    const std::string s = j.dump();
    if (static_cast<int>(s.size()) >= targetSize) return;
    // 用一个显式字段补齐（不是垃圾字节）：接收侧解析器会忽略未知字段（契约 §2）
    const std::size_t need = static_cast<std::size_t>(targetSize - s.size());
    // "padding":"" 与逗号约占 13 字节；need 不足时按需缩减，保证不越界
    if (need <= 13) return;
    j["padding"] = std::string(need - 13, 'x');
}

}  // namespace

int main(int argc, char** argv) {
    Options o;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto need = [&](const char* name) -> std::string {
            if (i + 1 >= argc) {
                std::cerr << "参数 " << name << " 缺值" << std::endl;
                std::exit(2);
            }
            return argv[++i];
        };
        if (a == "--host")            o.host = need("--host");
        else if (a == "--port")       o.port = std::atoi(need("--port").c_str());
        else if (a == "--group")      o.host = need("--group");
        else if (a == "--kind")       o.kind = need("--kind");
        else if (a == "--devices")    o.devices = std::atoi(need("--devices").c_str());
        else if (a == "--hz")         o.hz = std::atof(need("--hz").c_str());
        else if (a == "--seconds")    o.seconds = std::atoi(need("--seconds").c_str());
        else if (a == "--size")       o.size = std::atoi(need("--size").c_str());
        else if (a == "--drop-rate")  o.dropRate = std::atof(need("--drop-rate").c_str());
        else if (a == "--shuffle-rate") o.shuffleRate = std::atof(need("--shuffle-rate").c_str());
        else if (a == "--stop-at")    o.stopAt = std::atof(need("--stop-at").c_str());
        else if (a == "--restart-at") o.restartAt = std::atof(need("--restart-at").c_str());
        else if (a == "--id-prefix")  o.deviceIdPrefix = need("--id-prefix");
        else if (a == "--seed")       o.seed = std::atoi(need("--seed").c_str());
        else if (a == "--verbose")    o.verbose = true;
        else if (a == "--help" || a == "-h") {
            std::cout <<
                "device_sim · 设备模拟器（" << device_ingest::version() << "）\n"
                "  --host/--group <addr>  目标地址，默认 127.0.0.1（组播填 239.x.x.x）\n"
                "  --port <n>             目标端口（等于接入点配置的 port）\n"
                "  --kind <kind>          uav.pos | link.quality | target.state | node.state | raw\n"
                "  --devices <n>          设备台数（默认 1）\n"
                "  --hz <f>               每台每秒上报次数（默认 1）\n"
                "  --seconds <n>          运行秒数（默认 5）\n"
                "  --size <n>             报文目标字节数（默认 200；补 padding 字段）\n"
                "  --drop-rate <pct>      丢包注入百分比（seq 跳过，用于验证 ING-HLT-03）\n"
                "  --shuffle-rate <pct>   乱序注入概率百分比（用于验证 ING-HLT-04）\n"
                "  --stop-at <sec>        该秒后停发（造离线，验证 ING-HLT-01）\n"
                "  --restart-at <sec>     该秒后恢复（造上线，验证 ING-HLT-02）\n"
                "  --verbose              打印每条报文\n";
            return 0;
        }
    }
    if (o.hz <= 0.0 || o.devices <= 0 || o.port <= 0) {
        std::cerr << "参数非法：devices/hz/port 必须为正" << std::endl;
        return 2;
    }

#if defined(_WIN32)
    WSADATA wsa{};
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        std::cerr << "WSAStartup 失败" << std::endl;
        return 3;
    }
#endif
    socket_t s = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (s == BAD_SOCKET) {
        std::cerr << "socket() 失败" << std::endl;
        return 3;
    }
    {
        int ttl = 4;   // 组播 TTL：同机/同网段足够
        (void)setsockopt(s, IPPROTO_IP, IP_MULTICAST_TTL,
                         reinterpret_cast<const char*>(&ttl), sizeof(ttl));
    }
    sockaddr_in dst{};
    dst.sin_family = AF_INET;
    dst.sin_port = htons(static_cast<unsigned short>(o.port));
    // 用 inet_pton 而不是 inet_addr：后者已弃用，且无法区分 "0.0.0.0" 与解析失败
    if (inet_pton(AF_INET, o.host.c_str(), &dst.sin_addr) != 1) {
        std::cerr << "目标地址非法: " << o.host << std::endl;
        return 2;
    }

    std::mt19937 rng(static_cast<unsigned>(o.seed));
    std::uniform_real_distribution<double> uni(0.0, 100.0);

    std::vector<DeviceState> devices(static_cast<std::size_t>(o.devices));
    for (int i = 0; i < o.devices; ++i) {
        DeviceState& d = devices[static_cast<std::size_t>(i)];
        d.id = o.deviceIdPrefix + "-" + (o.kind == "link.quality" ? "link-" : "dev-") +
               std::to_string(i + 1);
        const double ang = (i / 6.0) * 2.0 * 3.14159265358979323846;
        d.baseLng += 0.05 * std::cos(ang) + 0.001 * i;
        d.baseLat += 0.04 * std::sin(ang);
        d.t = i * 0.7;
    }

    std::cout << "device_sim · kind=" << o.kind << " devices=" << o.devices
              << " hz=" << o.hz << " -> " << o.host << ":" << o.port
              << " size≈" << o.size
              << (o.dropRate > 0 ? (" drop=" + std::to_string(o.dropRate) + "%") : "")
              << (o.shuffleRate > 0 ? (" shuffle=" + std::to_string(o.shuffleRate) + "%") : "")
              << std::endl;

    const long long totalTicks = static_cast<long long>(o.hz * o.seconds);
    long long sent = 0;
    long long dropped = 0;
    long long shuffled = 0;
    const auto t0 = std::chrono::steady_clock::now();
    char pad[128] = {0};

    for (long long tick = 0; tick < totalTicks; ++tick) {
        const auto target = t0 + std::chrono::microseconds(
            static_cast<long long>(tick * 1e6 / o.hz));
        std::this_thread::sleep_until(target);

        const double elapsedSec =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
        const long long ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::system_clock::now().time_since_epoch()).count();

        for (auto& d : devices) {
            if (o.stopAt > 0 && elapsedSec >= o.stopAt) d.stopped = true;
            if (o.restartAt > 0 && elapsedSec >= o.restartAt) d.stopped = false;
            if (d.stopped) continue;
            if (o.restartAt > 0 && o.stopAt > 0 &&
                elapsedSec >= o.stopAt && elapsedSec >= o.restartAt) {
                // 停机后又恢复：seq 归零，等价于设备重启（契约 §4.2 按新会话处理）
                d.seq = 0;
            }

            // 丢包注入：**seq 照样递增**，只是不发出去——接收侧据此精确算出丢包率
            const long long sendSeq = d.seq;
            ++d.seq;
            ++d.t;
            if (o.dropRate > 0 && uni(rng) < o.dropRate) {
                ++dropped;
                continue;
            }

            std::string payload;
            if (o.kind == "raw") {
                std::size_t n = static_cast<std::size_t>(o.size > 0 ? o.size : 64);
                payload.assign(n, '\0');
                for (std::size_t k = 0; k < n; ++k) {
                    payload[k] = static_cast<char>(rng() & 0xFF);
                }
            } else {
                nlohmann::json e;
                if (o.kind == "uav.pos")            e = buildUav(d, ts);
                else if (o.kind == "link.quality")  e = buildLink(d, ts);
                else if (o.kind == "target.state")  e = buildTarget(d, ts);
                else if (o.kind == "node.state")    e = buildNode(d, ts);
                else {
                    std::cerr << "未知 kind: " << o.kind << std::endl;
                    CLOSE_SOCKET(s);
                    return 2;
                }
                // 乱序注入：把 seq 写小一点（模拟后到包序号更小）
                if (o.shuffleRate > 0 && uni(rng) < o.shuffleRate && sendSeq > 3) {
                    e["seq"] = sendSeq - 3;
                    ++shuffled;
                }
                padTo(e, o.size);
                payload = e.dump();
            }

            const int n = ::sendto(s, payload.data(), static_cast<int>(payload.size()), 0,
                                   reinterpret_cast<sockaddr*>(&dst), sizeof(dst));
            if (n == static_cast<int>(payload.size())) {
                ++sent;
                if (o.verbose) {
                    std::cout << "-> " << payload.substr(0, 120) << std::endl;
                }
            }
        }
    }

    std::cout << "发送完成: " << sent << " 包";
    if (dropped)  std::cout << "，注入丢包 " << dropped << " 包";
    if (shuffled) std::cout << "，注入乱序 " << shuffled << " 包";
    std::cout << std::endl;

    CLOSE_SOCKET(s);
#if defined(_WIN32)
    WSACleanup();
#endif
    return 0;
}
