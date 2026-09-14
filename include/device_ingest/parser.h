// device-ingest · 解析器插件接口（唯一的协议扩展点）
//
// 契约依据：《外设接入契约》§7 解析器插件接口；需求 ING-PRS-01/02/04。
//
// 为什么协议没来也能开工：这里的形状只依赖"一个 UDP 包 → 0..N 个事件"这个事实，
// 不依赖任何具体设备的字段名、单位、字节序。设备协议到位后只新增一个实现并注册，
// **不动接入层核心**（README 的立项理由）。
#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "device_ingest/types.h"

namespace device_ingest {

/// 解析器元信息（契约 §4.1 GET /api/v1/ingest/parsers）。
struct ParserInfo {
    std::string parserId;    ///< 稳定标识，建议带版本，如 "uav.json.v1"
    std::string name;        ///< 人可读名称
    std::string version;     ///< 实现版本（协议演进时递增）
    std::string deviceType;  ///< 适配的设备类型
    bool        builtin = false;
};

/// 协议解析器。
///
/// 约束（MUST）：
///   - **无状态、可重入**（ING-PRS-01）：同一份字节重复喂入得到相同结果；
///     实现里不得持有跨包状态（要统计请放 hlt）；
///   - **不抛异常也算失败**：无法解析时返回 false 并清空 out（计入 parseFailed）；
///   - 抛异常被接入层捕获并计入 parseFailed，**不影响接收线程、不影响其它接入点**（ING-PRS-04）。
class IParser {
public:
    virtual ~IParser() = default;

    /// 解析器标识；接入点配置通过它引用本实现。
    virtual std::string id() const = 0;
    /// 适配的设备类型（如 "uav"）。
    virtual std::string deviceType() const = 0;
    /// 人可读名称与实现版本，用于 /parsers 清单。
    virtual std::string name() const { return id(); }
    virtual std::string version() const { return "1"; }

    /// 核心：原始字节 → 0..N 个事件。
    /// @param data  报文首字节（非空终止，长度以 len 为准）
    /// @param len   报文长度
    /// @param peer  来源 "ip:port"（解析器可用它兜底 deviceId）
    /// @param out   输出；返回 true 时追加 0..N 条
    /// @return true = 报文符合本解析器约定；false = 无法解析（调用方计入 parseFailed）
    virtual bool parse(const char* data, std::size_t len,
                       const std::string& peer,
                       std::vector<ParsedEvent>& out) = 0;
};

using ParserPtr = std::shared_ptr<IParser>;

/// 解析器注册表：`parserId` → 实现。
///
/// 每个 Gateway 持有自己的实例（默认注册内置解析器），
/// 因此多个 Gateway 并行时互不污染（对齐 map-2d 的多实例隔离做法）。
class ParserRegistry {
public:
    ParserRegistry() = default;

    /// 可拷贝（含"注册表内容拷贝"语义）：供 ParserRegistry::builtin() 返回副本使用。
    ParserRegistry(const ParserRegistry& other) {
        std::lock_guard<std::mutex> lk(other.mtx_);
        parsers_ = other.parsers_;
    }
    ParserRegistry& operator=(const ParserRegistry& other) {
        if (this == &other) return *this;
        // 先锁自身、再锁对方，避免自赋值以外的死锁（注册表之间不会互相赋值）
        std::lock_guard<std::mutex> lkOther(other.mtx_);
        std::lock_guard<std::mutex> lkSelf(mtx_);
        parsers_ = other.parsers_;
        return *this;
    }

    /// 注册；同 id 覆盖旧实现并返回 false（便于重载测试）。空指针忽略。
    bool add(ParserPtr parser);
    /// 注销，返回是否存在。
    bool remove(const std::string& parserId);
    /// 查找；未注册返回 nullptr。
    ParserPtr find(const std::string& parserId) const;
    /// 是否已注册（接入点启动前检查，对应错误码 3001）。
    bool has(const std::string& parserId) const;
    /// 全部已注册解析器（按 id 排序，输出稳定）。
    std::vector<ParserInfo> list() const;
    /// 已注册数量。
    std::size_t size() const;
    /// 清空（测试用）。
    void clear();

    /// 注册模块内置解析器：legacy_kind（既有 4 类 kind）与 raw.passthrough。
    void addBuiltins();

    /// 进程级默认注册表（含内置解析器），供轻量宿主与工具直接取用。
    static ParserRegistry& builtin();

private:
    mutable std::mutex mtx_;
    std::map<std::string, ParserPtr> parsers_;
};

// ---------------------------------------------------------------- 内置解析器
/// 既有 4 类 kind 分派（uav.pos / link.quality / target.state / node.state）。
///
/// 兼容承诺（ING-PRS-05）：**WS 事件名与既有字段保持不变**——本解析器把原始
/// JSON 的**全部键原样保留**，因此主仓前端无需改动。同时按契约 §2 补齐
/// `deviceId / deviceType / tsSource / recvAt / source` 等归一字段。
ParserPtr makeLegacyKindParser();

/// 原始透传（ING-PRS-03）：把任意字节包成 `kind:"raw"` 事件，
/// 带来源地址、长度与十六进制摘要，用于协议未定的设备。
/// **不猜测任何字段语义**（需求专篇 §1.2 "不做协议反向工程"）。
ParserPtr makeRawPassthroughParser();

}  // namespace device_ingest
