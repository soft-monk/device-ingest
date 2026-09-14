// registry.cc · 解析器注册表实现
//
// ING-PRS-02（MUST）：解析器按 parserId 注册并被接入点引用；支持编译期静态注册；
// 未注册的 parserId **拒绝该接入点启动**并给出可读原因（错误码 3001）。
#include "device_ingest/parser.h"

#include <algorithm>

#include "prs/registry.h"
namespace device_ingest {

// 构造函数在头文件里 default + 声明（含拷贝语义），这里不再重复定义。

bool ParserRegistry::add(ParserPtr parser) {
    if (!parser) return false;
    const std::string id = parser->id();
    if (id.empty()) return false;
    std::lock_guard<std::mutex> lk(mtx_);
    auto it = parsers_.find(id);
    if (it != parsers_.end()) {
        it->second = std::move(parser);   // 同 id 覆盖（协议演进/测试重载）
        return false;
    }
    parsers_.emplace(id, std::move(parser));
    return true;
}

bool ParserRegistry::remove(const std::string& parserId) {
    std::lock_guard<std::mutex> lk(mtx_);
    return parsers_.erase(parserId) > 0;
}

ParserPtr ParserRegistry::find(const std::string& parserId) const {
    std::lock_guard<std::mutex> lk(mtx_);
    auto it = parsers_.find(parserId);
    if (it == parsers_.end()) return nullptr;
    return it->second;
}

bool ParserRegistry::has(const std::string& parserId) const {
    std::lock_guard<std::mutex> lk(mtx_);
    return parsers_.find(parserId) != parsers_.end();
}

std::vector<ParserInfo> ParserRegistry::list() const {
    std::lock_guard<std::mutex> lk(mtx_);
    std::vector<ParserInfo> out;
    out.reserve(parsers_.size());
    for (const auto& kv : parsers_) {
        ParserInfo info;
        info.parserId   = kv.first;
        info.name       = kv.second ? kv.second->name() : kv.first;
        info.version    = kv.second ? kv.second->version() : std::string();
        info.deviceType = kv.second ? kv.second->deviceType() : std::string();
        info.builtin    = kv.first == "legacy.kind.v1" || kv.first == "raw.passthrough";
        out.push_back(std::move(info));
    }
    std::sort(out.begin(), out.end(),
              [](const ParserInfo& a, const ParserInfo& b) { return a.parserId < b.parserId; });
    return out;
}

std::size_t ParserRegistry::size() const {
    std::lock_guard<std::mutex> lk(mtx_);
    return parsers_.size();
}

void ParserRegistry::clear() {
    std::lock_guard<std::mutex> lk(mtx_);
    parsers_.clear();
}

void ParserRegistry::addBuiltins() {
    add(makeLegacyKindParser());
    add(makeRawPassthroughParser());
}

ParserRegistry& ParserRegistry::builtin() {
    static ParserRegistry reg = [] {
        ParserRegistry r;          // 局部实例：注册内置解析器后再拷贝出来
        r.addBuiltins();
        return r;
    }();
    return reg;
}

}  // namespace device_ingest
