// prs/registry.h · 注册表内部声明（src/ 内部头）
//
// 只做一件事：把"内置解析器工厂"声明集中在一处，让 registry.cc 不必逐个
// include 解析器实现，也避免在公开头 parser.h 里塞进内部细节。
#pragma once

#include "device_ingest/parser.h"

namespace device_ingest {

/// 既有 4 类 kind 分派（uav.pos / link.quality / target.state / node.state）。
/// 实现见 prs/parsers/legacy_kind.cc。
ParserPtr makeLegacyKindParser();

/// 原始透传。实现见 prs/parsers/raw_passthrough.cc。
ParserPtr makeRawPassthroughParser();

/// 与既有 kind 对应的 WS 事件名（兼容承诺 ING-PRS-05 的唯一真值处）。
/// 未知 kind 返回空串。
std::string legacyEventName(const std::string& kind);

/// 既有 4 类 kind 是否为"需要补齐归一字段的兼容事件"。
bool isLegacyKind(const std::string& kind);

}  // namespace device_ingest
