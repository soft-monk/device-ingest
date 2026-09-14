# device-ingest · 外设接入层（独立构建）
#
# 设计依据：docs/设计/外设接入层抽仓设计方案.md §8.1
# 目标 G1：空环境 `cmake -S . -B build && cmake --build build` 通过，产出库 + 工具 + 示例。
#
# 三种接入方式（README §接入方式）：
#   ① FetchContent  add_subdirectory(device-ingest)
#   ② git submodule  同上
#   ③ 本地 SOURCE_DIR 同上
# 三种情况下本文件都作为子目录被 add_subdirectory()，因此不用 project() 之外的全局设置。
cmake_minimum_required(VERSION 3.20)
project(device_ingest
  VERSION 0.1.0
  DESCRIPTION "外设接入层：多端口 UDP 接入 → 插件式解析 → 统一归一化 → 设备健康 → 指令下发"
  LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "构建类型" FORCE)
endif()

# ---------------------------------------------------------------- 选项
# hub 层需要 Drogon（WS 广播）。OFF 时模块退化为"纯接入 + 队列 + 回调"，
# 可编译到没有任何 Web 框架的采集机上（设计方案 §8.1）。
option(DEVICE_INGEST_WITH_HUB      "编译 EventHub（需要 Drogon）"                  OFF)
# 默认值按"该目录是否存在"给：S0 阶段还没有 tools/ 与 tests/，
# 它们随 S1（模拟器）与 S2/S3（自测）陆续落地，这里不需要反复改默认值。
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tools")
  set(_ingest_default_tools ON)
else()
  set(_ingest_default_tools OFF)
endif()
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests")
  set(_ingest_default_tests ON)
else()
  set(_ingest_default_tests OFF)
endif()
option(DEVICE_INGEST_BUILD_TOOLS   "编译 tools/（模拟器、回声端）"                  ${_ingest_default_tools})
option(DEVICE_INGEST_BUILD_EXAMPLES "编译 examples/（最小宿主、压测）"              ON)
option(DEVICE_INGEST_BUILD_TESTS   "编译 tests/（零依赖自测）"                      ${_ingest_default_tests})
option(DEVICE_INGEST_WARNINGS_AS_ERRORS "把编译告警当错误"                          OFF)

# ---------------------------------------------------------------- 依赖
# nlohmann/json：必需。优先用系统/包管理器提供的，否则回落内置单头副本。
find_package(nlohmann_json CONFIG QUIET)
if(nlohmann_json_FOUND)
  set(DEVICE_INGEST_JSON_TARGET nlohmann_json::nlohmann_json)
  message(STATUS "[device-ingest] nlohmann/json: 使用已安装的包")
else()
  if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/third_party/nlohmann/json.hpp")
    message(FATAL_ERROR
      "[device-ingest] 未找到 nlohmann/json：既没有安装包，也没有内置副本 "
      "third_party/nlohmann/json.hpp。请安装 nlohmann-json 或用 CMake 选项指向它。")
  endif()
  add_library(device_ingest_nlohmann INTERFACE)
  target_include_directories(device_ingest_nlohmann INTERFACE
    "${CMAKE_CURRENT_SOURCE_DIR}/third_party")
  set(DEVICE_INGEST_JSON_TARGET device_ingest_nlohmann)
  message(STATUS "[device-ingest] nlohmann/json: 使用内置单头副本 third_party/nlohmann/json.hpp")
endif()

if(DEVICE_INGEST_WITH_HUB)
  find_package(Drogon CONFIG REQUIRED)
  message(STATUS "[device-ingest] hub 层：启用（Drogon）")
else()
  message(STATUS "[device-ingest] hub 层：关闭（DEVICE_INGEST_WITH_HUB=OFF，纯采集模式）")
endif()

find_package(Threads REQUIRED)

# ---------------------------------------------------------------- 库
# 用 GLOB + CONFIGURE_DEPENDS：新增一个解析器实现（src/prs/parsers/*.cc）后
# 不需要改本文件——这是"协议到位只加实现、不动架构"在构建侧的体现。
# （scripts/ 下的开发期小工具不参与构建，单独用编译器手工编译。）
file(GLOB_RECURSE DEVICE_INGEST_SOURCES CONFIGURE_DEPENDS
  "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc")

add_library(device_ingest STATIC ${DEVICE_INGEST_SOURCES})
add_library(device_ingest::device_ingest ALIAS device_ingest)

target_include_directories(device_ingest
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:include>"
  PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/src")

target_link_libraries(device_ingest
  PUBLIC  ${DEVICE_INGEST_JSON_TARGET}
  PRIVATE Threads::Threads)

# 对外声明：宿主包含本模块头文件时必须同时带上这几个宏
target_compile_definitions(device_ingest PUBLIC
  DEVICE_INGEST_VERSION="${PROJECT_VERSION}")

if(DEVICE_INGEST_WITH_HUB)
  target_compile_definitions(device_ingest PUBLIC DEVICE_INGEST_HAS_HUB=1)
  target_link_libraries(device_ingest PRIVATE Drogon::Drogon)
endif()

# 模块内部源码自带的头文件目录（src/*/ 之间互相包含用相对路径，避免污染公开面）
#
# ⚠️ 不要在这里加入 src/util：本仓曾有 src/util/time.h，与 CRT 的 <time.h> 同名，
#    一旦 src/util 进入 include 路径，<ctime> 内部的 #include <time.h> 就会命中它，
#    把 <chrono>/<filesystem> 整条链打断（MSVC 报 "chrono 不是类或命名空间名称"）。
#    src/util 下的头文件一律用全路径包含（#include "util/net.h"），因此不需要该目录。
target_include_directories(device_ingest PRIVATE
  "${CMAKE_CURRENT_SOURCE_DIR}/src/acc"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/prs"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/nrm"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/hlt"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/cmd"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/fan"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/hub")

if(MSVC)
  target_compile_options(device_ingest PRIVATE /utf-8 /W4 /permissive-)
  target_compile_definitions(device_ingest PRIVATE
    NOMINMAX WIN32_LEAN_AND_MEAN _WIN32_WINNT=0x0A00
    _CRT_SECURE_NO_WARNINGS)
  if(DEVICE_INGEST_WARNINGS_AS_ERRORS)
    target_compile_options(device_ingest PRIVATE /WX)
  endif()
else()
  target_compile_options(device_ingest PRIVATE -Wall -Wextra)
  if(DEVICE_INGEST_WARNINGS_AS_ERRORS)
    target_compile_options(device_ingest PRIVATE -Werror)
  endif()
endif()

if(WIN32)
  target_link_libraries(device_ingest PRIVATE ws2_32)
endif()

set_target_properties(device_ingest PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
  ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")

# ---------------------------------------------------------------- 工具 / 示例 / 测试
if(DEVICE_INGEST_BUILD_TOOLS)
  add_subdirectory(tools)
endif()
if(DEVICE_INGEST_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()
if(DEVICE_INGEST_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

# ---------------------------------------------------------------- 汇总
message(STATUS "[device-ingest] ${PROJECT_NAME} ${PROJECT_VERSION} · C++${CMAKE_CXX_STANDARD}")
message(STATUS "[device-ingest]   构建类型 : ${CMAKE_BUILD_TYPE}")
message(STATUS "[device-ingest]   hub/tools/examples/tests : "
               "${DEVICE_INGEST_WITH_HUB}/${DEVICE_INGEST_BUILD_TOOLS}/"
               "${DEVICE_INGEST_BUILD_EXAMPLES}/${DEVICE_INGEST_BUILD_TESTS}")
