# tests/ · 零依赖自测（一个可执行 + 手写断言，对齐 map-2d scripts/unit-test.mjs 的做法）
#
# 覆盖 S2（解析/归一）与 S3（健康/队列/指标/热插拔）的验收条目，
# **不需要外设、不需要网络**：全部通过 Gateway::inject 与 addPoint 在进程内驱动。

add_executable(selftest selftest.cc)
target_link_libraries(selftest PRIVATE device_ingest)

# 少量"纯逻辑"用例直接驱动内部类（HealthTracker / fan::merge），
# 因此需要模块内部头文件目录。这不是公开面，宿主不必也不能这么用。
target_include_directories(selftest PRIVATE
  "${CMAKE_SOURCE_DIR}/src"
  "${CMAKE_SOURCE_DIR}/src/hlt"
  "${CMAKE_SOURCE_DIR}/src/fan"
  "${CMAKE_SOURCE_DIR}/src/nrm"
  "${CMAKE_SOURCE_DIR}/src/prs"
  "${CMAKE_SOURCE_DIR}/src/util")

set_target_properties(selftest PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

if(MSVC)
  target_compile_options(selftest PRIVATE /utf-8)
  target_compile_definitions(selftest PRIVATE NOMINMAX WIN32_LEAN_AND_MEAN)
endif()

add_test(NAME device_ingest_selftest COMMAND selftest)
