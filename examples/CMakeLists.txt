# examples/ · 最小宿主与压测（设计方案 §3 目录结构）
#
# host_demo：目标 G2 —— 新仓可独立运行一个最小宿主，不连主仓数据库。

add_executable(host_demo host_demo/main.cc)
target_link_libraries(host_demo PRIVATE device_ingest)
set_target_properties(host_demo PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

if(MSVC)
  target_compile_options(host_demo PRIVATE /utf-8)
  target_compile_definitions(host_demo PRIVATE NOMINMAX WIN32_LEAN_AND_MEAN)
endif()
