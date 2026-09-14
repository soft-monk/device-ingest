# tools/ · 设备模拟器与指令回声端（设计方案 §3 / 契约 §10）
#
# device_sim：无外设环境下复现全部验收场景（ING-NFR-06）——
#             按端口/组播发多协议包，可配台数、频率、丢包、乱序、停机注入。

add_executable(device_sim device_sim/main.cc)
target_link_libraries(device_sim PRIVATE device_ingest)
set_target_properties(device_sim PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

if(MSVC)
  target_compile_options(device_sim PRIVATE /utf-8)
  target_compile_definitions(device_sim PRIVATE NOMINMAX WIN32_LEAN_AND_MEAN)
endif()
