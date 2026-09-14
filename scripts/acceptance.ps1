# scripts/acceptance.ps1 · S0 验收脚本（仓骨架）
#
# S0 的验收口径（设计方案 §9 迁移顺序 S0 行）：
#   「建仓骨架：目录 + CMakeLists.txt + 空库 + README，**不接主仓**」
#   验证：新仓独立 cmake 构建通过；examples/host_demo 可运行。
#
# 用法（在仓根执行）：
#   powershell -ExecutionPolicy Bypass -File scripts\acceptance.ps1
#   powershell -ExecutionPolicy Bypass -File scripts\acceptance.ps1 -SkipBuild
#
# 注：数据面（收包→解析→归一→健康→合并）的验收在 S1–S3 逐步加入本脚本。
#
# 编码约定：本文件带 UTF-8 BOM —— Windows PowerShell 5.1 对不带 BOM 的 .ps1
# 按系统 ANSI 代码页解码，中文注释会把整个脚本读崩。

param(
    [switch]$SkipBuild,
    [string]$Config = "Release"
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot

$script:fail = 0
$script:checks = 0

function Check([bool]$ok, [string]$what) {
    $script:checks++
    if ($ok) {
        Write-Host "  [通过] $what" -ForegroundColor Green
    } else {
        $script:fail++
        Write-Host "  [失败] $what" -ForegroundColor Red
    }
}

Write-Host "=== device-ingest S0 验收（仓骨架）===" -ForegroundColor Cyan
Write-Host "仓根: $repo"

# ---------------------------------------------------------------- 独立构建
if (-not $SkipBuild) {
    Write-Host "`n[1/3] 独立构建（cmake -S . -B build，不依赖主仓）" -ForegroundColor Cyan
    & cmake -S $repo -B (Join-Path $repo "build") -G "Visual Studio 17 2022" -A x64 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "cmake 配置失败" }
    & cmake --build (Join-Path $repo "build") --config $Config | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "cmake 构建失败" }
    Check $true "独立 cmake 构建通过"
} else {
    Write-Host "`n[1/3] 跳过构建（-SkipBuild）"
}

$bin = Join-Path $repo "build\bin\$Config"
Check (Test-Path (Join-Path $repo "build\lib\$Config\device_ingest.lib")) "产出静态库 device_ingest.lib"
Check (Test-Path (Join-Path $bin "host_demo.exe")) "产出最小宿主 host_demo.exe"

# ---------------------------------------------------------------- 公开面齐全
Write-Host "`n[2/3] 公开面（include/device_ingest 是宿主唯一入口）" -ForegroundColor Cyan
$headers = @("gateway.h", "config.h", "types.h", "parser.h", "sink.h",
             "device_source.h", "hub.h", "version.h")
foreach ($h in $headers) {
    Check (Test-Path (Join-Path $repo "include\device_ingest\$h")) "公开头 $h 就位"
}

# ---------------------------------------------------------------- 最小宿主可运行
Write-Host "`n[3/3] 最小宿主可运行（目标 G2 的 S0 形态）" -ForegroundColor Cyan
$out = & (Join-Path $bin "host_demo.exe") --seconds 1 2>&1 | Out-String
Check ($LASTEXITCODE -eq 0) "host_demo 正常退出（退出码 $LASTEXITCODE）"
Check ($out -match "device-ingest") "打印了版本横幅"
Check ($out -match "接入点\s*:\s*3\s*个") "读到 3 个接入点配置（配置解析就位）"
Check ($out -match "骨架阶段") "如实报告『数据面未接入』（不假装在跑）"

# ---------------------------------------------------------------- 汇总
Write-Host ""
Write-Host "----------------------------------------"
Write-Host "验收断言 $script:checks 项，失败 $script:fail 项"
if ($script:fail -eq 0) {
    Write-Host "S0 验收通过（数据面验收见 S1–S3）" -ForegroundColor Green
    exit 0
} else {
    Write-Host "S0 验收未通过" -ForegroundColor Red
    exit 1
}
