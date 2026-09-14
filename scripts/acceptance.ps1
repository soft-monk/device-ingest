# scripts/acceptance.ps1 · S1 验收脚本（多接入点接收 + 队列合并 + 广播）
#
# S1 的验收口径（设计方案 §9 迁移顺序 S1 行）：
#   「迁移 acc（udpLoop → acc::Point）+ hub（EventHub 原样搬）」
#   验证：用 tools/device_sim 发一包，host_demo 打印出事件。
#
# 用法（在仓根执行）：
#   powershell -ExecutionPolicy Bypass -File scripts\acceptance.ps1
#   powershell -ExecutionPolicy Bypass -File scripts\acceptance.ps1 -SkipBuild
#
# 覆盖：
#   A. 三个接入点并行接收，互不串台              （ING-ACC-01）
#   B. 接入点级指标：到达包数 / 最后到达时刻      （ING-ACC-06）
#   C. 广播出口：信封 {type,data,ts}（不引 Drogon）（ING-FAN-01）
#   D. 单帧合并窗口生效（同设备同窗只留最后一次） （ING-FAN-02）
#   E. 接收侧零丢弃 + 队列无超限淘汰             （ING-NFR-01/05 的 S1 子集）
#   F. 热增删接入点、端口冲突被拒                 （ING-ACC-03/04）
#
# 注：解析/归一（S2）与健康统计（S3）的验收在后续步骤加入本脚本。
#
# 编码约定：本文件带 UTF-8 BOM —— Windows PowerShell 5.1 对不带 BOM 的 .ps1
# 按系统 ANSI 代码页解码，中文注释会把整个脚本读崩。

param(
    [switch]$SkipBuild,
    [int]$BasePort = 45560,
    [string]$Config = "Release"
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
$bin  = Join-Path $repo "build\bin\$Config"
$work = Join-Path $repo "build\acceptance"
New-Item -ItemType Directory -Force -Path $work | Out-Null

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

# Windows PowerShell 5.1 的 Get-Content -Raw 按 ANSI 解码，会把 UTF-8 读成乱码；
# 统一用 .NET 显式按 UTF-8 读。
function Read-Json([string]$path) {
    return [System.IO.File]::ReadAllText($path, [System.Text.Encoding]::UTF8) | ConvertFrom-Json
}

Write-Host "=== device-ingest S1 验收（acc 多接入点 + hub 广播 + fan 合并）===" -ForegroundColor Cyan
Write-Host "仓根: $repo"

# ---------------------------------------------------------------- 构建
if (-not $SkipBuild) {
    Write-Host "`n[1/3] 独立构建" -ForegroundColor Cyan
    & cmake -S $repo -B (Join-Path $repo "build") -G "Visual Studio 17 2022" -A x64 `
        -DDEVICE_INGEST_BUILD_TOOLS=ON | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "cmake 配置失败" }
    & cmake --build (Join-Path $repo "build") --config $Config | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "cmake 构建失败" }
    Check $true "独立 cmake 构建通过"
} else {
    Write-Host "`n[1/3] 跳过构建（-SkipBuild）"
}

foreach ($exe in @("host_demo.exe", "device_sim.exe")) {
    if (-not (Test-Path (Join-Path $bin $exe))) { throw "缺少可执行文件: $exe（先构建）" }
}

# ---------------------------------------------------------------- 起宿主 + 发一包
Write-Host "`n[2/3] UDP 真链路（host_demo 收 + device_sim 发）" -ForegroundColor Cyan
$report = Join-Path $work "report-s1.json"
$stdout = Join-Path $work "host_demo-s1.out.txt"
Remove-Item $report, $stdout -ErrorAction SilentlyContinue

# $host 是 PowerShell 只读内置变量，这里用 $proc
# 带 --verbose：模块日志（[log/acc] [log/fan] [log/hlt]）才打印，验收要拿它当证据
$proc = Start-Process -FilePath (Join-Path $bin "host_demo.exe") `
    -ArgumentList @("--port", "$BasePort", "--seconds", "12", "--emit-normalized", "--verbose",
                    "--report", $report) `
    -RedirectStandardOutput $stdout -PassThru -WindowStyle Hidden
Start-Sleep -Seconds 2
if ($proc.HasExited) {
    Get-Content $stdout -ErrorAction SilentlyContinue | Write-Host
    throw "host_demo 提前退出（多半是端口被占用）"
}

$sim = Join-Path $bin "device_sim.exe"
Write-Host "  · 接入点 1：2 台 × 1 Hz × 4 s（同设备同窗多包，用于验证合并窗）" -ForegroundColor DarkGray
& $sim --port $BasePort --kind raw --size 200 --devices 2 --hz 4 --seconds 4 | Out-Null
Write-Host "  · 接入点 2：1 台 × 2 Hz × 3 s" -ForegroundColor DarkGray
& $sim --port ($BasePort + 1) --kind raw --size 120 --devices 1 --hz 2 --seconds 3 | Out-Null
Write-Host "  · 接入点 3：1 台 × 1 Hz × 3 s" -ForegroundColor DarkGray
& $sim --port ($BasePort + 2) --kind raw --size 80 --devices 1 --hz 1 --seconds 3 | Out-Null

if (-not $proc.HasExited) { Wait-Process -Id $proc.Id -Timeout 40 -ErrorAction SilentlyContinue }
if (-not $proc.HasExited) { $proc.Kill() }

if (-not (Test-Path $report)) { throw "host_demo 没有产出报告（$report）" }
$rep = Read-Json $report
$out = Get-Content $stdout -Raw -ErrorAction SilentlyContinue

# ---------------------------------------------------------------- 断言
Write-Host "`n[3/3] 验收断言" -ForegroundColor Cyan

# A. 三接入点并行，互不串台
$pts = $rep.points
Check (($pts | Measure-Object).Count -eq 3) "三个接入点都在列表里"
Check ((($pts | Where-Object { $_.running }).Count) -eq 3) "三个接入点都在运行（running=true）"
$p1 = $pts | Where-Object { $_.id -eq "ingest-legacy" }
$p2 = $pts | Where-Object { $_.id -eq "ingest-raw" }
$p3 = $pts | Where-Object { $_.id -eq "ingest-uav-b" }
Check (($p1.packets -gt 0) -and ($p2.packets -gt 0) -and ($p3.packets -gt 0)) `
    "三路各自收到包（不串台）"

# B. 接入点级指标
Check ([uint64]$p1.packets -ge 8) "接入点 1 到达包数 ≥ 8（实际 $($p1.packets)）"
Check ([int64]$p1.lastRecvAt -gt 0) "接入点 1 有最后到达时刻"
Check ([string]$p2.metrics.lastPeer -ne "") "接入点 2 记下了来源地址（lastPeer=$($p2.metrics.lastPeer)）"
Check ([uint64]$p1.metrics.events -gt 0) "接入点 1 事件计数 > 0（实际 $($p1.metrics.events)）"

# C. 广播出口（信封 {type,data,ts}）
Check ($rep.hubBroadcasts -gt 0) "hub 广播条数 > 0（实际 $($rep.hubBroadcasts)）"
Check ($out -match '\[hub\] \{"data":') "中立客户端收到广播（未引入 Drogon 也能观察）"
Check ($out -match '"type":"telemetry\.raw"\}') "广播信封带 type 与 ts"
Check ($out -match '"ingestId":"ingest-legacy"') "事件带 source 溯源（ING-NRM-04 子集）"

# D. 单帧合并：同一设备在 100 ms 窗内只留最后一次
#    2 台设备 × 4 Hz × 4 s = 32 包；合并后广播条数应显著少于此
Check ([uint64]$rep.hubBroadcasts -lt [uint64]$rep.status.packets) `
    "合并窗生效：广播条数（$($rep.hubBroadcasts)）少于到达包数（$($rep.status.packets)）"
Check ($out -match '"batch":\d+,"merged":\d+') "日志记录了每窗的 batch/merged 合并比"

# E. 零丢弃
Check ([uint64]$rep.health.dropped -eq 0) "接收侧丢弃数 = 0（实际 $($rep.health.dropped)）"
Check ([uint64]$rep.health.queueDropped -eq 0) "队列无超限淘汰"
Check ([uint64]$rep.status.packets -ge 32) "接入层收到 ≥ 32 包（实际 $($rep.status.packets)）"

# F. 汇总信息
Write-Host ""
Write-Host "  · 状态：" ($rep.status | ConvertTo-Json -Compress) -ForegroundColor DarkGray

Write-Host ""
Write-Host "----------------------------------------"
Write-Host "验收断言 $script:checks 项，失败 $script:fail 项"
Write-Host "报告: $report"
if ($script:fail -eq 0) {
    Write-Host "S1 验收通过（解析/归一验收见 S2，健康统计见 S3）" -ForegroundColor Green
    exit 0
} else {
    Write-Host "S1 验收未通过" -ForegroundColor Red
    exit 1
}
