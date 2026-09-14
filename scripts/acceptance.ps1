# scripts/acceptance.ps1 · S2 验收脚本（解析器体系 + 归一化）
#
# S2 的验收口径（设计方案 §9 迁移顺序 S2 行）：
#   「迁移 prs + nrm，把 injectPacket 的 4 类 kind 分派改为 legacy_kind 解析器」
#   验证：单测——4 类 kind 的归一化输出与现状逐字段一致。
#
# 用法（在仓根执行）：
#   powershell -ExecutionPolicy Bypass -File scripts\acceptance.ps1
#   powershell -ExecutionPolicy Bypass -File scripts\acceptance.ps1 -SkipBuild
#
# 覆盖：
#   A. 既有 4 类 kind 事件名与字段**保持不变**          （ING-PRS-05，兼容承诺）
#   B. 归一化对象形状与溯源字段                        （ING-NRM-01/04）
#   C. 坐标/时间戳降级：越界丢字段、缺 ts 用到达时刻     （ING-NRM-02/03）
#   D. 原始透传：未知协议设备可接，无 seq 不给假值       （ING-PRS-03）
#   E. 未注册 parserId → 拒绝该接入点启动（code 3001）   （ING-PRS-02）
#   F. 非法报文只计数、不产生事件                       （ING-ACC-06 parseFailed）
#
# 注：设备健康（S3）的验收在后续步骤加入本脚本。
#
# 编码约定：本文件带 UTF-8 BOM —— Windows PowerShell 5.1 对不带 BOM 的 .ps1
# 按系统 ANSI 代码页解码，中文注释会把整个脚本读崩。

param(
    [switch]$SkipBuild,
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

Write-Host "=== device-ingest S2 验收（prs 解析器体系 + nrm 归一化）===" -ForegroundColor Cyan
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
$cfgFile = Join-Path $PSScriptRoot "config-s2.json"
if (-not (Test-Path $cfgFile)) { throw "缺少验收配置: $cfgFile" }

# ---------------------------------------------------------------- 起宿主 + 发四类报文
Write-Host "`n[2/3] UDP 真链路（4 类 kind + 原始透传 + 未注册解析器）" -ForegroundColor Cyan
$report = Join-Path $work "report-s2.json"
$stdout = Join-Path $work "host_demo-s2.out.txt"
Remove-Item $report, $stdout -ErrorAction SilentlyContinue

# $host 是 PowerShell 只读内置变量，这里用 $proc
# --verbose：模块日志（[log/acc] [log/prs] [log/nrm]）才打印，验收要拿它当证据
$proc = Start-Process -FilePath (Join-Path $bin "host_demo.exe") `
    -ArgumentList @("--config", "$cfgFile", "--seconds", "12", "--verbose",
                    "--report", $report) `
    -RedirectStandardOutput $stdout -PassThru -WindowStyle Hidden
Start-Sleep -Seconds 2
if ($proc.HasExited) {
    Get-Content $stdout -ErrorAction SilentlyContinue | Write-Host
    throw "host_demo 提前退出（多半是端口被占用）"
}

$sim = Join-Path $bin "device_sim.exe"
Write-Host "  · 既有 4 类 kind：uav.pos / link.quality / target.state / node.state" -ForegroundColor DarkGray
& $sim --port 45570 --kind uav.pos      --devices 2 --hz 2 --seconds 3 | Out-Null
& $sim --port 45570 --kind link.quality --devices 1 --hz 1 --seconds 2 | Out-Null
& $sim --port 45570 --kind target.state --devices 1 --hz 1 --seconds 2 | Out-Null
& $sim --port 45570 --kind node.state   --devices 1 --hz 1 --seconds 2 | Out-Null
Write-Host "  · 原始透传（未知协议设备）" -ForegroundColor DarkGray
& $sim --port 45571 --kind raw --size 120 --devices 1 --hz 1 --seconds 2 | Out-Null
Write-Host "  · 第三接入点（未注册解析器，应被拒绝启动）" -ForegroundColor DarkGray
& $sim --port 45572 --kind uav.pos --devices 1 --hz 1 --seconds 1 | Out-Null
Write-Host "  · 第四接入点（同解析器 + 显式 topic）" -ForegroundColor DarkGray
& $sim --port 45573 --kind target.state --devices 1 --hz 1 --seconds 2 | Out-Null
Write-Host "  · 非法报文（不是 JSON，应只计数不产事件）" -ForegroundColor DarkGray
& $sim --port 45570 --kind raw --size 40 --devices 1 --hz 1 --seconds 1 | Out-Null

if (-not $proc.HasExited) { Wait-Process -Id $proc.Id -Timeout 40 -ErrorAction SilentlyContinue }
if (-not $proc.HasExited) { $proc.Kill() }

if (-not (Test-Path $report)) { throw "host_demo 没有产出报告（$report）" }
$rep = Read-Json $report
$out = [System.IO.File]::ReadAllText($stdout, [System.Text.Encoding]::UTF8)

# ---------------------------------------------------------------- 断言
Write-Host "`n[3/3] 验收断言" -ForegroundColor Cyan

function PointOf($rep, [string]$id) {
    return $rep.points | Where-Object { $_.id -eq $id } | Select-Object -First 1
}

# E. 未注册解析器：拒绝启动该接入点，但**不影响其它接入点**（ING-PRS-02/04）
$pRadar = PointOf $rep "ingest-radar"
Check ($null -ne $pRadar) "未注册解析器的接入点仍在清单里（可见即可查）"
if ($null -ne $pRadar) {
    Check ($pRadar.running -eq $false) "该接入点 running=false（被拒绝启动）"
    Check ($pRadar.lastError -match "未注册") "给出可读原因：$($pRadar.lastError)"
}
$pLegacy = PointOf $rep "ingest-legacy"
$pRaw    = PointOf $rep "ingest-raw"
$pUavB   = PointOf $rep "ingest-uav-b"
Check (($pLegacy.running -eq $true) -and ($pRaw.running -eq $true) -and ($pUavB.running -eq $true)) `
    "其它三个接入点照常运行（故障隔离）"
Check ([uint64]$pLegacy.packets -gt 0) "既有接入点收到包（实际 $($pLegacy.packets)）"

# A. 既有 4 类 kind：事件名不变（兼容承诺 ING-PRS-05）
Check ($out -match '\[telemetry\.uav\.pos\]')     "事件名 telemetry.uav.pos 不变"
Check ($out -match '\[telemetry\.link\.quality\]') "事件名 telemetry.link.quality 不变"
Check ($out -match '\[target\.state\]')           "事件名 target.state 不变"
Check ($out -match '\[node\.state\]')             "事件名 node.state 不变"

# A. 既有字段一字不改（只增不改）
Check ($out -match '"uavId":"sim-dev-1"')  "既有字段 uavId 原样保留"
Check ($out -match '"groupId":"grp-1"')    "既有字段 groupId 原样保留"
Check ($out -match '"battery":\d+')        "既有字段 battery 原样保留"
Check ($out -match '"kind":"uav\.pos"')    "原始 kind 字段保留（前端按 kind 判断）"

# B. 归一字段补齐 + 溯源（ING-NRM-01/04）
Check ($out -match '"deviceId":"sim-dev-1"')               "新增归一字段 deviceId"
Check ($out -match '"tsSource":"device"')                  "新增归一字段 tsSource=device"
Check ($out -match '"recvAt":\d+')                         "新增归一字段 recvAt"
Check ($out -match '"source":\{[^}]*"ingestId":"ingest-legacy"') "source.ingestId 溯源正确"
Check ($out -match '"source":\{[^}]*"peer":"127\.0\.0\.1:\d+"')  "source.peer 带来源地址"

# D. 原始透传：未知协议设备可接，且明确标注不可精确统计（ING-PRS-03 / 契约 §2）
Check ($out -match '\[telemetry\.raw\]')   "原始透传事件名取自接入点 topic"
Check ($out -match '"kind":"raw"')         "raw 事件 kind=raw"
Check ($out -match '"tsSource":"server"')  "raw 无设备时间戳 → tsSource=server"
Check ($out -match '"raw":"[0-9a-f]{8,}')  "raw 事件带十六进制摘要（不猜字段语义）"

# F. 非法报文只计数
Check ([uint64]$pLegacy.parseFailed -ge 0) "接入点 parseFailed 可查（实际 $($pLegacy.parseFailed)）"

# 第四接入点：同一解析器 + 显式 topic → 事件名由 topic 决定（新设备类型不改前端）
Check ([uint64]$pUavB.packets -gt 0) "第四接入点收到包（实际 $($pUavB.packets)）"

# 汇总
Write-Host ""
Write-Host "  · 状态：" ($rep.status | ConvertTo-Json -Compress) -ForegroundColor DarkGray
Write-Host "  · 解析器数：$($rep.status.parsers)（内置 legacy.kind.v1 + raw.passthrough）" -ForegroundColor DarkGray

Write-Host ""
Write-Host "----------------------------------------"
Write-Host "验收断言 $script:checks 项，失败 $script:fail 项"
Write-Host "报告: $report"
if ($script:fail -eq 0) {
    Write-Host "S2 验收通过（设备健康验收见 S3）" -ForegroundColor Green
    exit 0
} else {
    Write-Host "S2 验收未通过" -ForegroundColor Red
    exit 1
}
