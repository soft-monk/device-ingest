# scripts/acceptance.ps1 · S1–S3 验收脚本（无外设、无外部依赖）
#
# 它把需求专篇 §4「验收准则」与契约 §10「模拟与压测」里 S1–S3 能验的部分，
# 变成一条可重复执行的命令：起 host_demo → 用 device_sim 注入 → 校验 JSON 报告。
#
# 用法（在仓根执行）：
#   powershell -ExecutionPolicy Bypass -File scripts\acceptance.ps1
#   powershell -ExecutionPolicy Bypass -File scripts\acceptance.ps1 -SkipBuild
#   pwsh -File scripts/acceptance.ps1          # PowerShell 7 也可以
#
# 覆盖：
#   A. 三接入点并行接收，互不串台              （ING-ACC-01）
#   B. 原始透传：未知协议设备也能接            （ING-PRS-03）
#   C. 既有 4 类 kind 事件名不变               （ING-PRS-05）
#   D. 丢包率精确统计 ≈ 注入值                 （ING-HLT-03）
#   E. 乱序精确统计（有 seq 时）               （ING-HLT-04）
#   F. 停机 3 s 内判离线 → device.offline      （ING-HLT-01/06）
#   G. 接收侧零丢弃（收包数 = 发送端数量）     （ING-NFR-01 的 S3 子集）
#
# 编码约定：本文件带 UTF-8 BOM。Windows PowerShell 5.1 对不带 BOM 的 .ps1
# 按系统 ANSI 代码页（本机 CP936）解码，中文注释会把整个脚本读崩。
# 若你用编辑器改过本文件，请确认 BOM 仍在。

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

function CheckNear([double]$got, [double]$want, [double]$tol, [string]$what) {
    Check (($got -ge $want - $tol) -and ($got -le $want + $tol)) `
        ("$what（期望≈$want ±$tol，实际=$([math]::Round($got,4))）")
}

# Windows PowerShell 5.1 的 Get-Content -Raw 按 ANSI 解码，会把 UTF-8 JSON 读成乱码；
# 统一用 .NET 显式按 UTF-8 读。
function Read-Json([string]$path) {
    return [System.IO.File]::ReadAllText($path, [System.Text.Encoding]::UTF8) | ConvertFrom-Json
}

Write-Host "=== device-ingest S1–S3 验收 ===" -ForegroundColor Cyan
Write-Host "仓根: $repo"

# ---------------------------------------------------------------- 构建
if (-not $SkipBuild) {
    Write-Host "`n[1/4] 独立构建（cmake -S . -B build）" -ForegroundColor Cyan
    & cmake -S $repo -B (Join-Path $repo "build") -G "Visual Studio 17 2022" -A x64 `
        -DDEVICE_INGEST_BUILD_TESTS=ON | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "cmake 配置失败" }
    & cmake --build (Join-Path $repo "build") --config $Config | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "cmake 构建失败" }
    Check $true "独立构建通过（不依赖主仓任何头文件）"
} else {
    Write-Host "`n[1/4] 跳过构建（-SkipBuild）"
}

foreach ($exe in @("host_demo.exe", "device_sim.exe", "selftest.exe")) {
    if (-not (Test-Path (Join-Path $bin $exe))) { throw "缺少可执行文件: $exe（先构建）" }
}

# ---------------------------------------------------------------- 零依赖自测
Write-Host "`n[2/4] 零依赖自测（tests/selftest）" -ForegroundColor Cyan
& (Join-Path $bin "selftest.exe") | Out-Null
Check ($LASTEXITCODE -eq 0) "selftest 全部断言通过（退出码 $LASTEXITCODE）"

# ---------------------------------------------------------------- UDP 真链路验收
Write-Host "`n[3/4] UDP 真链路验收（host_demo + device_sim 注入）" -ForegroundColor Cyan

$stdout = Join-Path $work "host_demo.out.txt"
$sim    = Join-Path $bin "device_sim.exe"

# $host 是 PowerShell 只读内置变量，这里用 $proc
function Start-Demo([int]$seconds, [string]$reportPath) {
    Remove-Item $reportPath -ErrorAction SilentlyContinue
    Remove-Item $stdout -ErrorAction SilentlyContinue
    $p = Start-Process -FilePath (Join-Path $bin "host_demo.exe") `
        -ArgumentList @("--port", "$BasePort", "--seconds", "$seconds", "--report", $reportPath) `
        -RedirectStandardOutput $stdout -PassThru -WindowStyle Hidden
    Start-Sleep -Seconds 2
    if ($p.HasExited) {
        Get-Content $stdout -ErrorAction SilentlyContinue | Write-Host
        throw "host_demo 提前退出（多半是端口被占用）"
    }
    return $p
}

function Stop-Demo($p) {
    if (-not $p.HasExited) { Wait-Process -Id $p.Id -Timeout 60 -ErrorAction SilentlyContinue }
    if (-not $p.HasExited) { $p.Kill() }
}

# 为什么每个场景单独起一轮 host_demo：
# 健康统计按 60 s 窗口累积（契约 §4.2）。若多个场景挤在同一窗内，
# 后发设备的 seq 会把前一设备的 maxSeq 顶高，丢包率就会被算低——
# 这不是模块缺陷，而是"验收口径必须干净"。每场景一轮，窗口即纯净。
$r1 = Join-Path $work "report-run1.json"
$r2 = Join-Path $work "report-run2.json"
$r3 = Join-Path $work "report-run3.json"

Write-Host "  · 第 1 轮：三接入点并行 / 原始透传 / 既有 4 类 kind" -ForegroundColor DarkGray
$proc = Start-Demo 10 $r1
& $sim --port $BasePort --kind uav.pos --devices 2 --hz 1 --seconds 4 --id-prefix base | Out-Null
& $sim --port $BasePort --kind node.state --devices 1 --hz 1 --seconds 2 --id-prefix node | Out-Null
& $sim --port ($BasePort + 1) --kind raw --size 120 --devices 1 --hz 1 --seconds 3 `
    --id-prefix rawdev | Out-Null
& $sim --port ($BasePort + 2) --kind target.state --devices 1 --hz 1 --seconds 3 `
    --id-prefix tgt | Out-Null
Stop-Demo $proc
Copy-Item $stdout (Join-Path $work "run1.out.txt") -Force

Write-Host "  · 第 2 轮：丢包注入 10%（1 台 × 50 Hz × 8 s）" -ForegroundColor DarkGray
$proc = Start-Demo 12 $r2
& $sim --port $BasePort --kind uav.pos --devices 1 --hz 50 --drop-rate 10 --seconds 8 `
    --id-prefix drop | Out-Null
Stop-Demo $proc

Write-Host "  · 第 3 轮：乱序注入 20%，随后停机造离线" -ForegroundColor DarkGray
$proc = Start-Demo 14 $r3
& $sim --port $BasePort --kind uav.pos --devices 1 --hz 50 --shuffle-rate 20 --seconds 4 `
    --id-prefix ooo | Out-Null
& $sim --port $BasePort --kind uav.pos --devices 1 --hz 1 --stop-at 2 --seconds 6 `
    --id-prefix stop | Out-Null
Stop-Demo $proc

foreach ($f in @($r1, $r2, $r3)) {
    if (-not (Test-Path $f)) { throw "缺少验收报告: $f" }
}
$rep1 = Read-Json $r1
$rep2 = Read-Json $r2
$rep3 = Read-Json $r3

# ---------------------------------------------------------------- 断言
Write-Host "`n[4/4] 验收断言" -ForegroundColor Cyan

function DeviceIn($rep, [string]$id) {
    return $rep.devices | Where-Object { $_.deviceId -eq $id } | Select-Object -First 1
}

# ---- A. 三接入点并行，互不串台
$pts = $rep1.points
Check (($pts | Measure-Object).Count -eq 3) "三个接入点都在列表里"
Check ((($pts | Where-Object { $_.running }).Count) -eq 3) "三个接入点都在运行（running=true）"
$legacyPt = $pts | Where-Object { $_.id -eq "ingest-legacy" }
$rawPt    = $pts | Where-Object { $_.id -eq "ingest-raw" }
$uavbPt   = $pts | Where-Object { $_.id -eq "ingest-uav-b" }
Check (($legacyPt.packets -gt 0) -and ($rawPt.packets -gt 0) -and ($uavbPt.packets -gt 0)) `
    "三路各自收到包（不串台）"

# ---- B. 原始透传：未知协议设备也能接，且不冒充精确值
$rawDev = $rep1.devices | Where-Object { $_.deviceType -eq "unknown" } | Select-Object -First 1
Check ($null -ne $rawDev) "未知协议设备也被接入（原始透传 ING-PRS-03）"
if ($null -ne $rawDev) {
    Check ($rawDev.stats.seqPrecise -eq $false) "无 seq → seqPrecise=false"
    Check ($null -eq $rawDev.stats.packetLossRate) `
        "无 seq 时丢包率返回 null（不给估计值冒充精确值）"
}

# ---- C. 既有事件名与字段不变（从 host_demo 的 stdout 核对）
$out1 = Get-Content (Join-Path $work "run1.out.txt") -Raw -ErrorAction SilentlyContinue
Check ($out1 -match "\[telemetry\.uav\.pos\]")    "事件名 telemetry.uav.pos 出现（ING-PRS-05）"
Check ($out1 -match "\[target\.state\]")          "事件名 target.state 出现"
Check ($out1 -match "\[node\.state\]")            "事件名 node.state 出现"
Check ($out1 -match "\[telemetry\.raw\]")         "原始透传事件名取自接入点 topic"
Check ($out1 -match '"tsSource":"device"')        "tsSource 标注为 device"
Check ($out1 -match '"uavId"')                    "既有字段 uavId 原样保留（只增不改）"

# ---- D. 丢包率精确统计（独立窗口，理应 ≈ 注入值）
$dropDev = DeviceIn $rep2 "drop-dev-1"
Check ($null -ne $dropDev) "丢包设备进入台账（ING-HLT-05）"
if ($null -ne $dropDev) {
    Check ($dropDev.stats.seqPrecise -eq $true) "有 seq → seqPrecise=true"
    $expected = [int]$dropDev.stats.maxSeq - [int]$dropDev.stats.minSeq + 1
    Check ($expected -eq [int]$dropDev.stats.expected) `
        "期望包数口径 = maxSeq - minSeq + 1（$expected）"
    CheckNear ([double]$dropDev.stats.packetLossRate) 0.10 0.03 `
        "丢包率与注入值一致（ING-HLT-03）"
}

# ---- E. 乱序精确统计
$oooDev = DeviceIn $rep3 "ooo-dev-1"
Check ($null -ne $oooDev) "乱序设备进入台账"
if ($null -ne $oooDev) {
    Check ($oooDev.stats.outOfOrderCount -gt 0) `
        "乱序计数 > 0（实际 $($oooDev.stats.outOfOrderCount) 次，ING-HLT-04）"
}

# ---- F. 停机 → 3 s 内判离线
$stopDev = DeviceIn $rep3 "stop-dev-1"
Check ($null -ne $stopDev) "停机设备进入台账"
if ($null -ne $stopDev) {
    Check ($stopDev.online -eq $false) "停机 3 s 后被判离线（ING-HLT-01）"
    Check ([double]$stopDev.offlineSec -ge 3.0) "台账给出失联时长（ING-HLT-05）"
}

# ---- G. 接收侧零丢弃 + 事件确实交到了宿主
Check ([uint64]$rep1.health.dropped -eq 0) `
    "第 1 轮接收侧丢弃数 = 0（实际 $($rep1.health.dropped)）"
Check ([uint64]$rep2.health.dropped -eq 0) `
    "第 2 轮接收侧丢弃数 = 0（实际 $($rep2.health.dropped)）"
Check ([uint64]$rep3.health.dropped -eq 0) `
    "第 3 轮接收侧丢弃数 = 0（实际 $($rep3.health.dropped)）"
Check (($rep1.health.queueDropped + $rep2.health.queueDropped + $rep3.health.queueDropped) -eq 0) `
    "队列无超限淘汰（ING-NFR-05）"
Check (($rep1.sinkEvents -gt 0) -and ($rep2.sinkEvents -gt 0)) `
    "ISink 收到归一事件（第 1 轮 $($rep1.sinkEvents) / 第 2 轮 $($rep2.sinkEvents)）"

# ---- 状态快照（便于人工核对）
Write-Host ""
Write-Host "  · 第 2 轮状态：" ($rep2.status | ConvertTo-Json -Compress) -ForegroundColor DarkGray
if ($null -ne $dropDev) {
    Write-Host "  · 丢包设备统计：" ($dropDev.stats | ConvertTo-Json -Compress) -ForegroundColor DarkGray
}

# ---------------------------------------------------------------- 汇总
Write-Host ""
Write-Host "----------------------------------------"
Write-Host "验收断言 $script:checks 项，失败 $script:fail 项"
Write-Host "报告目录: $work"
if ($script:fail -eq 0) {
    Write-Host "S1–S3 验收通过" -ForegroundColor Green
    exit 0
} else {
    Write-Host "S1–S3 验收未通过" -ForegroundColor Red
    exit 1
}
