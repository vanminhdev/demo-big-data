# PowerShell Script: Demo tron goi Producer va Consumer
$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location "$ScriptDir\.."

Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "# BUOC 1: Reset topic ve trang thai sach" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
& "$ScriptDir\reset-topics.ps1"

Write-Host "`n================================================================" -ForegroundColor Cyan
Write-Host "# BUOC 2: Producer gui 200 su kien clickstream (key=session_id)" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
python producer.py --key-strategy session_id

Write-Host "`n================================================================" -ForegroundColor Cyan
Write-Host "# BUOC 3: 1 Consumer doc lai toan bo (tu dau)" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
python consumer.py --group demo-solo --consumer-name SOLO --from-beginning --max-messages 200 --duration 30

Write-Host "`n================================================================" -ForegroundColor Cyan
Write-Host "# BUOC 4: 2 Consumer CUNG Group - Kafka tu chia partition" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "Khoi chay 2 process Consumer C1 va C2 song song (Group: demo-shared)..."

$job1 = Start-Job -ScriptBlock {
    param($dir)
    Set-Location $dir
    python consumer.py --group demo-shared --consumer-name C1 --from-beginning --duration 20
} -ArgumentList (Get-Location).Path

$job2 = Start-Job -ScriptBlock {
    param($dir)
    Set-Location $dir
    python consumer.py --group demo-shared --consumer-name C2 --from-beginning --duration 20
} -ArgumentList (Get-Location).Path

Wait-Job $job1, $job2 | Out-Null
Write-Host "`n[KET QUA CONSUMER C1]:" -ForegroundColor Yellow
Receive-Job $job1
Write-Host "`n[KET QUA CONSUMER C2]:" -ForegroundColor Yellow
Receive-Job $job2
Remove-Job $job1, $job2

Write-Host "`n>>> DEMO HOAN TAT! So sanh phan vung duoc phan cong o tren de thay Kafka tu chia 4 partition cho 2 consumer. <<<" -ForegroundColor Green
