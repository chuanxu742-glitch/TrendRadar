param(
    [string]$KnowledgeBase = '\\guangcai\video\共享文件夹\AI\知识库\ipata_latest_2026-06-11',
    [string]$Output = ''
)

$ErrorActionPreference = 'Stop'
$ProjectRoot = Split-Path -Parent $PSScriptRoot
if (-not $Output) {
    $Output = Join-Path $ProjectRoot 'output\official-monitor\knowledge_sources.json'
}

$Python = (Get-Command python -ErrorAction Stop).Source
$Builder = Join-Path $PSScriptRoot 'build_knowledge_inventory.py'
& $Python $Builder --root $KnowledgeBase --output $Output
if ($LASTEXITCODE -ne 0) {
    throw "Knowledge inventory generation failed with exit code $LASTEXITCODE"
}
