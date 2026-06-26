param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Args
)

$ErrorActionPreference = "Stop"
$exe = Get-Command databricks -ErrorAction SilentlyContinue
if ($exe) {
    & $exe.Source @Args
    exit $LASTEXITCODE
}

$wingetPath = Join-Path $env:LOCALAPPDATA "Microsoft\WinGet\Packages\Databricks.DatabricksCLI_Microsoft.Winget.Source_8wekyb3d8bbwe\databricks.exe"
if (Test-Path $wingetPath) {
    & $wingetPath @Args
    exit $LASTEXITCODE
}

throw "Databricks CLI not found in PATH or WinGet package path."
