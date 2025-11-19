#requires -Version 5.1
[CmdletBinding()]
Param(
  [switch]$SkipBuild = $false,
  [string]$Python    = 'py',            # or 'python'
  [string]$SpecFile  = 'xtl.spec',
  [string]$ExeName   = 'xtl.exe',
  [string]$OutName   = 'xtl',           # folder name in dist
  [string]$Channel   = 'release'        # for output naming if you want
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

function Write-Info($m){ Write-Host "[*] $m" -ForegroundColor Cyan }
function Sha256($p){ (Get-FileHash -Path $p -Algorithm SHA256).Hash.ToUpperInvariant() }

$Root      = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

$BuildDir  = Join-Path $Root 'build'
$DistDir   = Join-Path $Root 'wizard\dist'
$StageDir  = Join-Path $DistDir $OutName            # final staged folder
$SpecPath  = Join-Path $Root $SpecFile
$ExeBuilt  = Join-Path $Root "dist\$OutName\$ExeName"  # expected PyInstaller output
$ZipPath   = Join-Path $DistDir  "$OutName.zip"
$WinswSrc  = Join-Path (Split-Path $SpecPath -Parent) 'winsw.exe'
$WinswBuilt= Join-Path $Root "dist\$OutName\winsw.exe"

# Clean output
if (Test-Path $DistDir) { Remove-Item -Recurse -Force $DistDir }
New-Item -ItemType Directory -Force -Path $StageDir | Out-Null

# ------------------------------------------------------------------------------------
# 1) Build with PyInstaller (onedir, no UPX)
# ------------------------------------------------------------------------------------
if (-not $SkipBuild) {
  if (Test-Path $SpecPath) {
    # Pre-build assert: winsw.exe must sit next to the spec so the spec can bundle it.
    if (-not (Test-Path $WinswSrc)) {
      throw "winsw.exe not found beside '$SpecFile'. Place winsw.exe next to the spec before building."
    }

    Write-Info "Building with PyInstaller spec: $SpecFile"
    # Ensure 'upx' not used even if present
    $env:PYINSTALLER_NO_UPX = "1"
    & $Python -m PyInstaller --clean --noconfirm $SpecFile
  } else {
    Write-Info "No spec; building with CLI (onedir, no console, no upx)"
    # If your entrypoint is xtl_installer.py:
    # & $Python -m PyInstaller --clean --noconfirm `
    #   --name $OutName `
    #   --onedir --noupx --noconsole `
    #   --manifest ".\res\manifest_asInvoker.xml" `
    #   --version-file ".\res\versioninfo.txt" `
    #   .\xtl_installer.py
  }
}

if (-not (Test-Path $ExeBuilt)) {
  throw "Expected EXE not found: $ExeBuilt"
}

# Post-build assert: winsw.exe must have been bundled into dist\<OutName>\
if (-not (Test-Path $WinswBuilt)) {
  throw "Build succeeded but dist\$OutName\winsw.exe is missing. Check the spec's WinSW bundling entry."
}

# ------------------------------------------------------------------------------------
# 2) Stage deliverables (service-first; no scheduled tasks)
# ------------------------------------------------------------------------------------
Write-Info "Staging deliverables"

# Copy the PyInstaller onedir tree
Copy-Item -Recurse -Force (Split-Path $ExeBuilt -Parent)/* $StageDir

# Optional: include sample cfg or internal assets if you ship any
# Copy-Item -Force .\xtl.cfg.sample $StageDir  -ErrorAction SilentlyContinue
# Copy-Item -Recurse -Force .\_internal $StageDir\_internal -ErrorAction SilentlyContinue

# Generate install/uninstall wrappers (service-first, no tasks)
$installPs1 = @'
# XTL install (service-first)
Param()
$ErrorActionPreference = "Stop"
$dest = Join-Path $PSScriptRoot ".\xtl.exe"
# Install (writes registry, pairs, installs/starts service)
& $dest "install"
'@

$uninstallPs1 = @'
# XTL uninstall (service-first)
Param()
$ErrorActionPreference = "Stop"
# NOTE: your exe should support "repair" or a dedicated "uninstall" if you add it later
Write-Host "No explicit uninstall implemented. Stop service + remove manually if needed."
'@

$installCmd = '@echo off
setlocal
powershell -ExecutionPolicy Bypass -NoProfile -File "%~dp0install.ps1"
'

$uninstallCmd = '@echo off
setlocal
powershell -ExecutionPolicy Bypass -NoProfile -File "%~dp0uninstall.ps1"
'

Set-Content -Path (Join-Path $StageDir 'install.ps1')   -Value $installPs1 -Encoding UTF8
Set-Content -Path (Join-Path $StageDir 'uninstall.ps1') -Value $uninstallPs1 -Encoding UTF8
Set-Content -Path (Join-Path $StageDir 'install.cmd')   -Value $installCmd -Encoding ASCII
Set-Content -Path (Join-Path $StageDir 'uninstall.cmd') -Value $uninstallCmd -Encoding ASCII

# ------------------------------------------------------------------------------------
# 3) Strip MOTW + ZIP + checksums
# ------------------------------------------------------------------------------------
Write-Info "Unblocking and packaging"
Get-ChildItem $StageDir -Recurse | Unblock-File -ErrorAction SilentlyContinue

# ZIP
if (Test-Path $ZipPath) { Remove-Item -Force $ZipPath }
Add-Type -AssemblyName System.IO.Compression.FileSystem
[System.IO.Compression.ZipFile]::CreateFromDirectory($StageDir, $ZipPath)

# Checksums
$ExeSha = Sha256 (Join-Path $StageDir $ExeName)
$ZipSha = Sha256 $ZipPath
Set-Content -Path (Join-Path $StageDir "$ExeName.sha256") -Value "$ExeSha *$ExeName`r`n" -Encoding ASCII
Set-Content -Path (Join-Path $DistDir  "$OutName.zip.sha256") -Value "$ZipSha *$OutName.zip`r`n" -Encoding ASCII

Write-Host ""
Write-Host "==============================================="
Write-Host " Build complete"
Write-Host "  EXE: $ExeName  (SHA256=$($ExeSha.Substring(0,12))...)"
Write-Host "  ZIP: $OutName.zip  (SHA256=$($ZipSha.Substring(0,12))...)"
Write-Host "  OUT: $DistDir"
Write-Host "==============================================="
