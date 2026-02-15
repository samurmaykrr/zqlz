<#
.SYNOPSIS
    ZQLZ Windows Bundle Script
    Creates a Windows installer using Inno Setup

.DESCRIPTION
    Builds ZQLZ for Windows and creates an installer executable.

.PARAMETER Architecture
    Target architecture: x86_64 (default) or aarch64

.PARAMETER Channel
    Release channel: stable, nightly, or dev (default)

.EXAMPLE
    .\script\bundle-windows.ps1 -Architecture x86_64
#>

param(
    [ValidateSet("x86_64", "aarch64")]
    [string]$Architecture = "x86_64",
    
    [ValidateSet("stable", "nightly", "dev")]
    [string]$Channel = "dev"
)

$ErrorActionPreference = "Stop"

# Configuration
$AppName = "ZQLZ"
$Version = if ($env:ZQLZ_VERSION) { $env:ZQLZ_VERSION } else { "0.1.0" }

switch ($Channel) {
    "stable" {
        $AppDisplayName = "ZQLZ"
        $AppIdentifier = "dev.zqlz.ZQLZ"
        $AppSuffix = ""
        $IconSuffix = ""
    }
    "nightly" {
        $AppDisplayName = "ZQLZ Nightly"
        $AppIdentifier = "dev.zqlz.ZQLZ-Nightly"
        $AppSuffix = "-nightly"
        $IconSuffix = "-nightly"
    }
    default {
        $AppDisplayName = "ZQLZ Dev"
        $AppIdentifier = "dev.zqlz.ZQLZ-Dev"
        $AppSuffix = "-dev"
        $IconSuffix = "-dev"
    }
}

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
$ResourcesDir = Join-Path $ProjectRoot "crates\zqlz-app\resources"
$WindowsResourcesDir = Join-Path $ResourcesDir "windows"

Set-Location $ProjectRoot

$TargetTriple = "$Architecture-pc-windows-msvc"

Write-Host "==> Building ZQLZ for $TargetTriple (channel: $Channel)" -ForegroundColor Cyan

# Determine build flags
if ($Channel -eq "dev") {
    $BuildFlags = @()
    $Profile = "debug"
} else {
    $BuildFlags = @("--release")
    $Profile = "release"
}

# Build the application
Write-Host "==> Building application..." -ForegroundColor Cyan
$BuildArgs = @("build") + $BuildFlags + @("--package", "zqlz-app", "--target", $TargetTriple)
& cargo @BuildArgs
if ($LASTEXITCODE -ne 0) {
    Write-Error "Build failed"
    exit 1
}

# Create staging directory for installer
$StagingDir = Join-Path "target" $TargetTriple $Profile "installer-staging"
$BinDir = Join-Path $StagingDir "bin"

Write-Host "==> Creating staging directory at $StagingDir" -ForegroundColor Cyan
if (Test-Path $StagingDir) {
    Remove-Item -Recurse -Force $StagingDir
}
New-Item -ItemType Directory -Force -Path $StagingDir | Out-Null
New-Item -ItemType Directory -Force -Path $BinDir | Out-Null

# Copy binary
$SourceBinary = Join-Path "target" $TargetTriple $Profile "zqlz.exe"
$DestBinary = Join-Path $StagingDir "ZQLZ.exe"
Copy-Item $SourceBinary $DestBinary

# Copy to bin directory for CLI access
Copy-Item $SourceBinary (Join-Path $BinDir "zqlz.exe")

# Determine icon name
$IconName = "app-icon$IconSuffix"
$IconPath = Join-Path $WindowsResourcesDir "$IconName.ico"
if (-not (Test-Path $IconPath)) {
    $IconPath = Join-Path $WindowsResourcesDir "app-icon.ico"
    $IconName = "app-icon"
}

# Copy resources needed by installer
Copy-Item $IconPath (Join-Path $StagingDir "$IconName.ico")

# Check if Inno Setup is available
$ISCC = $null
$InnoSetupPaths = @(
    "${env:ProgramFiles(x86)}\Inno Setup 6\ISCC.exe",
    "${env:ProgramFiles}\Inno Setup 6\ISCC.exe",
    "C:\Program Files (x86)\Inno Setup 6\ISCC.exe",
    "C:\Program Files\Inno Setup 6\ISCC.exe"
)

foreach ($path in $InnoSetupPaths) {
    if (Test-Path $path) {
        $ISCC = $path
        break
    }
}

if (-not $ISCC) {
    Write-Warning "Inno Setup not found. Creating portable ZIP instead."
    
    # Create portable ZIP
    $ZipName = "ZQLZ-$Architecture.zip"
    $ZipPath = Join-Path "target" $TargetTriple $Profile $ZipName
    
    Write-Host "==> Creating portable ZIP at $ZipPath" -ForegroundColor Cyan
    Compress-Archive -Path $StagingDir\* -DestinationPath $ZipPath -Force
    
    Write-Host ""
    Write-Host "==> Build complete!" -ForegroundColor Green
    Write-Host "    Portable ZIP: $ZipPath"
    Write-Host ""
    exit 0
}

Write-Host "==> Creating installer with Inno Setup..." -ForegroundColor Cyan

# Inno Setup parameters
$IssFile = Join-Path $WindowsResourcesDir "zqlz.iss"
$OutputDir = Join-Path "target" $TargetTriple $Profile
$SetupName = "ZQLZ-$Architecture"

# Create Inno Setup defines
$Defines = @(
    "/DAppId=$AppIdentifier",
    "/DAppName=$AppName$AppSuffix",
    "/DAppDisplayName=$AppDisplayName",
    "/DAppExeName=ZQLZ.exe",
    "/DAppMutex=$AppIdentifier",
    "/DAppUserId=$AppIdentifier",
    "/DRegValueName=ZQLZ$AppSuffix",
    "/DVersion=$Version",
    "/DOutputDir=$OutputDir",
    "/DAppSetupName=$SetupName",
    "/DSourceDir=$ProjectRoot",
    "/DResourcesDir=$StagingDir",
    "/DAppIconName=$IconName"
)

# Run Inno Setup
$ISCCArgs = $Defines + @($IssFile)
& $ISCC @ISCCArgs
if ($LASTEXITCODE -ne 0) {
    Write-Error "Inno Setup failed"
    exit 1
}

$InstallerPath = Join-Path $OutputDir "$SetupName.exe"

# Sign the installer if credentials are available
if ($env:AZURE_TENANT_ID -and $env:AZURE_CLIENT_ID -and $env:AZURE_CLIENT_SECRET) {
    $SignScript = Join-Path $WindowsResourcesDir "sign.ps1"
    if (Test-Path $SignScript) {
        Write-Host "==> Signing installer..." -ForegroundColor Cyan
        & $SignScript -FilePath $InstallerPath
    }
}

Write-Host ""
Write-Host "==> Build complete!" -ForegroundColor Green
Write-Host "    Installer: $InstallerPath"
Write-Host ""
