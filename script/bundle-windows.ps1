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
    [string]$Channel = "dev",

    [switch]$Install
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

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

function Get-ArchitectureSettings {
    param([string]$Arch)

    switch ($Arch) {
        "x86_64" {
            return @{
                TargetTriple = "x86_64-pc-windows-msvc"
                ArchitecturesAllowed = "x64compatible"
                ArchitecturesInstallIn64BitMode = "x64compatible"
            }
        }
        "aarch64" {
            return @{
                TargetTriple = "aarch64-pc-windows-msvc"
                ArchitecturesAllowed = "arm64"
                ArchitecturesInstallIn64BitMode = "arm64"
            }
        }
        default {
            throw "Unsupported architecture '$Arch'"
        }
    }
}

function Enter-VsDevShell {
    param([string]$Arch)

    $vsDevShellPath = "C:\Program Files\Microsoft Visual Studio\2022\Community\Common7\Tools\Launch-VsDevShell.ps1"
    if (-not (Test-Path $vsDevShellPath)) {
        return
    }

    $vsArch = if ($Arch -eq "aarch64") { "arm64" } else { "amd64" }
    Push-Location
    & $vsDevShellPath -Arch $vsArch -HostArch $vsArch | Out-Null
    Pop-Location
}

function Test-CiSigningEnvironment {
    if (-not $env:CI) {
        return
    }

    $requiresSigning = $env:ZQLZ_REQUIRE_SIGNING -eq "1" -or (Get-Item "env:AZURE_SIGNING_ENDPOINT" -ErrorAction SilentlyContinue)
    if (-not $requiresSigning) {
        return
    }

    $requiredVars = @(
        "AZURE_TENANT_ID",
        "AZURE_CLIENT_ID",
        "AZURE_CLIENT_SECRET",
        "AZURE_SIGNING_ENDPOINT",
        "AZURE_SIGNING_ACCOUNT",
        "AZURE_SIGNING_CERT_PROFILE"
    )

    $missingVars = @()
    foreach ($var in $requiredVars) {
        if (-not (Get-Item "env:$var" -ErrorAction SilentlyContinue)) {
            $missingVars += $var
        }
    }

    if ($missingVars.Count -gt 0) {
        throw "Missing required signing environment variables in CI: $($missingVars -join ', ')"
    }
}

$ArchitectureSettings = Get-ArchitectureSettings -Arch $Architecture
$TargetTriple = $ArchitectureSettings.TargetTriple
Enter-VsDevShell -Arch $Architecture
Test-CiSigningEnvironment
rustup target add $TargetTriple

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
$StagingDir = [System.IO.Path]::Combine("target", $TargetTriple, $Profile, "installer-staging")
$BinDir = Join-Path $StagingDir "bin"

Write-Host "==> Creating staging directory at $StagingDir" -ForegroundColor Cyan
if (Test-Path $StagingDir) {
    Remove-Item -Recurse -Force $StagingDir
}
New-Item -ItemType Directory -Force -Path $StagingDir | Out-Null
New-Item -ItemType Directory -Force -Path $BinDir | Out-Null

# Copy binary
$SourceBinary = [System.IO.Path]::Combine("target", $TargetTriple, $Profile, "zqlz.exe")
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
$MessagesSourceDir = Join-Path $WindowsResourcesDir "messages"
$MessagesDestDir = Join-Path $StagingDir "messages"
if (Test-Path $MessagesSourceDir) {
    Copy-Item $MessagesSourceDir $MessagesDestDir -Recurse -Force
}

# Copy installer language/messages files expected by zqlz.iss
$MessagesSourceDir = Join-Path $WindowsResourcesDir "messages"
if (Test-Path $MessagesSourceDir) {
    $MessagesDestDir = Join-Path $StagingDir "messages"
    New-Item -ItemType Directory -Force -Path $MessagesDestDir | Out-Null
    Copy-Item (Join-Path $MessagesSourceDir "*") -Destination $MessagesDestDir -Recurse -Force
}

# Check if Inno Setup is available
$ISCC = $null
$IsccCommand = Get-Command "iscc.exe" -ErrorAction SilentlyContinue
if (-not $IsccCommand) {
    $IsccCommand = Get-Command "iscc" -ErrorAction SilentlyContinue
}

if ($IsccCommand) {
    $ISCC = $IsccCommand.Source
}

$InnoSetupPaths = @(
    "${env:ProgramFiles(x86)}\Inno Setup 6\ISCC.exe",
    "${env:ProgramFiles}\Inno Setup 6\ISCC.exe",
    "$env:LOCALAPPDATA\Programs\Inno Setup 6\ISCC.exe",
    "C:\Program Files (x86)\Inno Setup 6\ISCC.exe",
    "C:\Program Files\Inno Setup 6\ISCC.exe"
)

if (-not $ISCC) {
    foreach ($path in $InnoSetupPaths) {
        if (Test-Path $path) {
            $ISCC = $path
            break
        }
    }
}

if (-not $ISCC) {
    if ($env:CI) {
        Write-Error "Inno Setup not found in CI environment"
        exit 1
    }

    Write-Warning "Inno Setup not found. Creating portable ZIP instead."
    
    # Create portable ZIP
    $ZipName = "ZQLZ-$Architecture.zip"
    $ZipPath = [System.IO.Path]::Combine("target", $TargetTriple, $Profile, $ZipName)
    
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
$OutputDir = [System.IO.Path]::Combine("target", $TargetTriple, $Profile)
$SetupName = "ZQLZ-$Architecture"
$InstallerArchitecture = if ($Architecture -eq "aarch64") { "arm64" } else { "x64compatible" }

# Create Inno Setup defines
$Definitions = @{
    "AppId" = $AppIdentifier
    "AppName" = "$AppName$AppSuffix"
    "AppDisplayName" = $AppDisplayName
    "AppExeName" = "ZQLZ"
    "AppMutex" = $AppIdentifier
    "AppUserId" = $AppIdentifier
    "RegValueName" = "ZQLZ$AppSuffix"
    "Version" = $Version
    "OutputDir" = $OutputDir
    "AppSetupName" = $SetupName
    "SourceDir" = $ProjectRoot
    "ResourcesDir" = $StagingDir
    "AppIconName" = $IconName
    "ArchitecturesAllowed" = $ArchitectureSettings.ArchitecturesAllowed
    "ArchitecturesInstallIn64BitMode" = $ArchitectureSettings.ArchitecturesInstallIn64BitMode
}

$Defines = @()
foreach ($key in $Definitions.Keys) {
    $value = $Definitions[$key]
    $Defines += "/D$key=$value"
}

# Run Inno Setup
$ISCCArgs = $Defines + @($IssFile)
if (
    $env:CI -and
    (Test-Path (Join-Path $WindowsResourcesDir "sign.ps1")) -and
    $env:AZURE_TENANT_ID -and
    $env:AZURE_CLIENT_ID -and
    $env:AZURE_CLIENT_SECRET -and
    $env:AZURE_SIGNING_ENDPOINT -and
    $env:AZURE_SIGNING_ACCOUNT -and
    $env:AZURE_SIGNING_CERT_PROFILE
) {
    $signTool = "powershell.exe -ExecutionPolicy Bypass -File $WindowsResourcesDir\sign.ps1 -FilePath `$f"
    $ISCCArgs += "/sDefaultsign=`"$signTool`""
}
& $ISCC @ISCCArgs
if ($LASTEXITCODE -ne 0) {
    Write-Error "Inno Setup failed"
    exit 1
}

$InstallerPath = Join-Path $OutputDir "$SetupName.exe"

# Sign the installer if all signing credentials are available
if (
    $env:AZURE_TENANT_ID -and
    $env:AZURE_CLIENT_ID -and
    $env:AZURE_CLIENT_SECRET -and
    $env:AZURE_SIGNING_ENDPOINT -and
    $env:AZURE_SIGNING_ACCOUNT -and
    $env:AZURE_SIGNING_CERT_PROFILE
) {
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

if ($Install) {
    Write-Host "==> Launching installer..." -ForegroundColor Cyan
    Start-Process -FilePath $InstallerPath
}
