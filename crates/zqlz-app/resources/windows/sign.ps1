<#
.SYNOPSIS
    Sign Windows executables using Azure Trusted Signing

.DESCRIPTION
    This script signs Windows executables using Azure Trusted Signing.
    Requires Azure credentials and signing configuration.

.PARAMETER FilePath
    Path to the file to sign

.EXAMPLE
    .\sign.ps1 -FilePath "C:\path\to\app.exe"
#>

param(
    [Parameter(Mandatory=$true)]
    [string]$FilePath
)

$ErrorActionPreference = "Stop"

# Check for required environment variables
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
    if (-not (Get-Item env:$var -ErrorAction SilentlyContinue)) {
        $missingVars += $var
    }
}

if ($missingVars.Count -gt 0) {
    Write-Warning "Missing environment variables for Azure Trusted Signing:"
    foreach ($var in $missingVars) {
        Write-Warning "  - $var"
    }
    Write-Warning "Skipping signing."
    exit 0
}

# Check if file exists
if (-not (Test-Path $FilePath)) {
    Write-Error "File not found: $FilePath"
    exit 1
}

Write-Host "Signing $FilePath with Azure Trusted Signing..." -ForegroundColor Cyan

try {
    # Install Azure.CodeSigning module if not present
    if (-not (Get-Module -ListAvailable -Name Azure.CodeSigning)) {
        Write-Host "Installing Azure.CodeSigning module..." -ForegroundColor Yellow
        Install-Module -Name Azure.CodeSigning -Force -Scope CurrentUser
    }

    Import-Module Azure.CodeSigning

    # Perform signing
    Invoke-TrustedSigning `
        -Endpoint $env:AZURE_SIGNING_ENDPOINT `
        -CodeSigningAccountName $env:AZURE_SIGNING_ACCOUNT `
        -CertificateProfileName $env:AZURE_SIGNING_CERT_PROFILE `
        -Files $FilePath `
        -FileDigest SHA256 `
        -TimestampRfc3161 "http://timestamp.acs.microsoft.com" `
        -TimestampDigest SHA256

    Write-Host "Successfully signed $FilePath" -ForegroundColor Green
}
catch {
    Write-Error "Failed to sign file: $_"
    exit 1
}
