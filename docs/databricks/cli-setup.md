# Databricks CLI Setup

## Current Windows Note

This machine has Databricks CLI installed through WinGet, but `databricks` is not on the current PowerShell `PATH` yet.

Use wrapper:

```powershell
.\scripts\databricks\dbx.ps1 --version
```

Or open a new terminal after adding the WinGet package folder to `PATH`.

## Login

Run with your workspace URL:

```powershell
.\scripts\databricks\dbx.ps1 auth login --host https://<your-workspace-url>
```

Then verify:

```powershell
.\scripts\databricks\dbx.ps1 auth profiles
.\scripts\databricks\dbx.ps1 current-user me
.\scripts\databricks\dbx.ps1 bundle validate
```

## Deploy Later

```powershell
.\scripts\databricks\dbx.ps1 bundle deploy
.\scripts\databricks\dbx.ps1 bundle run crypto_whale_lakehouse_job
```
