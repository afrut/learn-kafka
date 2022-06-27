# Starts a new powershell process
param (
    [string]$WindowTitle
    ,[string]$Script
)
Start-Process pwsh -ArgumentList "-NoExit start-bash-script.ps1 $WindowTitle $Script $args"