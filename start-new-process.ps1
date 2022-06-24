# Starts a new powershell process
param (
    $WindowTitle
    ,$Script
    ,$Arguments
)
Start-Process pwsh -ArgumentList "-NoExit start-bash-script.ps1 $WindowTitle $Script $Arguments"