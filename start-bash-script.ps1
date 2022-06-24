# Starts a bash script within powershell
param (
    $WindowTitle
    ,$Script
    ,$Arguments
)
$Host.UI.RawUI.WindowTitle = $WindowTitle
bash $Script $Arguments