# Starts a bash script within powershell
param (
    $WindowTitle
    ,$Script
)
$Host.UI.RawUI.WindowTitle = $WindowTitle
bash $Script $args