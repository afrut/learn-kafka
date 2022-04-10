# Reference for starting other programs and processes

# Start zookeeper in another powershell session.
$scriptblk = {
    $Host.UI.RawUI.WindowTitle='Kafka Zookeeper';
    zookeeper-server-start.bat "$Env:KAFKA_HOME/config/zookeeper.properties"
}
$StartInfo = New-Object System.Diagnostics.ProcessStartInfo
$StartInfo.FileName = "D:\Program Files\Powershell\7\pwsh.exe"
$StartInfo.Arguments = '-NoExit' `
    ,"-Command $scriptblk"
[System.Diagnostics.Process]::Start($StartInfo)

# Start kafka-server in another powershell session.
$scriptblk = {
    $Host.UI.RawUI.WindowTitle='Kafka Server';
    kafka-server-start.bat "$Env:KAFKA_HOME/config/server.properties"
}
$StartInfo.Arguments = '-NoExit' `
    ,"-Command $scriptblk"
[System.Diagnostics.Process]::Start($StartInfo)
