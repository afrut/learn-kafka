# Reference for starting other programs and processes

# Start zookeeper in another powershell session.
$scriptblk = {
    $Host.UI.RawUI.WindowTitle='Kafka Zookeeper';
    zookeeper-server-start.bat "$Env:KAFKA_HOME/config/zookeeper.properties"
}
Start-Process pwsh -ArgumentList "-NoExit","-Command",$scriptblk



# Start kafka-server in another powershell session.
$scriptblk = {
    $Host.UI.RawUI.WindowTitle='Kafka Server';
    kafka-server-start.bat "$Env:KAFKA_HOME/config/server.properties"
}
Start-Process pwsh -ArgumentList "-NoExit","-Command",$scriptblk
