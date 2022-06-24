# Start two bash instances to run zookeper and kafka server
pwsh start-zookeeper.ps1
pwsh start-kafka-broker.ps1



# Write-Host $kafkahome
# $scriptblk = {
#     $Host.UI.RawUI.WindowTitle='Kafka Zookeeper';
#     bash "$Env:KAFKA_HOME/bin/zookeeper-server-start.sh" "$Env:KAFKA_HOME/config/zookeeper.properties"
# }
# Start-Process pwsh -ArgumentList "-NoExit", "-Command",$scriptblk



# # Start kafka-server in another powershell session.
# $scriptblk = {
#     $Host.UI.RawUI.WindowTitle='Kafka Server';
#     bash "$Env:KAFKA_HOME/bin/kafka-server-start.sh" "$Env:KAFKA_HOME/config/server.properties"
# }
# Start-Process pwsh -ArgumentList "-NoExit", "-Command",$scriptblk
