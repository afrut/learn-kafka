param (
    $num = ""
)

pwsh start-new-process.ps1 "KafkaBroker$num" "$Env:KAFKA_HOME/bin/kafka-server-start.sh" "$Env:KAFKA_HOME/config/server.properties"