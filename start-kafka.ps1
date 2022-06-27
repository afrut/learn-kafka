# Start zookeeper and a number of brokers
param (
    $num = 1
)
pwsh start-zookeeper.ps1
pwsh start-kafka-broker.ps1 -num $num