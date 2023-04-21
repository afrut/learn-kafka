pwsh start-new-process.ps1 "ZooKeeper" `
    "$Env:KAFKA_HOME/bin/zookeeper-server-start.sh" `
    "$Env:KAFKA_HOME/config/zookeeper.properties"