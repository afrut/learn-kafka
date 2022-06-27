# Starts one or multiple kafka brokers
param (
    [int]$num = 1
    ,[int]$id = 0
    ,[int]$port = 9092
)

if($num -gt 1)
{
    $p = 9092
    $dirs = "/tmp/kafka-logs"
    for($cnt = 0; $cnt -lt $num; $cnt++)
    {
        $WindowTitle = "KafkaBroker-" + $cnt
        $props = @("$Env:KAFKA_HOME/config/server.properties"
            , "--override listeners=PLAINTEXT://[::1]:$p"
            , "--override broker.id=$cnt"
            , "--override log.dir=$dirs"
            , "--override log.dirs=$dirs") -join " "
        pwsh start-new-process.ps1 `
            $WindowTitle `
            "$Env:KAFKA_HOME/bin/kafka-server-start.sh" `
            $props
        $p++
        $dirs = "/tmp/kafka-logs-" + "$($cnt + 1)"
    }
}
else
{
    pwsh start-new-process.ps1 `
        "KafkaBroker:$port" `
        "$Env:KAFKA_HOME/bin/kafka-server-start.sh" `
        "$Env:KAFKA_HOME/config/server.properties --override listeners=PLAINTEXT://[::1]:$port --override broker.id=$id"
}