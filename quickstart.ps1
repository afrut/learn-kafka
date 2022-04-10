## To start the kafka cluster, run these commands in a new window
#zookeeper-server-start.bat "$Env:KAFKA_HOME/config/zookeeper.properties"
#kafka-server-start.bat "$Env:KAFKA_HOME/config/server.properties"

# Get machine ip of host running WSL2, where kafka is started.
$hostname = "[::1]"
$port = 9092
$socketaddress = "$hostname`:$port"

# Create a topic
kafka-topics.bat --create --topic quickstart-events --bootstrap-server $socketaddress

# See status of a topic
kafka-topics.bat --describe --topic quickstart-events --bootstrap-server $socketaddress

# List available topics int he cluster
kafka-topics.bat --list --bootstrap-server $socketaddress

# Write some events into the topic using the console producer client
# This will prompt for some input to write to the topic
kafka-console-producer.bat --topic quickstart-events --bootstrap-server $socketaddress

# Use the console consumer client to read the topic
# This will print the contents of the topic
kafka-console-consumer.bat --topic quickstart-events --from-beginning --bootstrap-server $socketaddress
