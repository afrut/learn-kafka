# To start the kafka cluster, run these commands in a new window
zookeeper-server-start.bat "$Env:KAFKA_HOME/config/zookeeper.properties"
kafka-server-start.bat "$Env:KAFKA_HOME/config/server.properties"

# Create a topic
kafka-topics.bat --create --topic quickstart-events --bootstrap-server localhost:9092

# See status of a topic
kafka-topics.bat --describe --topic quickstart-events --bootstrap-server localhost:9092

# Write some events into the topic using the console producer client
# This will prompt for some input to write to the topic
kafka-console-producer.bat --topic quickstart-events --bootstrap-server localhost:9092

# Use the console consumer client to read the topic
# This will print the contents of the topic
kafka-console-consumer.bat --topic quickstart-events --from-beginning --bootstrap-server localhost:9092
