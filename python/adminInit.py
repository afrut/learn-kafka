#exec(open("adminInit.py").read())
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import NewTopic
from time import sleep
from printAttr import printAttr

if __name__ == "__main__":
    # Socket address with which to access kafka cluster
    socketAddress = "[::1]:9092"

    # Create an admin client. Specify server.
    ac = AdminClient({"bootstrap.servers":socketAddress})

    # Sample topic name:
    topicName = "test_topic"

    # Dictionary of topics on the cluster.
    topics = ac.list_topics().topics
    print(f"Number of topics: {len(topics)}")
    for topic in topics.keys():
        print(f"    {topic}")

    # Create a topic. This is an async operation.
    if topicName not in topics.keys():
        dctFuture = ac.create_topics([NewTopic(topicName, 1)], operation_timeout = 60)
        future = dctFuture[topicName]
        future.result()     # Block until topic has been created
        print(f"Created topic {topicName}")

    topics = ac.list_topics().topics
    if topicName in topics.keys():
        dctFuture = ac.delete_topics([topicName], operation_timeout = 60)
        future = dctFuture[topicName]
        future.result()
        print(f"Deleted topic {topicName}")

    topics = ac.list_topics().topics
    if topicName not in topics.keys():
        # Wait until this creation succeeds.
        # This creation may fail because topic deltion in the previous block
        # has to complete.
        while True:
            try:
                dctFuture = ac.create_topics([NewTopic(topicName, 1)], operation_timeout = 60)
                future = dctFuture[topicName]
                future.result()     # Block until topic has been created
                break
            except Exception as e:
                if e.args[0].name() == "TOPIC_ALREADY_EXISTS":
                    # Sleep for 1 second if topic still exists
                    print("Waiting for topic deletion to complete")
                    sleep(1)
        print(f"Created topic {topicName}")
