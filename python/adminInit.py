#exec(open("adminInit.py").read())
from time import sleep
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import NewTopic
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
        dctFuture = ac.create_topics([NewTopic(topicName, 1)])
        future = dctFuture[topicName]
        future.result()     # Block until topic has been created
        print(f"Created topic {topicName}")

    topics = ac.list_topics().topics
    if topicName in topics.keys():
        dctFuture = ac.delete_topics([topicName])
        future = dctFuture[topicName]
        future.result()
        print(f"Deleted topic {topicName}")
