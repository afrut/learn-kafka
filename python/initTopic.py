# Check to see if a topic exists. If it does, delete and create it. If not, create it.
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import NewTopic
from time import sleep

def initTopic(topicName: str, server: str = "[::1]:9092", ac: AdminClient = None):
    # Create an admin client. Specify server.
    if ac is None:
        ac = AdminClient({"bootstrap.servers": server})

    # Check if topic already exists. Delete if it does.
    topics = ac.list_topics().topics
    if topicName in topics.keys():
        dctFuture = ac.delete_topics([topicName])
        future = dctFuture[topicName]
        future.result()

    # Create a topic. Keep trying until no errors.
    # Errors can come up due to topic deletion being incomplete.
    while True:
        try:
            dctFuture = ac.create_topics([NewTopic(topicName, 1)], operation_timeout = 60)
            future = dctFuture[topicName]
            future.result()     # Block until topic has been created
            break
        except Exception as e:
            if e.args[0].name() == "TOPIC_ALREADY_EXISTS":
                # Sleep for 1 second if topic still exists
                sleep(1)



if __name__ == "__main__":
    server = "[::1]:9092"
    topicName = "test_topic"

    # Create an admin client. Specify server.
    ac = AdminClient({"bootstrap.servers": server})

    # Initialize a topic.
    initTopic("test_topic", server)

    # Check if topic was created
    topics = ac.list_topics().topics
    if topicName in topics.keys():
        print(f"{topicName} initialized.")
