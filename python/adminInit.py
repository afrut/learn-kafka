#exec(open("adminInit.py").read())
from time import sleep
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import NewTopic
from printAttr import printAttr

if __name__ == "__main__":
    # Create an admin client. Specify server.
    ac = AdminClient({"bootstrap.servers":"localhost:9092"})

    # Dictionary of topics on the cluster.
    topics = ac.list_topics().topics
    print(f"Number of topics: {len(topics)}")
    for topic in topics.keys():
        print(f"    {topic}")

    # Create a topic. This is an async operation.
    if "test_topic" not in topics.keys():
        dctFuture = ac.create_topics([NewTopic("test_topic", 1)])
        future = dctFuture["test_topic"]
        future.result()     # Block until topic has been created
        print("Created topic \"test_topic\"")
    else:
        dctFuture = ac.delete_topics(["test_topic"])
        future = dctFuture["test_topic"]
        future.result()
        print("Deleted topic \"test_topic\"")

    ## Delete a topic.
    #dctFuture = ac.delete_topics(["test_topic"])
    #future = dctFuture["test_topic"]
    #print(future.done())
    #future.result()
