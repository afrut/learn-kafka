#python partitionDemo.py
import threading
import time
import random
import logging
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import NewTopic
from confluent_kafka.cimpl import KafkaException
from confluent_kafka import Producer
from confluent_kafka import Consumer

if __name__ == "__main__":
    server = "[::1]:9092"                       # kafka server
    topic = "learn.partition"                   # topic name
    numPartitions = 5                           # number of partitions in topic
    group = "learn.partition.consumers"         # group name for consumers
    numConsumers = 5                            # number of consumers to start
    ts = 2                                      # seconds to sleep when consumers receive no messages
    run = True                                  # global flag to keep threads running
    numRecords = 10                             # number of records to produce every second

    # ----------------------------------------
    #  Initialization
    # ----------------------------------------
    # Configure logging
    fmt = "%(asctime)s: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(format = fmt, level = logging.INFO, datefmt = datefmt)

    # Create admin client
    ac = AdminClient({"bootstrap.servers": server})

    # Try to delete the topic
    try:
        future = ac.delete_topics(topics = [topic])[topic]
        future.result()
    except KafkaException as e:
        ke = e.args[0]
        if ke.name() != "UNKNOWN_TOPIC_OR_PART":
            raise e

    # Create the topic
    nt = NewTopic(topic = topic, num_partitions = numPartitions)
    while True:
        try:
            future = ac.create_topics(new_topics = [nt])[topic]
            future.result()
            break
        except KafkaException as e:
            ke = e.args[0]
            if ke.name() == "TOPIC_ALREADY_EXISTS"\
                and ke.str() == "Topic 'learn.partition' is marked for deletion.":
                continue

    assert len(ac.list_topics().topics[topic].partitions) == numPartitions, "Incorrect number of partitions"
    logging.info(f"Topic \"{topic}\" created with {numPartitions} partitions.")

    # ----------------------------------------
    #  Consumer
    # ----------------------------------------
    config = {"bootstrap.servers": server
        , "group.id": group
        , "auto.offset.reset": "earliest"}

    def consumerThread(num: int, config: dict):
        global run
        consumer = Consumer(config)
        consumer.subscribe([topic])
        while True:
            if run:
                msg = consumer.poll(1.0)
                if msg is None:
                    logging.info(f"Consumer {num} waiting...")
                elif msg.error():
                    logging.info(f"ERROR: Consumer {num}: {msg.error()}")
                else:
                    key = msg.key()
                    if key:
                        key = key.decode("utf-8")
                    value = msg.value().decode("utf-8")
                    logging.info(f"Consumer {num} received ({key}, {value})")
            else:
                consumer.close()
                logging.info(f"Consumer {num} closed.")
                break

    # Create list of consumer threads
    threads = [threading.Thread(target = consumerThread, args = (num, config,), daemon = True)
        for num in range(numConsumers)]

    # Start consumer threads
    for thread in threads:
        thread.start()

    # ----------------------------------------
    #  Producer
    # ----------------------------------------
    producer = Producer({"bootstrap.servers": server})
    users = ['eabara', 'jsmith', 'sgarcia', 'jbernard', 'htanaka', 'awalther']
    products = ['book', 'alarm clock', 't-shirts', 'gift card', 'batteries']

    # Wait until Ctrl + c is pressed to terminate all threads and close all consumers
    try:
        while run:
            for _ in range(numRecords):
                producer.produce(topic
                    , random.choice(products)
                    , random.choice(users))
            producer.flush()
            time.sleep(0.5)
    except KeyboardInterrupt:
        logging.info("Terminating threads.")
        run = False
        for thread in threads:
            thread.join()

    logging.info("Done.") 