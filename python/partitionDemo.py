#python partitionDemo.py
import threading
import time
import random
import logging
import datetime
import json
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import NewTopic
from confluent_kafka.cimpl import KafkaException
from confluent_kafka import Producer
from confluent_kafka import Consumer

if __name__ == "__main__":
    server = "[::1]:9092"                                       # kafka server
    topic = "learn.partition"                                   # topic name
    numPartitions = 5                                           # number of partitions in topic
    group = "learn.partition.consumers"                         # group name for consumers
    numConsumers = 5                                            # number of consumers to start
    ts = 2                                                      # seconds to sleep when consumers receive no messages
    run = True                                                  # global flag to keep threads running
    numRecords = 10                                             # number of records to produce per flush
    runTime = 10                                                # number of seconds to continually produce
    init = True                                                 # delete and re-create topic
    lock = threading.Lock()                                     # lock for output dictionary partitionKeyCounts
    filepath = ".\\txt\\learn.partition.counts.txt"             # text file containing key counts
    partitionKeyCounts = {}                                     # dictionary of key counts per partition

    # ----------------------------------------
    #  Initialization
    # ----------------------------------------
    # Configure logging
    fmt = "%(asctime)s: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(format = fmt, level = logging.INFO, datefmt = datefmt)

    # Empty the file of key counts
    with open(filepath, "wt") as fl:
        fl.write("")

    # Create admin client
    ac = AdminClient({"bootstrap.servers": server
        # , "partitioner": "consistent"
    })

    # Try to delete the topic
    if init:
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

    def consumerThread(config: dict):
        global run, lock, partitionKeyCounts
        consumer = Consumer(config)
        consumer.subscribe([topic])
        keys = dict()

        def process(msg):
            if msg is None:
                    logging.info(f"Consumer {num} waiting...")
            elif msg.error():
                logging.info(f"ERROR: Consumer {num}: {msg.error()}")
            else:
                key = msg.key()
                if key:
                    key = key.decode("utf-8")
                if key in keys:
                    n = keys[key]
                    keys[key] = n + 1
                else:
                    keys[key] = 1
                value = msg.value().decode("utf-8")
                logging.info(f"Consumer {consumer.assignment()[0].partition} received ({key}, {value})")

        # Get the partition number assigned to this consumer
        while len(consumer.assignment()) == 0:
            if run:
                msg = consumer.poll(1.0)
                if msg:
                    process(msg)
        num = consumer.assignment()[0].partition

        # Main consume loop
        while True:
            if run:
                process(consumer.poll(1.0))
            else:
                # Obtain a lock for the file of key counts
                with lock:
                    partitionKeyCounts[num] = keys
                consumer.close()
                logging.info(f"Consumer for partition {num} closed.")
                break

    # Create list of consumer threads
    threads = [threading.Thread(target = consumerThread, args = (config,), daemon = True)
        for _ in range(numConsumers)]

    # Start consumer threads
    for thread in threads:
        thread.start()

    # ----------------------------------------
    #  Producer
    # ----------------------------------------
    # Function to stop all threads
    def stop():
        logging.info("Terminating threads.")
        global run
        run = False
        for thread in threads:
            thread.join()

    producer = Producer({"bootstrap.servers": server, "partitioner": "consistent"})
    users = ["eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther", "mscott", "dschrute", "jhalpert", "pbeesly", "jlevinson", "ehannon", "abernard", "tflenderson", "kmalone", "amartin", "shudson", "rhoward"]
    products = ["book", "alarm clock", "t-shirts", "gift card", "batteries"]

    # Wait until Ctrl + c is pressed to terminate all threads and close all
    # consumers
    now = datetime.datetime.now()
    try:
        while run:
            for _ in range(numRecords):
                producer.produce(
                    topic = topic
                    , value = random.choice(products)
                    , key = random.choice(users)
                    # , partition = "consistent"
                )
            producer.flush()
            if (datetime.datetime.now() - now).total_seconds() >= runTime:
                break
    except KeyboardInterrupt:
        pass
    finally:
        stop()

    # Print count of keys in each partition
    logging.info(f"Partition key counts:")
    for partitionNum, keyCounts in sorted(partitionKeyCounts.items()):
        logging.info(f"    {partitionNum}")
        for key_, value_ in keyCounts.items():
            logging.info(f"        {key_}: {value_}")

    logging.info("Done.")

    # NOTE: Refer to partitioner entry in
    # https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md for
    # different partitioners.