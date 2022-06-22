#python partitionDemo.py
#exec(open("partitionDemo.py").read())
import threading
import random
import logging
import datetime
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import NewTopic
from confluent_kafka.cimpl import KafkaException
from confluent_kafka import Producer
from confluent_kafka import Consumer
from confluent_kafka import TopicPartition

if __name__ == "__main__":
    server = "[::1]:9092"                                       # kafka server
    topic = "learn.partition"                                   # topic name
    numPartitions = 5                                           # number of partitions in topic
    group = "learn.partition.consumers"                         # group name for consumers
    numRecords = 10                                             # number of records to produce per flush
    runTime = 60                                                # number of seconds to continually produce
    init = True                                                 # delete and re-create topic
    lock = threading.Lock()                                     # lock for output dictionary partitionKeyCounts
    numTrials = 5                                               # number of trials to execute
    trials = list()                                             # list containing partition count for each trial
    outputFile = ".\\txt\\learn.partition.counts.txt"

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
    #  Consumer Configuration
    # ----------------------------------------
    config = {"bootstrap.servers": server
        , "group.id": group
        , "auto.offset.reset": "earliest"}

    def consumerThread(num: int, config: dict):
        global run, lock, partitionKeyCounts
        consumer = Consumer(config)
        # Manually assign a partition to the consumer instead of subscribing.
        # When subscribing, sometimes, other consumers are assigned partitions
        # and start consuming before all consumers have been assigned a partition.
        consumer.assign([TopicPartition(topic = topic, partition = num)])
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

    # ----------------------------------------
    #  Producer Configuration
    # ----------------------------------------
    # Function to stop all threads
    def stop(threads):
        logging.info("Terminating threads.")
        global run
        run = False
        for thread in threads:
            thread.join()

    producer = Producer({"bootstrap.servers": server})
    users = ["eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther", "mscott", "dschrute", "jhalpert", "pbeesly", "jlevinson", "ehannon", "abernard", "tflenderson", "kmalone", "amartin", "shudson", "rhoward"]
    products = ["book", "alarm clock", "t-shirts", "gift card", "batteries"]

    # ----------------------------------------
    #  Execution
    # ----------------------------------------
    for tn in range(numTrials):
        now = datetime.datetime.now()
        partitionKeyCounts = {}                                     # dictionary of key counts per partition
        run = True                                                  # global flag to keep threads running

        # Create list of consumer threads
        threads = [threading.Thread(target = consumerThread, args = (num, config,), daemon = True)
            for num in range(numPartitions)]

        # Start consumer threads
        for thread in threads:
            thread.start()

        # Continuously produce threads for a specified amount of time
        try:
            while run:
                for _ in range(numRecords):
                    producer.produce(
                        topic = topic
                        , value = random.choice(products)
                        , key = random.choice(users)
                    )
                producer.flush()
                if (datetime.datetime.now() - now).total_seconds() >= runTime:
                    break
        except KeyboardInterrupt:
            pass
        finally:
            stop(threads)
            trials.append(partitionKeyCounts)

    logging.info("Trials done.")

    # ----------------------------------------
    #  Output Assertion
    # ----------------------------------------
    dct = dict()
    for trialNum in range(numTrials):
        dct[trialNum] = dict()

        # Get a list of all keys in every partition
        keys = list()
        for partition, keyCounts in trials[trialNum].items():

            # List of keys in this trial and partition
            ls = list(keyCounts.keys())

            # For each key in this trial, create a list of partitions that it
            # was found in
            if len(ls) > 0:
                for key in ls:
                    if key in dct[trialNum]:
                        dct[trialNum][key].append(partition)
                    else:
                        dct[trialNum][key] = [partition]

            # Append list of keys in this partition to key sin this trial
            keys = keys + ls

        # Check for duplication
        if len(keys) == len(set(keys)):
            print(f"    Trial {trialNum} PASS.")
        else:
            print(f"    Trial {trialNum} FAIL.")
            for key, partitionsFound in dct[trialNum].items():
                if len(partitionsFound) > 1:
                    print(f"        Key {key} found in partitions {partitionsFound}.")

    # ----------------------------------------
    #  Output
    # ----------------------------------------
    logging.info(f"Writing key counts to {outputFile}.")
    with open(outputFile, "wt") as fl:
        fl.write(f"Key counts:\n")
        for trialNum in range(numTrials):
            fl.write(f"    Trial {trialNum}----------------------------------------\n")
            for partitionNum, keyCounts in sorted(trials[trialNum].items()):
                fl.write(f"        Partition {partitionNum}:\n")
                for k, v in sorted(keyCounts.items()):
                    fl.write(f"            {k}: {v}\n")



    logging.info("All done.")

    # NOTE: Refer to partitioner entry in
    # https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md for
    # different partitioners.