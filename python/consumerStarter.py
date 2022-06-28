#python consumerStarter.py
import subprocess as sp
import threading
import logging
from time import sleep
from confluent_kafka import Consumer, TopicPartition

from initTopic import initTopic


if __name__ == '__main__':
    # ----------------------------------------
    #  Parameters
    # ----------------------------------------
    topic = "learn.consumerStarter"
    server = "[::1]:9092"
    partitions = 7
    group = topic + ".consumers"
    lock = threading.Lock()

    # ----------------------------------------
    #  Parameters
    # ----------------------------------------
    # Initialize topic
    initTopic(topic, server = server, partitions = partitions)

    # Configure logging
    fmt = "%(asctime)s: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(format = fmt, level = logging.INFO, datefmt = datefmt)

    # ----------------------------------------
    #  Consumer Configuration
    # ----------------------------------------
    # Function callback when partitions are assigned to a consumer
    def assignCallback(consumer, partitions):
        global lock
        with lock:
            logging.info(f"Consumer is assigned {len(partitions)} partitions:")
            for partition in partitions:
                logging.info(f"    {partition.partition}")
        # logging.info(f"Consumer {consumer.")
        # print(type(consumer.assignment()))
        # print(type(partitions[0]))
        # partition = partitions[0]
        # print(f"partition.error[{type(partition.error)}] = {partition.error}")
        # print(f"partition.offset[{type(partition.offset)}] = {partition.offset}")
        # print(f"partition.partition[{type(partition.partition)}] = {partition.partition}")
        # print(f"partition.topic[{type(partition.topic)}] = {partition.topic}")
        # for x in dir(partitions[0]):
        #     print(x)

    # Function to create consumer threads
    def consumerThread(config: dict, stop: threading.Event):
        consumer = Consumer(config)
        # Manually assign a partition to the consumer instead of subscribing.
        # When subscribing, sometimes, other consumers are assigned partitions
        # and start consuming before all consumers have been assigned a partition.
        consumer.subscribe([topic], on_assign = assignCallback)
        keys = dict()

        # Function to process messages
        def process(msg):
            if msg is None:
                # logging.info(f"Consumer waiting...")
                pass
            elif msg.error():
                logging.info(f"ERROR: Consumer {msg.error()}")
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
                # logging.info(f"Consumer received ({key}, {value})")

        # Main consume loop
        try:
            while True:
                process(consumer.poll(1.0))
                if stop.is_set():
                    break
        finally:
            consumer.close()
            logging.info("Consumer closed.")

    config = {"bootstrap.servers": server
        , "group.id": group
        , "auto.offset.reset": "earliest"}

    # ----------------------------------------
    #  Execution
    # ----------------------------------------
    threads = list()
    try:
        while True:
            try:
                with lock:
                    logging.info("1 - Add thread")
                    logging.info("2 - Remove thread")
                    logging.info("3 - View current threads")
                    logging.info("4 - Quit")
                    logging.info("Select an item:")
                val = int(input())
                if val == 1:
                    stop = threading.Event()
                    thread = threading.Thread(target = consumerThread, args = (config, stop, ), daemon = True)
                    threads.append((thread, stop))
                    thread.start()
                    logging.info(f"There are now {len(threads)} threads running.")
                elif val == 2:
                    with lock:
                        logging.info(f"{len(threads)} threads currently exist. Select a thread number to remove:")
                    remove = int(input())
                    if remove >= len(threads):
                        logging.info("No such thread. Index out of range.")
                        continue
                    tpl = threads[remove]
                    tpl[1].set()
                    threads.remove(tpl)
                    logging.info(f"There are now {len(threads)} threads running.")
                elif val == 3:
                    cnt = 0
                    for tpl in threads:
                        logging.info(f"Thread {cnt}")
                        cnt = cnt + 1
                elif val == 4:
                    break
            except KeyboardInterrupt:
                break
    except Exception as e:
        logging.info(f"ERROR {e}")
    finally:
        logging.info("Closing all consumers.")
        for tpl in threads:
            tpl[1].set()
        for tpl in threads:
            tpl[0].join()