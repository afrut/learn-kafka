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
    # Function to create consumer threads
    def consumerThread(config: dict, stop: threading.Event):
        consumer = Consumer(config)
        # Manually assign a partition to the consumer instead of subscribing.
        # When subscribing, sometimes, other consumers are assigned partitions
        # and start consuming before all consumers have been assigned a partition.
        consumer.subscribe([topic])
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
                print("1 - Add thread")
                print("2 - Remove thread")
                print("3 - View current threads")
                val = int(input("Select an item: "))
                if val == 1:
                    stop = threading.Event()
                    thread = threading.Thread(target = consumerThread, args = (config, stop, ), daemon = True)
                    threads.append((thread, stop))
                    thread.start()
                    logging.info(f"There are now {len(threads)} threads running.")
                elif val == 2:
                    remove = int(input(f"{len(threads)} threads currently exist. Select a thread number to remove: "))
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
                        print(f"Thread {cnt}")
                        cnt = cnt + 1

            except KeyboardInterrupt:
                break
    except Exception as e:
        logging.info(f"ERROR {e}")
    finally:
        logging.info("Closing all consumer.")
        for tpl in threads:
            tpl[1].set()
        for tpl in threads:
            tpl[0].join()

    # sp.call("cls", shell = True)
    # val = input("Type something:")
    # print(f"{val}")