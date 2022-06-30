#python consumerMisc.py
import logging
from argparse import ArgumentParser
from confluent_kafka import Consumer, TopicPartition
from time import sleep

from initTopic import initTopic

if __name__ == "__main__":
    server = "[::1]:9092"
    topic = "test"
    group = "consumerMisc"
    num_messages = 500

    parser = ArgumentParser()
    parser.add_argument("-i", "--init", action = "store_true")
    parser.add_argument("-to", "--timeout", type = float, default = 0.00001)
    args = parser.parse_args()
    timeout = args.timeout

    fmt = "%(asctime)s: %(message)s"
    datefmt = "%Y-%m-%D %H:%M:%S"
    logging.basicConfig(format = fmt, level = logging.INFO, datefmt = datefmt)

    if args.init:
        initTopic(topic)

    def process(msgs):
        N = len(msgs)
        if N > 0:
            for msg in msgs:
                if msg is None:
                    logging.info("Waiting...")
                elif msg.error():
                    logging.error(f"{msg.error()}")
                else:
                    key = msg.key()
                    if key:
                        key = key.decode("utf-8")
                    value = msg.value().decode("utf-8")
                    logging.info(f"Received ({key}, {value})")
            logging.info(f"Processed {N} messages")
        else:
            logging.info("No messages")

    def assigned(consumer, partitions):
        logging.info("assigned() callback executed")

    config = {"bootstrap.servers": server
        ,"group.id": group
        ,"auto.offset.reset": "earliest"}
    consumer = Consumer(config)
    consumer.subscribe([topic], on_assign = assigned)
    # consumer.subscribe([topic])

    # ----------------------------------------
    #  Consumer Assignment - Upon first call to poll() or consume(), consumer
    #  contacts the broker for assignment of partitions. If poll() or consume()
    #  is too short, consumer may not have received assignment before executing
    #  the next statement.
    # ----------------------------------------
    try:
        # Ensure that consumer has been assigned partitions
        consumer.poll(timeout)
        while(len(consumer.assignment()) == 0):
            logging.info("Consumer has no assignment yet")
            consumer.poll(timeout)
        tps = consumer.assignment()
        logging.info(f"Consumer assigned {len(tps)} partitions:")
        for tp in tps:
            logging.info(f"{' ':4}Partition {tp.partition}")
    except KeyboardInterrupt:
        pass






    consumer.close()
    logging.info("Consumer closed.")