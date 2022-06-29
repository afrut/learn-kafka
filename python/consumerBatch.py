#python consumerBatch.py
import logging
from argparse import ArgumentParser
from confluent_kafka import Consumer

from initTopic import initTopic

if __name__ == "__main__":
    server = "[::1]:9092"
    group = "consumerBatch"
    topic = "test"

    parser = ArgumentParser()
    parser.add_argument("-i", "--init", action = "store_true", default = False)
    args = parser.parse_args()

    fmt = "%(asctime)s: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(format = fmt, level = logging.INFO, datefmt = datefmt)

    if args.init:
        initTopic(topic)

    config = {"bootstrap.servers": server
        ,"group.id": group
        ,"auto.offset.reset": "earliest"}
    consumer = Consumer(config)
    consumer.subscribe([topic])

    def process(msgs: list):
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
        if len(msgs) > 0:
            logging.info(f"Processed {len(msgs)} messages")

    try:
        while True:
            msgs = consumer.consume(num_messages = 10, timeout = 1)
            process(msgs)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        logging.info("Consumer closed.")

# NOTE: Behind the scenes, batching is abstracted away from the developer by
# librdkafka maintaining a buffer for the consumer. consume() has much better
# throughput than poll(). See https://github.com/confluentinc/confluent-kafka-python/issues/580