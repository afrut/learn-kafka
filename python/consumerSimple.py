#python consumerSimple.py
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
from time import sleep

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument("-s", "--server", default = "[::1]:9092")
    parser.add_argument("-r", "--reset", action = "store_true")
    args = parser.parse_args()

    # Create Consumer instance
    config = {"bootstrap.servers": args.server
        ,"group.id": "consumerSimple"
        # Which offset to start reading when no committed offsets exist.
        ,"auto.offset.reset": "earliest"}
    consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # Subscribe to topic
    topics = ["test"]
    consumer.subscribe(topics, on_assign = reset_offset)

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                # Extract the (optional) key and value, and print.
                key = msg.key()
                if key is not None:
                    key = key.decode('utf-8')
                else:
                    key = ''
                print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                    topic=msg.topic(), key=key, value=msg.value().decode('utf-8')))
    except KeyboardInterrupt:
        print(consumer.assignment())
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()