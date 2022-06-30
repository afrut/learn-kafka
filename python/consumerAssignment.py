#python consumerMisc.py
import logging
from argparse import ArgumentParser
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from initTopic import initTopic
from adminInspectOffsets import printOffsets

if __name__ == "__main__":
    server = "[::1]:9092"
    topic = "test"
    group = "consumerMisc"
    num_messages = 500

    parser = ArgumentParser()
    parser.add_argument("-i", "--init", action = "store_true")
    parser.add_argument("-to", "--timeout", type = float, default = 0.001)
    parser.add_argument("-np", "--num_partitions", type = int, default = 7)
    args = parser.parse_args()
    timeout = args.timeout

    fmt = "%(asctime)s.%(msecs)03d: %(message)s"
    datefmt = "%Y-%m-%D %H:%M:%S:"
    logging.basicConfig(format = fmt, level = logging.INFO, datefmt = datefmt)

    if args.init:
        initTopic(topic, partitions = args.num_partitions)

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
                    # logging.info(f"Received ({key}, {value})")
            # logging.info(f"Processed {N} messages")
            return True
        else:
            logging.info("No messages")
            return False

    def assigned(consumer, partitions):
        logging.info(f"assigned() callback executed")

    config = {"bootstrap.servers": server
        ,"group.id": group
        ,"auto.offset.reset": "earliest"}
    consumer = Consumer(config)
    consumer.subscribe([topic], on_assign = assigned)

    # ----------------------------------------
    #  Consumer Assignment
    # ----------------------------------------
    try:
        # Ensure that consumer has been assigned partitions
        while(len(consumer.assignment()) == 0):
            logging.info("Consumer has no assignment yet")
            consumer.poll(timeout)

        # Get partitions assigned to this consumer
        tps = sorted(consumer.assignment(), key = lambda tp: tp.partition)
        logging.info(f"Consumer assigned {len(tps)} partitions:")
        printOffsets(consumer, tps)

        # Start consuming from half-way point of all partitions
        wos = {tp.partition: consumer.get_watermark_offsets(tp) for tp in tps}
        for tp in tps:
            pnum = tp.partition
            lo = wos[pnum][0]
            hi = wos[pnum][1]
            tp.offset = int((lo + hi) / 2)
            consumer.seek(tp)

        while process(consumer.consume(num_messages = 1, timeout = 1)):
            pass

    except KeyboardInterrupt:
        pass
    # NOTE: Upon call to subscribe(), consumer receives partition assignments
    # from broker. Some commands need partition assignment to be complete before
    # properly executing, ie. (seek(), ). An error is raised when such methods
    # are called before partition assignment completes.
    
    printOffsets(consumer, tps)
    consumer.close()
    logging.info("Consumer closed.")