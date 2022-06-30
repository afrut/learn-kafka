#python adminInspectOffsets.py

# This script uses a consumer to read some metadata/status in the context of a
# (consumer group, topic, partitions). To test, start consumerSimple.py,
# producerSimple.py -c -f, and watch output of this script.

import subprocess as sp
import logging
import time
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

if __name__ == "__main__":
    server = "[::1]:9092"
    group = "consumerSimple"
    topic = "test"

    fmt = "%(asctime)s.%(msecs)03d: %(message)s"
    datefmt = "%Y-%m-%D %H:%M:%S"
    logging.basicConfig(format = fmt, level = logging.INFO, datefmt = datefmt)

    config = {"bootstrap.servers": server
        ,"group.id": group}
    consumer = Consumer(config)

    ac = AdminClient({"bootstrap.servers": server})
    partitions = ac.list_topics().topics[topic].partitions
    tps = [TopicPartition(topic, p, 0) for p in partitions]
    logging.info(f"Found {len(partitions)} partition in topic {topic}")

    try:
        while True:
            sp.call("cls", shell = True)
            wos = {tp.partition: consumer.get_watermark_offsets(tp) for tp in tps}
            ctds = consumer.committed(tps)
            for ctd in ctds:
                p = ctd.partition
                logging.info(f"partition = {p}, low = {wos[p][0]}, high = {wos[p][1]}, committed = {ctd.offset}")
            time.sleep(1)
    except KeyboardInterrupt:
        pass

    # NOTE: There is no call to consumer.subscribe() or consumer.assign(), so
    # this script does not trigger a rebalance by having a consumer join in the
    # consumer group. Watch the broker console/logs while running this script to
    # verify.