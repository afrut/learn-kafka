#exec(open("producerSimple.py").read())
#python producerSimple.py --num_records 10 --poll
#python producerSimple.py --continuous -t "learn.consumerCallback"
#python producerSimple.py --continuous --sleep 10 --flush
import time
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer

from initTopic import initTopic

if __name__ == "__main__":
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument("-s", "--server", default = "[::1]:9092")
    parser.add_argument("-n", "--num_records", default = 10, type = int)
    parser.add_argument("-p", "--poll", action = "store_true")
    parser.add_argument("-f", "--flush", default = False, action = "store_true")
    parser.add_argument("-t", "--topic", type = str, default = "test")
    parser.add_argument("-c", "--continuous", action = "store_true")
    parser.add_argument("-st", "--sleep", type = int, default = 2)
    parser.add_argument("-i", "--init", action = "store_true")
    parser.add_argument("-np", "--num_partitions", type = int, default = 1)
    args = parser.parse_args()

    # Create Producer instance
    producer = Producer({"bootstrap.servers": args.server})

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    # Produce data by selecting random values from these lists.
    topic = args.topic
    user_ids = ['eabara', 'jsmith', 'sgarcia', 'jbernard', 'htanaka', 'awalther']
    products = ['book', 'alarm clock', 't-shirts', 'gift card', 'batteries']

    # Delete and re-create topic
    if args.init:
        initTopic(topic, partitions = args.num_partitions)

    try:
        while True:
            for _ in range(args.num_records):
                user_id = choice(user_ids)
                product = choice(products)
                producer.produce(topic, product, user_id, callback = delivery_callback)

            # Send messages. Once message is sent, execute callback function.
            # Asynchronous sends.
            if args.poll:
                print(producer.poll(1))

            # Convenience function. Block until the messages are sent. Synchronous send.
            if args.flush:
                producer.flush()

            # Terminate if not continuous mode
            if not(args.continuous):
                break

            time.sleep(args.sleep)
    except KeyboardInterrupt:
        pass