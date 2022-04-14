#exec(open("producerFileText.py").read())
import sys
from random import choice
from random import randint
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
from time import sleep
from initTopic import initTopic
from datetime import datetime

if __name__ == "__main__":
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('data_file', type=FileType('r', encoding='utf-8'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create Producer instance
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print(f"    {datetime.now()}: Sent {msg.value().decode('utf-8')}", end = "")

    # Read a file as data source.
    lines = args.data_file.readlines()
    N = len(lines) - 1

    # Produce data by selecting random values from these lists.
    topic = "lines_of_text"

    initTopic(topic)

    while True:
        nMsg = randint(1, 20)
        print(f"{datetime.now()}: Sending {nMsg} messages")
        for _ in range(nMsg):
            producer.produce(topic, choice(lines), callback=delivery_callback)

        # Send messages. Once message is sent, execute callback function
        producer.poll(10000)

        # Block until the messages are sent. Not strictly necessary for fire and forget.
        producer.flush()

        # Sleep for a random number of seconds
        sleep(randint(1,3))
