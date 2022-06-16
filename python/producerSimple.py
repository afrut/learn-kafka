#exec(open("producerSimple.py").read())
#python producerSimple.py
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer

if __name__ == "__main__":
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument("-s", "--server", default = "[::1]:9092")
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
    topic = "test"
    user_ids = ['eabara', 'jsmith', 'sgarcia', 'jbernard', 'htanaka', 'awalther']
    products = ['book', 'alarm clock', 't-shirts', 'gift card', 'batteries']

    count = 0
    for _ in range(10):
        user_id = choice(user_ids)
        product = choice(products)
        producer.produce(topic, product, user_id, callback = delivery_callback)
        count += 1

    # Send messages. Once message is sent, execute callback function.
    producer.poll(1)

    # Convenience function. Block until the messages are sent.
    producer.flush()