
"""
This consumes a kafka topic and posts the image to a browser

It just wraps the image"""

import numpy as np
from kafka import KafkaConsumer


def loop():
    i = 0
    while True:
        for msg in consumer:
            # print the message number and the length of the mesage
            print("v == {} : {}".format(i, len(msg.value)))
            i += 1

if __name__ == '__main__':

    #
    #
    # connect to Kafka server and pass the topic we want to consume
    #
    #  auto_offset_reset='earlist',
    #
    # Continuously listen to the connection and print messages as recieved
    consumer = KafkaConsumer(
        'my-topic_txt',
        group_id='view',
        auto_offset_reset='earlist',
        bootstrap_servers=['0.0.0.0:9092'])

    loop()
