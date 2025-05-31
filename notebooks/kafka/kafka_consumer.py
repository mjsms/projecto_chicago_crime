"""
This simple code illustrates a Kafka producer:
- read data from a topic in a Kafka messaging system.
- print out the data

As it stands, it should work with any data as long as the data is in JSON 
(one can modify the code to handle other types of data)

We use the Python client library kafka-python from 
    https://kafka-python.readthedocs.io/en/master/.

Also, see https://pypi.org/project/kafka-python/
"""
import os, json
from kafka import KafkaConsumer

#=======================================================
topic = 'chicago-crime'
# consumer_group = 'my-group'

#=======================================================

# Declare the topic
kafka_consumer = KafkaConsumer(
    bootstrap_servers = 'localhost:9092',
    api_version = (3, 9),
    auto_offset_reset = 'earliest',
    enable_auto_commit = False,
    value_deserializer = lambda x: json.loads(x).encode('utf-8')
    # group_id = consumer_group,
)

kafka_consumer.subscribe([topic])

print("Starting to listen for messages on topic : " + topic + ". ")

for msg in kafka_consumer:
    # msg value and key may be raw bytes - decode if necessary!
    print (f'Received message is [{msg.topic}:{msg.partition}:{msg.offset}] key={msg.key}')
    print (f'   value={msg.value}')

    # Note: msg can be stored in a database or subject to other processing
    # Also, we could have used other formats.

