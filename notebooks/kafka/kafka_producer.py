"""
This simple code illustrates a Kafka producer:
- read data from a CSV file, stored locally, books-amazon.csv
- for each data record, produce a json record
- send the json record to a Kafka messaging system

We use the Python client library kafka-python from 
    https://kafka-python.readthedocs.io/en/master/.

Also, see https://pypi.org/project/kafka-python/
"""

import os, time, json, datetime, csv
import pandas as pd

from kafka import KafkaProducer

#=======================================================
# args: could have been set while calling the code

topic = 'chicago-crime'
inputfile = '../../dados/chicago_crime.csv'
chunksize = 1000  # number of rows
sleeptime = 0.3

#=======================================================

# Function for jsonifying a timestamp into a string
def datetime_converter(dt):
    if isinstance(dt, datetime.datetime):
        return dt.__str__()

def on_send_success(metadata):
    print('Published successfully to Kafka cluster. Metadata is')
    print(f'Topic: {metadata.topic}, Partition: {metadata.partition}, Offset: {metadata.offset}')
   

def on_send_error(excp):
    print('I am an errback', exc_info=excp)
    # handle exception

# Kafka connection
kafka_producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    api_version=(3, 9),
    value_serializer=lambda x: json.dumps(x).encode('utf-8') # instead of byte array
)

# Emulating the situation that we have real-time data to be sent to kafka
# And because file might be big, we emulate by reading chunk, using iterator 
# and chunksize

# Read data from file
input_data = pd.read_csv(inputfile, iterator=True, chunksize=int(chunksize))

# Open the CSV file
with open(inputfile, mode='r') as file:
    # Create a CSV DictReader object
    csv_reader = csv.DictReader(file)

    # Iterate through the lines in the CSV file
    for row in csv_reader:
        # Each row is a dictionary
        row = {k.replace(' ', '_'): v for k, v in row.items()}
        json_data = json.dumps(row, default=datetime_converter)
        # json_data = row.to_dict(orient='records')

        # Check if any event/error sent
        print(f'----- Sending to Kafka: {json_data}')
        try:
            kafka_producer.send(topic, value=json_data).add_callback(on_send_success).add_errback(on_send_error)
            
        except Exception as e:
            print(f'Encountered error while trying to publish: {e}')

        # Block until all pending messages are at least put on the network
        # But still it may not guarantee delivery or success!
        # not using because it is just one row
        # kafka_producer.flush() 

        time.sleep(sleeptime)

# Close the producer
kafka_producer.close()

# Case of blocking until a single message is sent (or timeout)
# For example, if we want to send the 1st row of
# a csv file separately
# columns = pd.read_csv('...', nrows=0).columns
# future = producer.send(topic, columns)
# result = future.get(timeout=60)

