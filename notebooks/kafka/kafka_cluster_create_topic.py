from kafka import KafkaAdminClient
from kafka.admin import NewTopic

#============================
my_topic = 'chicago-crime'
#============================

# Create Kafka topic
try:
    admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
    topic = NewTopic(name=my_topic,
                     num_partitions=1,
                     replication_factor=1)
    admin.create_topics([topic])
    print(f'DEBUG: Topic {my_topic} successfully created.')
except Exception as e:
    print(f'Error creating topic.\n{e}')