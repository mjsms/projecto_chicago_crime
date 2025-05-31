from kafka import KafkaAdminClient
from kafka.cluster import ClusterMetadata


try:
    admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
    print(f'List of topics in the cluster: {admin.list_topics()}')
    print(f'List of consumer groups known to the cluster: {admin.list_consumer_groups()}')
except Exception:
    print(f'Error connecting to cluster')


# Additional details

# From metadata
clusterMetadata = ClusterMetadata(bootstrap_servers=['localhost:9092'])

# Get all brokers metadata
print(f'All brokers metadata: {clusterMetadata.brokers()}')

# Get all partitions of a topic
print(clusterMetadata.partitions_for_topic('chicago-crime'))

# List topics
print(f'Topics: {clusterMetadata.topics()}')