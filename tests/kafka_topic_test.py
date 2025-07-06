from confluent_kafka.admin import AdminClient
from dotenv import load_dotenv
import os

load_dotenv()

# Create admin client
admin_config = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("KAFKA_API_KEY"),
    'sasl.password': os.getenv("KAFKA_API_SECRET")
}

admin_client = AdminClient(admin_config)

# List all topics
print("📋 Listing all topics...")
metadata = admin_client.list_topics(timeout=10)

print(f"Available topics: {list(metadata.topics.keys())}")

# Check if our topic exists
topic_name = "polygon_stocks"
if topic_name in metadata.topics:
    topic_metadata = metadata.topics[topic_name]
    print(f"✅ Topic '{topic_name}' exists!")
    print(f"Partitions: {len(topic_metadata.partitions)}")
    for partition_id, partition_info in topic_metadata.partitions.items():
        print(f"  Partition {partition_id}: Leader={partition_info.leader}, Replicas={len(partition_info.replicas)}")
else:
    print(f"❌ Topic '{topic_name}' does not exist!")
    print("This is why the consumer can't get partition assignments.")
    print("Make sure your producer is running first to create the topic.")