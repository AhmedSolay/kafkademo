from confluent_kafka import Consumer, KafkaException, KafkaError
import requests
import random

# Group ID
me = 'soulytopic-1'
# Configuration consumer
conf = {
    'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094',
    'enable.auto.commit':True,
    'group.id': 'group1',                    
    'auto.offset.reset': 'earliest'          
}

# Create Consumer 
consumer = Consumer(conf)
consumer.subscribe([me])

def detect_object(id):
    return random.choice(['car', 'house', 'person'])

try:
    while True:
        # Poll for new messages
        msg = consumer.poll(timeout=1.0)  # Timeout in seconds

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Reached end of partition {msg.partition()}")
            else:
                raise KafkaException(msg.error())
        else:
            message_value = msg.value().decode('utf-8')
            requests.put('http://127.0.0.1:5000/object/' + message_value, json={"object": detect_object(id)})
            print(f"Consumer group {'group1'}: Received message: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    print("Consumer interrupted")

finally:
    consumer.close()
