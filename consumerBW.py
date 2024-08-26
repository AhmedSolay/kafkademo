from confluent_kafka import Consumer, KafkaException, KafkaError
import requests
import os
# import random
import cv2

# Group ID and Topic
topic = 'soulytopic-1'

# Configuration for the consumer
conf = {
    'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094',
    'enable.auto.commit': True,
    'group.id': 'group2',
    'auto.offset.reset': 'earliest'
}

# Create Consumer
consumer = Consumer(conf)
consumer.subscribe([topic])

IMAGES_DIR = "images"  # Directory where images are stored

def process_image(image_id):
    image_path = os.path.join(IMAGES_DIR, f"{image_id}.jpg")  # Assuming image extension is jpg; adjust if needed
    if os.path.exists(image_path):
        # Read the image using OpenCV
        img = cv2.imread(image_path)
        if img is not None:
            # Convert the image to black and white
            bw_image = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            # Save the black and white image
            cv2.imwrite(image_path, bw_image)
            print(f"Processed image {image_id} and saved as black and white.")
        else:
            print(f"Failed to read image {image_id}.")
    else:
        print(f"Image with ID {image_id} not found.")


# def detect_object(id):
#     return random.choice(['car', 'house', 'person'])


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
            process_image(message_value)  # Process the image based on the ID
            requests.put('http://127.0.0.1:5000/object/' + message_value)
            print(f"Consumer group 'group2': Received message: {message_value}")

except KeyboardInterrupt:
    print("Consumer interrupted")

finally:
    consumer.close()
#, json={"object": "processed_img"}