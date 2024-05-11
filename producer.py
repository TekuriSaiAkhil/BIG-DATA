from kafka import KafkaProducer
import csv
import time
from concurrent.futures import ThreadPoolExecutor

def produce_to_kafka(topic, file_path):
    kafka_broker = "localhost:9092"
    producer = KafkaProducer(bootstrap_servers=kafka_broker)
    print("Producer fetching data from API")
    with open(file_path, "r") as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        for row in reader:
            message_data = ",".join(row)
            print(message_data)
            producer.send(topic, value=message_data.encode("utf-8"))
            time.sleep(3)  # Simulate real-time streaming

    producer.close()

if __name__ == "__main__":
    kafka_topics = ["weather_data_time"]
    file_paths = ["new_data_2years.csv"]

    with ThreadPoolExecutor(max_workers=len(kafka_topics)) as executor:
        executor.map(produce_to_kafka, kafka_topics, file_paths)