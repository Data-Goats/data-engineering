import csv
from kafka import KafkaProducer
import json


def kafka_python_producer_sync(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8'))
    print("Sending " + msg)
    producer.flush(timeout=60)


def success(metadata):
    print(metadata.topic)


def error(exception):
    print(exception)


def kafka_python_producer_async(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8')).add_callback(success).add_errback(error)
    producer.flush()


if __name__ == '__main__':
    producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    with open('e-shop clothing 2008.csv') as file:
        reader = csv.DictReader(file, delimiter=";")
        for row in reader:
            kafka_python_producer_sync(producer, row, 'stable_topic')
            producer.flush()
    f.close()
