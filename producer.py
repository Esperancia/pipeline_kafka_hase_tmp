import json
from kafka import KafkaProducer

def error_callback(exc):
    raise Exception('Error while sending data to kafka: {0}'.format(str(exc)))


def writeToKafka(topic_name, data, kafka_server_url):
    producer = KafkaProducer(bootstrap_servers=[kafka_server_url])
    producer.send(topic_name, value=json.dumps(data).encode("utf-8")).add_errback(error_callback)
    producer.flush()

