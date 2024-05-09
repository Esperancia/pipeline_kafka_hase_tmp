import json
import time
from datetime import datetime

from kafka import KafkaConsumer
import happybase


def connectToHbase(topic_name, hbase_server_url):
    conn = happybase.Connection(host="localhost", port=16201)
    conn.open()
    table = conn.table(topic_name)

    # check if connexion works by getting all data in hbase table
    rows = table.scan()

    if not rows:
        conn.create_table(topic_name)
    #else:
        #print(row for row in rows)
    return table


def writeInHbase(topic_name, kafka_server_url, hbase_server_url):
    while True:
        print("good bye")
        consumer = KafkaConsumer(topic_name, bootstrap_servers=[kafka_server_url])
        table = connectToHbase(topic_name, hbase_server_url)

        for message in consumer:
            #print(message)

            row = json.loads(message.value)
            #print(row)

            #print(message.timestamp)
            table.put(
                message.timestamp,
                {
                    "time": row['time'],
                    "customer": row['customer'],
                    "action": row['action'],
                    "device": row['device']
                })
        print("From kafka to Hbase done!")
        time.sleep(1)
