import json
import time
from kafka import KafkaConsumer
import happybase


def connectToHbase(topic_name):
    conn = happybase.Connection(host="127.0.0.1", port=9090)
    conn.open()
    table = conn.table(topic_name)

    tables = conn.tables()
    print(tables)

    rows = table.scan()

    if not rows:
        try:
            conn.create_table(topic_name, {'cf': {}})
            print("Table created!")
        except:
            print("Maybe table already exists but just empty!")
    #else:
        #print(row for row in rows)

    print("tables liste 2:", conn.tables())
    return table


def writeInHbase(topic_name, kafka_server_url):
    print("good bye")
    consumer = KafkaConsumer(topic_name, bootstrap_servers=[kafka_server_url], api_version=(0, 11, 5))
    # consumer = KafkaConsumer(topic_name, bootstrap_servers=[kafka_server_url], auto_offset_reset='earliest')

    table = connectToHbase(topic_name)

    while True:
        print("good bye", table, consumer.poll(60 * 1000))

        '''

        for i, consumer_records in consumer.poll().items():
            for message in consumer_records:
                print(message, 'toto')
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
                    '''

        for message in consumer:
            print(message, 'toto')


        print("From kafka to Hbase done!")
        print("good bye 2", table)

        for i in table.scan():
            print(i)
        time.sleep(1)
