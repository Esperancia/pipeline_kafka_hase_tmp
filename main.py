import json
import os
import shutil
import threading

from kafka.admin import KafkaAdminClient, NewTopic
from consumer import writeInHbase
from producer import writeToKafka

# Define globale variables
source_folder = "./new/"
destination_folder = "./done/"
kafka_server_url = "127.0.0.1:9092"
topic_name = "sensor_data"


def getTopic():
    # Check if topic exists, if not create it
    admin_client = KafkaAdminClient(bootstrap_servers=kafka_server_url, client_id='PouchPipeline')
    server_topics = admin_client.list_topics()

    #print(server_topics)

    if topic_name not in server_topics:
        topics_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
        admin_client.create_topics(new_topics=topics_list, validate_only=False)
        print("Topic created!")

    return topic_name


def getNewJson():
    # Check if there are new files in folder "new"
    files = list(filter(lambda f: f.endswith('.json'), os.listdir(source_folder)))
    #print(files)
    return files


def moveDoneJson(file):
    # Move used files into destination folder
    try:
        shutil.move(f'{source_folder}{file}', f'{destination_folder}{file}')
        print("Done!")
    except:
        print("There is an error when trying to move file, please check!")

def readFileData(file):
    with open(f"{source_folder}/{file}", "r") as file:
        file_data = [json.loads(line.rstrip()) for line in file]
        return file_data


def getData():
    #while True:
        for f in getNewJson():
            data = readFileData(f)
            count = 0

            for row in data:
                writeToKafka(getTopic(), row, kafka_server_url)
                count+=1
                print("Wrote {0} messages into topic: {1}".format(count, topic_name))

            #moveDoneJson(f)
        print("hello world")
        #time.sleep(5)


if __name__ == "__main__":
    consumer = threading.Thread(target=getData())
    producer = threading.Thread(target=writeInHbase(topic_name, kafka_server_url))

    producer.start()
    consumer.start()

    producer.join()
    consumer.join()
