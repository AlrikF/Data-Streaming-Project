import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
   
   
default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 00)
}


# Fetch data from randomuser.me
def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]

    return res


def format_data(res):
    data = {}
    location = res['location']
    data['id'] = uuid.uuid4()
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data


def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging


    # A Kafka client that publishes records to the Kafka cluster.
    # The producer consists of a pool of buffer space that holds records that haven’t yet been transmitted to the server 
    # as well as a background I/O thread that is responsible for turning these records into requests and transmitting them 
    # to the cluster.
    # bootstrap_servers – ‘host[:port]’ string (or list of ‘host[:port]’ strings) that the producer should contact to bootstrap 
    # initial cluster metadata. This does not have to be the full node list. It just needs to have at least one broker that will
    # respond to a Metadata API Request. Default port is 9092. If no servers are specified, will default to localhost:9092.
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            res = get_data()
            res = format_data(res)

            producer.send('users_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
