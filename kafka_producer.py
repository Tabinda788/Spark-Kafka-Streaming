import time
import requests
from kafka import KafkaProducer
from json import dumps
import json

url = 'https://projects.propublica.org/nonprofits/api/v2/search.json?order=revenue&sort_order=desc'

KAFKA_TOPIC_NAME_CONS = "test-topic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'



if __name__ == "__main__":
    print("Kafka producer application Started..")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                        value_serializer=lambda x: dumps(x).encode('utf-8'))
    
    print("printing before while loop starts.")
    while True:
        try:
            stream_api_response = requests.get(
                    url, stream=True)
            if stream_api_response.status_code == 200:
                for api_response_message in stream_api_response.iter_lines():
                    print("Message Recieved:")
                    api_response_message = json.loads(api_response_message)

                organizations = api_response_message["organizations"]
                for organinsation in organizations:
                    print("Message to be send:",organinsation)
                    kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, organinsation)
                    time.sleep(1)
        except Exception as ex:
            print("Connection to ergast stream api cannot be published..")

    print("Printing after while loop is completed")


                
                # print(city)
                            