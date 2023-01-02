import time
import requests
from kafka import KafkaProducer
from json import dumps
import json


# The Ergast Developer API is an experimental web service which provides a historical record of motor racing data for non-commercial purposes.

ergast_dot_com_test_api_url = 'http://ergast.com/api/f1/2004/1/results.json'
kafka_topic_name = 'streaming-topic'
kafka_bootstrap_servers = 'localhost:9092'


if __name__ == "__main__":
    print("Kafka producer application Started..")
    kafka_producer_obj = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers, value_serializer=lambda x: dumps(x).encode('utf-8'))

    print("printing before while loop starts.")

    while True:
        # print("hi")
        try:
            stream_api_response = requests.get(
                ergast_dot_com_test_api_url, stream=True)
            if stream_api_response.status_code == 200:
                for api_response_message in stream_api_response.iter_lines():
                    print("Message Recieved:")
                    # print(api_response_message)
                    print(type(api_response_message))

                    api_response_message = json.loads(api_response_message)
                    print("Message to be sent:")

                    
                    print(type(api_response_message))
                    print(api_response_message)
                    # sending messages one by one to the consumer side (spark structure streaming application) to serialize properly we loop through messages
                    kafka_producer_obj.send(kafka_topic_name, api_response_message)
                    # putting sleep for every message as after making api call the message has to be published to the topic so lagging is needed otherwise it will not publish properly
                    time.sleep(1)

        except Exception as ex:
            print("Connection to ergast stream api cannot be published..")

    print("Printing after while loop is completed")
