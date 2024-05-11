import json

import pika


def main():
    with pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost")
    ) as connection:
        channel = connection.channel()
        
        #? Creates the necessary queues needed for the other services to work 
        for i in range(7):
            channel.queue_declare(queue=f"topic{i+1}")
            
        data = {
            "from": "banking_app",
            "to": "commercial_service",
            "msg": "Credit request",
        }
        

        data_json = json.dumps(data)

        channel.basic_publish(exchange="", routing_key="topic1", body=data_json)
        print(f"{data["from"]}: {data["msg"]} -> {data["to"]}")


if __name__ == "__main__":
    main()
