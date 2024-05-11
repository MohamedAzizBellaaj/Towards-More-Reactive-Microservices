import json

import pika


def main():
    with pika.BlockingConnection(pika.ConnectionParameters("localhost")) as connection:
        channel = connection.channel()


        def receive_credit_request(ch, method, properties, body):
            data = {"from": "commercial_service", "to": "ocr_service", "msg": "Data request"}
            
            body = json.loads(body)
            print(
                f"{data["from"]}: {body["msg"]} received. Requesting data from {data["to"]}"
            )

            data_json = json.dumps(data)

            channel.basic_publish(
                exchange="", routing_key="topic4", body=data_json
            )

        def receive_ocr_data(ch, method, properties, body):
            data = {"from": "commercial_service", "to": "risk_management_service", "msg": "Forward credit request"}
        
            body = json.loads(body)
            print(f"{data["from"]}: {body["msg"]} Received. Processing...")
            
            print(
                f"{data["from"]}: {data["msg"]} -> {data["to"]}"
            )

            data_json = json.dumps(data)

            channel.basic_publish(
                exchange="", routing_key="topic2", body=data_json
            )
                    

        channel.basic_consume(
            queue="topic1", on_message_callback=receive_credit_request, auto_ack=True
        )
        channel.basic_consume(
            queue="topic5", on_message_callback=receive_ocr_data, auto_ack=True
        )

        print("Commercial Service: Waiting for requests...")
        channel.start_consuming()


if __name__ == "__main__":
    main()
