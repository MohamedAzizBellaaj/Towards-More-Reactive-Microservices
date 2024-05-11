import json

import pika


def main():
    with pika.BlockingConnection(pika.ConnectionParameters("localhost")) as connection:
        channel = connection.channel()


        def receive_commercial_service_request(ch, method, properties, body):
            data = {"from": "risk_management_service", "to": "ocr_service", "msg": "Data request"}
            
            body = json.loads(body)
            print(
                f"{data["from"]}: {body["msg"]} received. Requesting data from {data["to"]}"
            )

            data_json = json.dumps(data)

            channel.basic_publish(
                exchange="", routing_key="topic6", body=data_json
            )

        def receive_ocr_data(ch, method, properties, body):
            data = {"from": "risk_management_service", "to": "credit_service", "msg": "Credit approved!"}
        
            body = json.loads(body)
            print(f"{data["from"]}: {body["msg"]} Received. Processing...")
            
            print(
                f"{data["from"]}: {data["msg"]} -> {data["to"]}"
            )       

        channel.basic_consume(
            queue="topic2", on_message_callback=receive_commercial_service_request, auto_ack=True
        )
        channel.basic_consume(
            queue="topic7", on_message_callback=receive_ocr_data, auto_ack=True
        )

        print("Risk Management Service: Waiting for requests...")
        channel.start_consuming()


if __name__ == "__main__":
    main()
