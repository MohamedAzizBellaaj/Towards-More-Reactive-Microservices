import json

import pika


def main():
    with pika.BlockingConnection(pika.ConnectionParameters("localhost")) as connection:
        channel = connection.channel()


        def receive_commercial_service_request(ch, method, properties, body):
            data = {"from": "ocr_service", "to": "commercial_service", "msg": "Forward data"}
            
            body = json.loads(body)
            
            print(
                f"{data["from"]}: {body["msg"]} received. Requesting data from DFS"
            )
            print(
                f"{data["from"]}: {data["msg"]} -> {data["to"]}"
            )

            data_json = json.dumps(data)

            channel.basic_publish(
                exchange="", routing_key="topic5", body=data_json
            )

        def receive_risk_management_service_request(ch, method, properties, body):
            data = {"from": "ocr_service", "to": "risk_management_service", "msg": "Forward data"}
        
            body = json.loads(body)
            
            print(
                f"{data["from"]}: {body["msg"]} received. Requesting data from DFS"
            )
            print(
                f"{data["from"]}: {data["msg"]} -> {data["to"]}"
            )

            data_json = json.dumps(data)

            channel.basic_publish(
                exchange="", routing_key="topic7", body=data_json
            )
                    

        channel.basic_consume(
            queue="topic4", on_message_callback=receive_commercial_service_request, auto_ack=True
        )
        channel.basic_consume(
            queue="topic6", on_message_callback=receive_risk_management_service_request, auto_ack=True
        )

        print("OCR service: Waiting for requests...")
        channel.start_consuming()


if __name__ == "__main__":
    main()
