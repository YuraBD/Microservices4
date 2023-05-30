from fastapi import FastAPI, APIRouter
import uvicorn
import argparse
from messages_repository import MessagesRepository
from messages_service import MessagesService
import json
from kafka import KafkaConsumer
import asyncio
from aiokafka import AIOKafkaConsumer

localhost = "127.0.0.1"

class MessagesController:
    def __init__(self, messages_service, kafka_topic):
        self.router = APIRouter()
        self.router.add_api_route("/", self.get_req, methods=["GET"])
        self.messages_service = messages_service
        self.kafka_topic = kafka_topic

    async def consume_messages(self):
        consumer = AIOKafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=[f'localhost:9092'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        await consumer.start()
        try:
            async for msg in consumer:
                print("Got message: ", msg.value['msg'])
                self.messages_service.add_message(msg.value['msg'])
        finally:
            await consumer.stop()

    async def post_req(self):
        message = message.value
        print(f'Received message: {message}')

    async def get_req(self):
        print("Getting messages...")
        msg_list = self.messages_service.get_messages()
        return msg_list


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Accept a port number as argument.")
    parser.add_argument("--port", type=int, required=True, help="Port number (8006 or 8007)")
    args = parser.parse_args()
    messages_port = args.port
    if messages_port in [8006, 8007]:
        print(f"Using port {messages_port}")
    else:
        print("Invalid port number. Please use 8004, 8005, or 8006.")
        exit()

    if messages_port == 8006:
        topic = 'service1'
    else:
        topic = 'service2'

    messages_repository = MessagesRepository()
    messages_service = MessagesService(messages_repository)
    app = FastAPI()
    messages_controller = MessagesController(messages_service, topic)
    app.include_router(messages_controller.router) 

    @app.on_event("startup")
    async def startup_event():
        asyncio.create_task(messages_controller.consume_messages())

    uvicorn.run(app, host=localhost, port=messages_port)
