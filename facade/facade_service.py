from fastapi import FastAPI, APIRouter
import requests
import json
from kafka import KafkaProducer
import random


class FacadeService:
    def __init__(self):
        self.kafka_mq = KafkaProducer(
            bootstrap_servers = ['localhost:9092'],
            value_serializer = lambda x:json.dumps(x).encode('utf-8')
        )
        self.topics = ['service1', 'service2']

    def log_message(self, msg, url):
        response = requests.post(url, json=json.loads(msg.json()))
        return response

    def send_message(self, msg):
        topic = random.choice(self.topics)
        self.kafka_mq.send(topic, value = {'msg' : msg})

    def get_logged_messages(self, url):
        response = requests.get(url)
        return response
    
    def get_messages(self, url):
        response = requests.get(url)
        return response