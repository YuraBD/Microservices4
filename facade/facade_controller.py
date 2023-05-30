import uvicorn
from fastapi import FastAPI, APIRouter
from facade_service import FacadeService
import random
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from message import Message
from requests.exceptions import ConnectionError
from urllib3.exceptions import MaxRetryError

localhost = "127.0.0.1"
facade_port = 8001
logging_ports = [8003, 8004, 8005]
messages_ports = [8006, 8007]

class FacadeController:
    def __init__(self, facade_service: FacadeService):
        self.router = APIRouter()
        self.router.add_api_route("/", self.get_req, methods=["GET"])
        self.router.add_api_route("/", self.post_req, methods=["POST"])
        self.facade_service = facade_service

    async def get_req(self):
        success = False
        print("Logging: Connecting to random port...")
        while not success:
            try:
                logging_port = random.choice(logging_ports)
                logging_url = f"http://{localhost}:{logging_port}"
                logging_response = self.facade_service.get_logged_messages(logging_url)
                success = True
            except (ConnectionError, MaxRetryError) as e:
                pass

        print("Getting messages from messages service")
        messages_resposes = ""
        for messages_port in messages_ports:
            messages_url = f"http://{localhost}:{messages_port}"
            messages_response = self.facade_service.get_messages(messages_url)
            messages_resposes += ', ' + messages_response.json()

        text = "Logged messages: " +  logging_response.json() + " | " + "Messages service response: "  + messages_resposes
        return text

    async def post_req(self, msg: Message):
        if not msg.msg:
            return "Empty message"

        success = False
        print("Logging: Connecting to random port...")
        while not success:
           try:
               logging_port = random.choice(logging_ports)
               logging_url = f"http://{localhost}:{logging_port}"
               logging_response = self.facade_service.log_message(msg, logging_url)
               success = True
           except (ConnectionError, MaxRetryError) as e:
               pass
        
        if logging_response.json() == "Empty message":
           return "Empty message"

        print("Sending message")
        self.facade_service.send_message(msg.msg)

        return f"Message: '{msg.msg}' sent"


if __name__ == "__main__":
    facade_service = FacadeService()
    app = FastAPI() 
    facade_controller = FacadeController(facade_service) 
    app.include_router(facade_controller.router) 
    uvicorn.run(app, host=localhost, port=facade_port)