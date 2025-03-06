from aiokafka import AIOKafkaConsumer
import asyncio
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from crud import *
from dotenv import load_dotenv
import os

load_dotenv()
app = FastAPI()
templates = Jinja2Templates(directory="templates")

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPICS = ["traffics", "intrusion", "network-flows"]

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)
    
    async def broadcast_json(self, message: str):
        for connection in self.active_connections:
            await connection.send_json(message)


manager = ConnectionManager()

@app.on_event("startup")
async def startup_event():
    """Start Kafka consumer on FastAPI startup"""
    asyncio.create_task(consume_from_kafka())

@app.get("/")
async def home(request: Request):
    packets = get_network_packets(10)
    intrusions = get_intrusion_detection_results(10)
    return templates.TemplateResponse("index.html", {"request": request, "packets": packets, "intrusions": intrusions})

@app.get("/intrusion")
async def home(request: Request):
    packets = get_network_packets(10)
    
    return templates.TemplateResponse("intrusion.html", {"request": request, "packets": packets})

async def consume_from_kafka():
    consumer = AIOKafkaConsumer(*TOPICS, bootstrap_servers=KAFKA_BROKER, group_id="fastapi-group")
    await consumer.start()
    try:
        async for msg in consumer:
            values = json.loads(msg.value.decode("utf-8"))
            topic = msg.topic
            data = {"topic": topic, "value": values}
            if topic == "traffics":
                create_network_packet(values)
            elif topic == "intrusion":
                create_intrusion_detection_result(values)
            # print(f"📩 Received: {data}")
            await broadcast(data) 
    finally:
        await consumer.stop()


async def broadcast(data):
    """Send Kafka messages to all connected WebSocket clients"""
    try:
        # print("data to be send:", data)
        # print('sending...')
        await manager.broadcast_json(data)
        # print('sended...')

    except Exception as e:
        print(f"⚠️ WebSocket Error: {e}")


@app.websocket("/websocket")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time message updates"""
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text() 
    except WebSocketDisconnect:
        manager.disconnect(websocket)