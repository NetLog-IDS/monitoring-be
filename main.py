from aiokafka import AIOKafkaConsumer
import asyncio
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from crud import *
from dotenv import load_dotenv
import os

load_dotenv(override=True)
app = FastAPI()
templates = Jinja2Templates(directory="templates")

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPICS = ["network-traffic", "intrusion", "network-flows"]

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
    intrusions = get_intrusion_detection_results(10)
    return templates.TemplateResponse("intrusion-list.html", {"request": request, "intrusions": intrusions})

@app.get("/details/{flow_id}/{prediction}")
async def flow_details(request: Request, flow_id: str = None, prediction: str = None):
    flow = get_network_flows_with_fid(flow_id)
    if not flow:
       raise HTTPException(status_code=404, detail="Flow not found")
    packets = get_network_packets_for_flow(flow.timestamp - timedelta(minutes=3), flow.timestamp + timedelta(minutes=3), flow.srcIp, flow.srcPort, flow.dstIp, flow.dstPort)
    if not packets:
       raise HTTPException(status_code=404, detail="Packets not found")
    packets_as_dict = []
    for packet in packets:
        packet_dict = packet.__dict__
        print(packet_dict['timestamp'])
        packet_dict['timestamp'] = packet_dict['timestamp'].strftime("%d/%m/%Y %H:%M:%S")
        packet_dict.pop('_sa_instance_state', None)
        packets_as_dict.append(packet_dict)
    return templates.TemplateResponse("flow-details.html", {"request": request, "flow": flow.__dict__, "packets": packets_as_dict, "prediction": prediction})

async def consume_from_kafka():
    consumer = AIOKafkaConsumer(*TOPICS, bootstrap_servers=KAFKA_BROKER, group_id="fastapi-group")
    await consumer.start()
    try:
        async for msg in consumer:
            values = json.loads(msg.value.decode("utf-8"))
            topic = msg.topic
            data = {"topic": topic, "value": values}
            if topic == "network-traffic":
                create_network_packet(values)
            elif topic == "network-flows":
                create_network_flows(values)
            else:
                create_intrusion_detection_result(values)
                await broadcast(data) 
            # # print(f"📩 Received: {data}")
            # await broadcast(data) 
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