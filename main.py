from typing import List
from aiokafka import AIOKafkaConsumer
import asyncio
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI()

KAFKA_BROKER = "34.204.166.243:9092"  
TOPICS = ["a", "b"]

messages = []  
active_connections: List[WebSocket] = []  


async def consume_from_kafka():
    """Continuously consume messages from Kafka and broadcast via WebSocket"""
    consumer = AIOKafkaConsumer(*TOPICS, bootstrap_servers=KAFKA_BROKER, group_id="fastapi-group")
    await consumer.start()
    try:
        async for msg in consumer:
            values = json.loads(msg.value.decode("utf-8"))
            topic = msg.topic
            data = {"topic": topic, "value": values}
            messages.append(data)
            print(f"📩 Received: {data}")
            await broadcast(data) 
    finally:
        await consumer.stop()


async def broadcast(data):
    """Send Kafka messages to all connected WebSocket clients"""
    disconnected_clients = []
    for ws in active_connections:
        try:
            await ws.send_json(data)
        except Exception:
            disconnected_clients.append(ws)
    
    # Remove disconnected clients
    for ws in disconnected_clients:
        active_connections.remove(ws)


@app.on_event("startup")
async def startup_event():
    """Start Kafka consumer on FastAPI startup"""
    asyncio.create_task(consume_from_kafka())


@app.get("/messages/")
async def get_messages():
    """Retrieve the latest Kafka messages"""
    return {"messages": messages}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time message updates"""
    await websocket.accept()
    active_connections.append(websocket)
    try:
        while True:
            await websocket.receive_text()  # Keep connection alive (can be ignored)
    except WebSocketDisconnect:
        active_connections.remove(websocket)
