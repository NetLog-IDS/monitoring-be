from aiokafka import AIOKafkaConsumer
import asyncio
from asyncio import Queue
import json
from fastapi import FastAPI, Form, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from websocket.websocket import ConnectionManager
from service.subscription import send_email
from service.intrusions import create_intrusion_detection_batch, get_intrusion_detection_results
from service.flows import *
from service.packets import *
from service.email import *
from dotenv import load_dotenv
import os
import time

load_dotenv(override=True)
app = FastAPI()
templates = Jinja2Templates(directory="templates")

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPICS = ["network-traffic", "DOS","PORT_SCAN", "network-flows"]

intrusion_queue = Queue()

BATCH_SIZE = 50
BATCH_INTERVAL = 0.1  # seconds

manager = ConnectionManager()

@app.on_event("startup")
async def startup_event():
    """Start Kafka consumer on FastAPI startup"""
    asyncio.create_task(intrusion_worker())
    asyncio.create_task(consume_from_kafka())

@app.get("/")
async def home(request: Request):
    print("Request")
    intrusions = await get_intrusion_detection_results(10)
    return templates.TemplateResponse("intrusion-list.html", {"request": request, "intrusions": intrusions})

async def consume_from_kafka():
    consumer = AIOKafkaConsumer(*TOPICS, bootstrap_servers=KAFKA_BROKER, group_id="fastapi-group")
    await consumer.start()
    try:
        async for msg in consumer:
            values = json.loads(msg.value.decode("utf-8"))
            topic = msg.topic
            data = {"topic": topic, "value": values}
            if topic == "network-traffic":
                values['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(values['timestamp']) // 1_000_000))
                await create_network_packet(values)
            elif topic == "network-flows":
                await create_network_flows(values)
            else:
                values['TIMESTAMP_START'] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(values['TIMESTAMP_START']) // 1_000_000))
                values['TIMESTAMP_END'] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(values['TIMESTAMP_END']) // 1_000_000))
                await intrusion_queue.put((values, topic))
    finally:
        await consumer.stop()


async def broadcast(data):
    """Send Kafka messages to all connected WebSocket clients"""
    try:
        await manager.broadcast_json(data)
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

@app.post('/subscribe')
async def subscribe(email: str = Form(...)):
    try:
        await create_email_subscription(email)
        return {"message": "Subscribed successfully!"}
    except Exception as e:
        if str(e).find("UniqueViolation") != -1:
            raise HTTPException(status_code=400, detail="Email already subscribed!")
        else:
            raise HTTPException(status_code=400, detail=str(e))

@app.delete('/unsubscribe/{email}')
async def unsubscribe(email: str = None):
    try:
        await delete_email_subscription(email)
        return {"message": "Unsubscribed successfully!"}
    except Exception as e:
        if str(e).find("Email not found") != -1:
            raise HTTPException(status_code=404, detail="Email not found!")
        else:
            raise HTTPException(status_code=400, detail=str(e)) 
        
async def intrusion_worker():
    buffer = []
    while True:
        try:
            item = await asyncio.wait_for(intrusion_queue.get(), timeout=BATCH_INTERVAL)
            buffer.append(item)
            
            if len(buffer) >= BATCH_SIZE:
                await flush_intrusions(buffer)
                buffer = []
                
        except asyncio.TimeoutError:
            if buffer:
                await flush_intrusions(buffer)
                buffer = []

async def intrusion_worker():
    buffer = []
    while True:
        try:
            item = await asyncio.wait_for(intrusion_queue.get(), timeout=BATCH_INTERVAL)
            buffer.append(item)
            
            if len(buffer) >= BATCH_SIZE:
                await flush_intrusions(buffer)
                buffer = []
                
        except asyncio.TimeoutError:
            if buffer:
                await flush_intrusions(buffer)
                buffer = []

async def intrusion_worker():
    buffer = []
    while True:
        try:
            item = await asyncio.wait_for(intrusion_queue.get(), timeout=BATCH_INTERVAL)
            buffer.append(item)
            
            if len(buffer) >= BATCH_SIZE:
                await flush_intrusions(buffer)
                buffer = []
                
        except asyncio.TimeoutError:
            if buffer:
                await flush_intrusions(buffer)
                buffer = []


async def flush_intrusions(buffer):
    docs = []
    broadcasts = []
    for values, topic in buffer:
        data = values.copy()
        data["topic"] = topic
        docs.append(data)
        broadcasts.append({"topic": topic, "value": values})
    
    await create_intrusion_detection_batch(docs)
    
    await asyncio.gather(*[broadcast(b) for b in broadcasts])
    
    email_tasks = []
    for values, topic in buffer:
        if values["STATUS"] != "NOT DETECTED":
            email_tasks.append(send_email(topic, values))
    asyncio.gather(*email_tasks)