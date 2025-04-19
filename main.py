from aiokafka import AIOKafkaConsumer
import asyncio
from asyncio import Queue
import json
from fastapi import FastAPI, Form, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from websocket.websocket import ConnectionManager
from service.subscription import send_email
from service.intrusions import *
from service.flows import *
from service.packets import *
from service.email import *
from dotenv import load_dotenv
import os
import uuid
import datetime
from fastapi.responses import JSONResponse
from datetime import datetime, timezone
load_dotenv(override=True)
app = FastAPI()
templates = Jinja2Templates(directory="templates")

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPICS = ["network-traffic", "DOS","PORT_SCAN", "network-flows"]

intrusion_queue = Queue()
packets_queue = Queue()
flows_queue = Queue()
email_queue = Queue()
broadcast_queue = Queue()

BATCH_SIZE = 1000
BATCH_INTERVAL = 1  # seconds

manager = ConnectionManager()

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(intrusion_worker())
    asyncio.create_task(consume_from_kafka())
    asyncio.create_task(packets_worker())
    asyncio.create_task(flows_worker())


@app.get("/")
async def home(request: Request):
    return templates.TemplateResponse("intrusion-list.html", {"request": request})

@app.get("/flows")
async def flows(request: Request):
    flows = await get_network_flows()
    return JSONResponse(content=flows)

@app.get("/packets")
async def packets(request: Request):
    packets = await get_network_packets()
    return JSONResponse(content=packets)
    

@app.get("/intrusions")
async def intrusions(request: Request, detected: bool = None):
    intrusions = await get_all_intrusion_results(detected=detected)
    return JSONResponse(content=intrusions)

@app.get("/intrusions/delete")
async def delete_intrusions(request: Request):
    await delete_all_intrusion_results()
    return {"message": "Intrusions deleted successfully!"}

async def consume_from_kafka():
    consumer = AIOKafkaConsumer(*TOPICS, 
                                bootstrap_servers=KAFKA_BROKER, 
                                group_id=f"fastapi-group-{uuid.uuid4()}",
                                auto_offset_reset="earliest")
    await consumer.start()
    try:
        async for msg in consumer:
            values = json.loads(msg.value.decode("utf-8"))
            topic = msg.topic
            if topic == "network-traffic":
                values['timestamp'] = int(values['timestamp']) // 1_000_000
                values['sniff_time'] = int(values['sniff_time']) // 1_000_000
                await packets_queue.put((values, topic))
            elif topic == "network-flows":
                await flows_queue.put((values, topic))
            else:
                values['TIMESTAMP_START'] = int(values['TIMESTAMP_START']) // 1_000_000
                values['TIMESTAMP_END'] = int(values['TIMESTAMP_END']) // 1_000_000
                values['SNIFF_TIMESTAMP_START'] = int(values['SNIFF_TIMESTAMP_START']) // 1_000_000
                await intrusion_queue.put((values, topic))
    finally:
        await consumer.stop()


async def broadcast(data):
    try:
        await manager.broadcast_json(data)
    except Exception as e:
        print(f"⚠️ WebSocket Error: {e}")


@app.websocket("/websocket")
async def websocket_endpoint(websocket: WebSocket):
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

async def packets_worker():
    buffer = []
    while True:
        try:
            item = await asyncio.wait_for(packets_queue.get(), timeout=BATCH_INTERVAL)
            buffer.append(item)
            
            if len(buffer) >= BATCH_SIZE:
                await flush_packets(buffer)
                buffer = []
        except asyncio.TimeoutError:
            if buffer:
                await flush_packets(buffer)
                buffer = []

async def flows_worker():
    buffer = []
    while True:
        try:
            item = await asyncio.wait_for(flows_queue.get(), timeout=BATCH_INTERVAL)
            buffer.append(item)

            if len(buffer) >= BATCH_SIZE:
                await flush_flows(buffer)
                buffer = []
        except asyncio.TimeoutError:
            if buffer:
                await flush_flows(buffer)
                buffer = []

async def flush_intrusions(buffer):
    docs = []
    broadcasts_dos = None
    broadcasts_port_scan = None
    email_tasks = []
    pkt_cnt = 0
    intrusion_cnt = 0
    dos_cnt = 0
    port_scan_cnt = 0
    for values, topic in buffer:
        data = values.copy()
        data["topic"] = topic
        docs.append(data)
        pkt_cnt += 1
        if data["STATUS"] != "NOT DETECTED":
            intrusion_cnt += 1
            if data["topic"] == "DOS":
                dos_cnt += 1
                broadcasts_dos = {"topic": topic, "value": values}
            elif data["topic"] == "PORT_SCAN":
                port_scan_cnt += 1
                broadcasts_port_scan = {"topic": topic, "value": values}
            email_tasks.append(send_email(topic, values))

    if broadcasts_dos:
        await broadcast(broadcasts_dos)
    if broadcasts_port_scan:
        await broadcast(broadcasts_port_scan)

    await broadcast({"pkt_cnt": pkt_cnt, 
                     "intrusion_cnt": intrusion_cnt, 
                     "dos_cnt": dos_cnt, 
                     "port_scan_cnt": port_scan_cnt})

    asyncio.gather(*email_tasks)

    await create_intrusion_detection_batch(docs)

async def flush_flows(buffer):
    docs = []

    for values, _ in buffer:
        data = values.copy()
        docs.append(data)
    
    await create_network_flows_batch(docs)

async def flush_packets(buffer):
    docs = []
    
    for values, _ in buffer:
        data = values.copy()
        docs.append(data)

    await create_network_packets_batch(docs)