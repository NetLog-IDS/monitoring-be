from aiokafka import AIOKafkaConsumer
import asyncio
from asyncio import Queue
import json
from fastapi import FastAPI, Form, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from websocket.websocket import ConnectionManager
from service.subscription import send_email
from service.dos import *
from service.portscan import *
from service.flows import *
from service.packets import *
from service.email import *
from dotenv import load_dotenv
import os
import uuid
from fastapi.responses import JSONResponse
load_dotenv(override=True)
app = FastAPI()
templates = Jinja2Templates(directory="templates")

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPICS = ["network-traffic", "DOS","PORT_SCAN", "network-flows"]

# Buffers
dos_intrusion_queue = Queue()
portscan_intrusion_queue = Queue()
packets_queue = Queue()
flows_queue = Queue()
email_queue = Queue()
broadcast_queue = Queue()

# Batch sizes and intervals for buffering
BATCH_SIZE_INTRUSIONS = 1000
BATCH_SIZE_PACKETS = 100000
BATCH_SIZE_FLOWS = 10000
BATCH_INTERVAL = 0.1  # seconds

# WebSocket connection manager
manager = ConnectionManager()

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(dos_intrusion_worker())
    asyncio.create_task(portscan_intrusion_worker())
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
    dos = await get_all_dos_intrusion_results(detected=detected)
    portscan = await get_all_portscan_intrusion_results(detected=detected)
    intrusions = {
        "DOS": dos,
        "PORT_SCAN": portscan
    }
    return JSONResponse(content=intrusions)

@app.get("/network-traffic")
async def get_network_traffic(request: Request, detected: bool = None):
    traffics = await get_network_packets()
    return JSONResponse(content=traffics)

@app.get("/network-flows")
async def get_network_flow(request: Request, detected: bool = None):
    flows = await get_network_flows()
    return JSONResponse(content=flows)

# WebSocket
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

# Email Subscription
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

# Kafka related
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
                await packets_queue.put(values)
            elif topic == "network-flows":
                await flows_queue.put(values)
            elif topic == "DOS":
                values['TIMESTAMP_START'] = int(values['TIMESTAMP_START']) // 1_000_000
                values['TIMESTAMP_END'] = int(values['TIMESTAMP_END']) // 1_000_000
                values['SNIFF_TIMESTAMP_START'] = int(values['SNIFF_TIMESTAMP_START']) // 1_000_000
                await dos_intrusion_queue.put((values, topic))
            elif topic == "PORT_SCAN":
                values['TIMESTAMP_START'] = int(values['TIMESTAMP_START']) // 1_000_000
                values['TIMESTAMP_END'] = int(values['TIMESTAMP_END']) // 1_000_000
                values['SNIFF_TIMESTAMP_START'] = int(values['SNIFF_TIMESTAMP_START']) // 1_000_000
                await portscan_intrusion_queue.put((values, topic))
    finally:
        await consumer.stop()
        
# Worker for flushing from buffer
async def dos_intrusion_worker():
    buffer = []
    while True:
        try:
            item = await asyncio.wait_for(dos_intrusion_queue.get(), timeout=BATCH_INTERVAL)
            buffer.append(item)
            
            if len(buffer) >= BATCH_SIZE_INTRUSIONS:
                await flush_dos_intrusions(buffer)
                buffer = []
                
        except asyncio.TimeoutError:
            if buffer:
                await flush_dos_intrusions(buffer)
                buffer = []

async def portscan_intrusion_worker():
    buffer = []
    while True:
        try:
            item = await asyncio.wait_for(portscan_intrusion_queue.get(), timeout=BATCH_INTERVAL)
            buffer.append(item)
            
            if len(buffer) >= BATCH_SIZE_INTRUSIONS:
                await flush_portscan_intrusions(buffer)
                buffer = []
                
        except asyncio.TimeoutError:
            if buffer:
                await flush_portscan_intrusions(buffer)
                buffer = []
                

async def packets_worker():
    buffer = []
    while True:
        try:
            item = await asyncio.wait_for(packets_queue.get(), timeout=BATCH_INTERVAL)
            buffer.append(item)
            
            if len(buffer) >= BATCH_SIZE_PACKETS:
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

            if len(buffer) >= BATCH_SIZE_FLOWS:
                await flush_flows(buffer)
                buffer = []
        except asyncio.TimeoutError:
            if buffer:
                await flush_flows(buffer)
                buffer = []

# Flushing
async def flush_dos_intrusions(buffer):
    docs = []

    dos_timestamp_start = 99909990000
    dos_timestamp_end = -99909990000
    unique_dos_dst_ip = set()

    email_tasks = []

    pkt_cnt = 0
    intrusion_cnt = 0
    dos_cnt = 0

    for values, topic in buffer:
        data = values.copy()
        data["topic"] = topic
        docs.append(data)
        pkt_cnt += 1

        if data["STATUS"] != "NOT DETECTED":
            intrusion_cnt += 1

            dos_cnt += 1
            dos_timestamp_start = min(dos_timestamp_start, values["TIMESTAMP_START"])
            dos_timestamp_end = max(dos_timestamp_end, values["TIMESTAMP_END"])
            if values["IP_DST"] not in unique_dos_dst_ip:
                unique_dos_dst_ip.add(values["IP_DST"])

            email_tasks.append(send_email(topic, values))

    await broadcast({"pkt_cnt": pkt_cnt, 
                     "intrusion_cnt": intrusion_cnt, 
                     "dos_cnt": dos_cnt, 
                     "port_scan_cnt": 0})

    if(unique_dos_dst_ip.__len__() > 0):
        await broadcast({
            "topic": "DOS",
            "timestamp_start": dos_timestamp_start,
            "timestamp_end": dos_timestamp_end,
            "unique_ip": list(unique_dos_dst_ip)
        })

    asyncio.gather(*email_tasks)

    await create_dos_intrusion_detection_batch(docs)

async def flush_portscan_intrusions(buffer):
    docs = []

    port_scan_timestamp_start = 99909990000
    port_scan_timestamp_end = -99909990000
    unique_portscan_src_ip = set()

    email_tasks = []

    pkt_cnt = 0
    intrusion_cnt = 0
    port_scan_cnt = 0

    for values, topic in buffer:
        data = values.copy()
        docs.append(data)
        pkt_cnt += 1

        if data["STATUS"] != "NOT DETECTED":
            intrusion_cnt += 1

            port_scan_cnt += 1
            port_scan_timestamp_start = min(port_scan_timestamp_start, values["TIMESTAMP_START"])
            port_scan_timestamp_end = max(port_scan_timestamp_end, values["TIMESTAMP_END"])
            if values["IP_SRC"] not in unique_portscan_src_ip:
                unique_portscan_src_ip.add(values["IP_SRC"])

            email_tasks.append(send_email(topic, values))

    await broadcast({"pkt_cnt": pkt_cnt, 
                     "intrusion_cnt": intrusion_cnt, 
                     "dos_cnt": 0, 
                     "port_scan_cnt": port_scan_cnt})
    
    if(unique_portscan_src_ip.__len__() > 0):
        await broadcast({
            "topic": "PORT_SCAN",
            "timestamp_start": port_scan_timestamp_start,
            "timestamp_end": port_scan_timestamp_end,
            "unique_ip": list(unique_portscan_src_ip)
        })

    asyncio.gather(*email_tasks)

    await create_portscan_intrusion_detection_batch(docs)

async def flush_flows(buffer):
    await create_network_flows_batch(buffer)

async def flush_packets(buffer):
    await create_network_packets_batch(buffer)