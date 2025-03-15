from aiokafka import AIOKafkaConsumer
import asyncio
import json
from fastapi import FastAPI, Form, WebSocket, WebSocketDisconnect, HTTPException, BackgroundTasks
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from websocket import ConnectionManager
from subscription import send_email
from crud import *
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta

load_dotenv(override=True)
app = FastAPI()
templates = Jinja2Templates(directory="templates")

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPICS = ["network-traffic", "intrusion", "network-flows"]

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
    packets = get_network_packets_for_flow(flow.timestamp, flow.timestamp + timedelta(seconds=flow.duration//1_000_000), flow.srcIp, flow.srcPort, flow.dstIp, flow.dstPort)
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
                if values["prediction"] != "Benign":
                    emails_tuple = get_email_subscriptions()
                    email = [e[0] for e in emails_tuple]
                    body = {
                        "prediction": values["prediction"],
                        "fid": values["fid"],
                        "timestamp": datetime.now(),
                        "srcIp": values["fid"].split("-")[0],
                        "srcPort": values["fid"].split("-")[2],
                        "dstIp": values["fid"].split("-")[1],
                        "dstPort": values["fid"].split("-")[3]
                    }
                    asyncio.create_task(send_email({"email":email,"body": body}))
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
        create_email_subscription(email)
        return {"message": "Subscribed successfully!"}
    except Exception as e:
        if str(e).find("UniqueViolation") != -1:
            raise HTTPException(status_code=400, detail="Email already subscribed!")
        else:
            raise HTTPException(status_code=400, detail=str(e))

@app.delete('/unsubscribe/{email}')
async def unsubscribe(email: str = None):
    try:
        delete_email_subscription(email)
        return {"message": "Unsubscribed successfully!"}
    except Exception as e:
        if str(e).find("Email not found") != -1:
            raise HTTPException(status_code=404, detail="Email not found!")
        else:
            raise HTTPException(status_code=400, detail=str(e)) 