from fastapi import WebSocket
from typing import Dict, Set
import asyncio

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []
        self.pending_acks: Dict[str, Set[WebSocket]] = {}

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

    async def broadcast_json_with_ack(self, message: dict, timeout: float = 10.0):
        import uuid
        message_id = str(uuid.uuid4())
        message['message_id'] = message_id

        self.pending_acks[message_id] = set(self.active_connections)

        for conn in self.active_connections:
            await conn.send_json(message)

        try:
            await asyncio.wait_for(self._wait_for_acks(message_id), timeout=timeout)
        except asyncio.TimeoutError:
            print(f"Timeout waiting for ACKs for message {message_id}")
        finally:
            self.pending_acks.pop(message_id, None)

    async def _wait_for_acks(self, message_id: str):
        while self.pending_acks[message_id]:
            await asyncio.sleep(0.1)

    def acknowledge(self, websocket: WebSocket, message_id: str):
        if message_id in self.pending_acks:
            self.pending_acks[message_id].discard(websocket)