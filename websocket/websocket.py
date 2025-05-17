from fastapi import WebSocket
from typing import Dict, Set
import asyncio
import uuid

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []
        self.pending_acks: Dict[str, Set[WebSocket]] = {}

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        """
        Broadcasts a message to all connected WebSocket clients.

        Args:
            message (str): The message to broadcast to all connected WebSocket clients.
        """
        for connection in self.active_connections:
            await connection.send_text(message)

    async def broadcast_json_with_ack(self, message: dict, timeout: float = 10.0):
        """
        Broadcasts a JSON message to all connected WebSocket clients and waits for acknowledgments from all clients.

        This asynchronous function sends a JSON message with a unique message ID to each connected client.
        It waits for acknowledgment (ACK) messages from all clients or until the specified timeout is reached.
        If the timeout occurs before all acknowledgments are received, it prints a timeout message.

        Args:
            message (dict): The JSON message to broadcast. A unique 'message_id' is added to this message.
            timeout (float, optional): The maximum time to wait for acknowledgments from all clients. Defaults to 10.0 seconds.
        """
        message_id = str(uuid.uuid4())
        message['message_id'] = message_id

        self.pending_acks[message_id] = set(self.active_connections)

        for conn in self.active_connections:
            await conn.send_json(message)

        try:
            await asyncio.wait_for(self.wait_for_acks(message_id), timeout=timeout)
        except asyncio.TimeoutError:
            print(f"Timeout waiting for ACKs for message {message_id}")
        finally:
            self.pending_acks.pop(message_id, None)

    async def wait_for_acks(self, message_id: str):
        """
        Waits for all pending ACKs for the given message ID to be received from all connected WebSocket clients.

        This asynchronous function blocks until all pending ACKs for the given message ID are received from all connected WebSocket clients.
        It checks every 0.1 seconds if all ACKs have been received.

        Args:
            message_id (str): The message ID to wait for ACKs for.
        """
        while self.pending_acks[message_id]:
            await asyncio.sleep(0.1)

    def acknowledge(self, websocket: WebSocket, message_id: str):
        """
        Acknowledges a message with the given message ID from the given WebSocket client.

        This method is used to acknowledge a message with the given message ID from the given WebSocket client.
        It removes the given WebSocket client from the set of pending ACKs for the given message ID.

        Args:
            websocket (WebSocket): The WebSocket client sending the ACK message.
            message_id (str): The message ID of the message being acknowledged.
        """
        if message_id in self.pending_acks:
            self.pending_acks[message_id].discard(websocket)