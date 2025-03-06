from pydantic import BaseModel
from datetime import datetime
from typing import Dict, Any

class NetworkPacketBase(BaseModel):
    id: str
    timestamp: datetime
    order: int
    layers: Dict[str, Any] 

class NetworkPacketCreate(NetworkPacketBase):
    pass

class NetworkPacketResponse(NetworkPacketBase):
    class Config:
        from_attributes = True
