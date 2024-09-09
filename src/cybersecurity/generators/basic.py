
from datetime import datetime
from pydantic import BaseModel, Field

import random
import socket
import struct

def ip_generator():
    return socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))

class BasicGenerator(BaseModel):
    timestamp: datetime = Field(default_factory=datetime.now)
    source_ip: str = Field(default_factory=ip_generator)
    destination_ip: str = Field(default_factory=ip_generator)
