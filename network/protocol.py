import json
from enum import Enum
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict

class MessageType(str, Enum):
    HANDSHAKE = "HANDSHAKE"
    GOSSIP_PING = "GOSSIP_PING"
    GOSSIP_ACK = "GOSSIP_ACK"
    MERKLE_SYNC_REQ = "MERKLE_SYNC_REQ"
    MERKLE_SYNC_RESP = "MERKLE_SYNC_RESP"
    CHUNK_REQ = "CHUNK_REQ"
    CHUNK_RESP = "CHUNK_RESP"
    MUTATION_BROADCAST = "MUTATION_BROADCAST"

@dataclass
class PeerInfo:
    node_id: str
    host: str
    port: int
    root_hash: str = ""
    last_seen: float = 0.0
    status: str = "ALIVE"  # ALIVE | SUSPECT | DEAD
    files_count: int = 0

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> 'PeerInfo':
        return cls(**data)


@dataclass
class SyncMessage:
    msg_type: MessageType
    sender_id: str
    payload: Dict[str, Any]
    timestamp: float

    def to_dict(self) -> dict:
        return {
            "msg_type": self.msg_type.value,
            "sender_id": self.sender_id,
            "payload": self.payload,
            "timestamp": self.timestamp
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict) -> 'SyncMessage':
        return cls(
            msg_type=MessageType(data["msg_type"]),
            sender_id=data["sender_id"],
            payload=data.get("payload", {}),
            timestamp=data.get("timestamp", 0.0)
        )

    @classmethod
    def from_json(cls, json_str: str) -> 'SyncMessage':
        return cls.from_dict(json.loads(json_str))
