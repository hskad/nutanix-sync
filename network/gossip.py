import time
import random
import asyncio
import logging
from typing import Dict, List, Optional, Callable
from network.protocol import PeerInfo, SyncMessage, MessageType
from network.transport import AsyncTCPTransport

logger = logging.getLogger("Gossip")

class GossipManager:
    """
    SWIM/Epidemic Gossip Protocol manager.
    Maintains dynamic cluster membership, detects node failures,
    and gossips Merkle root hashes for eventual consistency across 100+ nodes.
    """
    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        get_root_hash_fn: Callable[[], str],
        on_divergence_cb: Optional[Callable[[PeerInfo], None]] = None,
        ping_interval: float = 1.5,
        fanout: int = 3
    ):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.get_root_hash_fn = get_root_hash_fn
        self.on_divergence_cb = on_divergence_cb
        self.ping_interval = ping_interval
        self.fanout = fanout

        self.peers: Dict[str, PeerInfo] = {}
        self._running = False
        self._gossip_task: Optional[asyncio.Task] = None

    def add_peer(self, peer: PeerInfo):
        if peer.node_id == self.node_id:
            return
        self.peers[peer.node_id] = peer

    def get_peer(self, node_id: str) -> Optional[PeerInfo]:
        return self.peers.get(node_id)

    def get_alive_peers(self) -> List[PeerInfo]:
        return [p for p in self.peers.values() if p.status == "ALIVE"]

    def start(self):
        self._running = True
        self._gossip_task = asyncio.create_task(self._gossip_loop())

    async def stop(self):
        self._running = False
        if self._gossip_task:
            self._gossip_task.cancel()
            try:
                await self._gossip_task
            except asyncio.CancelledError:
                pass

    def handle_ping(self, message: SyncMessage) -> SyncMessage:
        """Processes incoming gossip ping and returns pong."""
        sender_id = message.sender_id
        payload = message.payload
        remote_root = payload.get("root_hash", "")
        remote_host = payload.get("host", "127.0.0.1")
        remote_port = payload.get("port", 0)

        # Update sender peer info
        self.peers[sender_id] = PeerInfo(
            node_id=sender_id,
            host=remote_host,
            port=remote_port,
            root_hash=remote_root,
            last_seen=time.time(),
            status="ALIVE",
            files_count=payload.get("files_count", 0)
        )

        # Merge peer list gossiped by sender
        gossiped_peers = payload.get("peers", [])
        for p_data in gossiped_peers:
            p_id = p_data.get("node_id")
            if p_id and p_id != self.node_id and p_id not in self.peers:
                self.peers[p_id] = PeerInfo(
                    node_id=p_id,
                    host=p_data["host"],
                    port=p_data["port"],
                    root_hash=p_data.get("root_hash", ""),
                    last_seen=time.time(),
                    status="ALIVE"
                )

        # Check for state divergence
        local_root = self.get_root_hash_fn()
        if remote_root and remote_root != local_root and self.on_divergence_cb:
            self.on_divergence_cb(self.peers[sender_id])

        # Return pong response
        sample_peers = [p.to_dict() for p in list(self.peers.values())[:5]]
        return SyncMessage(
            msg_type=MessageType.GOSSIP_ACK,
            sender_id=self.node_id,
            payload={
                "root_hash": local_root,
                "status": "ALIVE",
                "peers": sample_peers
            },
            timestamp=time.time()
        )

    async def _gossip_loop(self):
        while self._running:
            try:
                await asyncio.sleep(self.ping_interval)
                await self._ping_random_peers()
                self._check_dead_nodes()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"[{self.node_id}] Gossip loop error: {e}")

    async def _ping_random_peers(self):
        if not self.peers:
            return

        candidate_ids = list(self.peers.keys())
        sample_count = min(self.fanout, len(candidate_ids))
        selected_ids = random.sample(candidate_ids, sample_count)

        local_root = self.get_root_hash_fn()
        sample_peers = [p.to_dict() for p in list(self.peers.values())[:6]]

        ping_msg = SyncMessage(
            msg_type=MessageType.GOSSIP_PING,
            sender_id=self.node_id,
            payload={
                "host": self.host,
                "port": self.port,
                "root_hash": local_root,
                "peers": sample_peers
            },
            timestamp=time.time()
        )

        for peer_id in selected_ids:
            peer = self.peers[peer_id]
            resp = await AsyncTCPTransport.send_message(peer.host, peer.port, ping_msg, timeout=1.5)
            if resp and resp.msg_type == MessageType.GOSSIP_ACK:
                peer.last_seen = time.time()
                peer.status = "ALIVE"
                peer.root_hash = resp.payload.get("root_hash", "")
                
                # Check for hash divergence
                if peer.root_hash and peer.root_hash != local_root and self.on_divergence_cb:
                    self.on_divergence_cb(peer)
            else:
                # Node didn't respond
                if time.time() - peer.last_seen > 6.0:
                    peer.status = "DEAD"
                elif time.time() - peer.last_seen > 3.0:
                    peer.status = "SUSPECT"

    def _check_dead_nodes(self):
        now = time.time()
        for peer in self.peers.values():
            if now - peer.last_seen > 10.0:
                peer.status = "DEAD"
