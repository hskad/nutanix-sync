import base64
import asyncio
from typing import Dict, Set, List, Optional, Tuple
from network.protocol import PeerInfo, SyncMessage, MessageType
from network.transport import AsyncTCPTransport
from engine.chunker import ChunkInfo

class SwarmManager:
    """
    Manages distributed chunk availability and parallel multi-peer chunk fetching.
    Prevents single-node egress bottlenecks across large clusters.
    """
    def __init__(self, node_id: str):
        self.node_id = node_id
        # chunk_hash -> set of node_ids known to have this chunk
        self.chunk_holders: Dict[str, Set[str]] = {}
        
        # Metrics
        self.total_bytes_transferred: int = 0
        self.total_bytes_saved: int = 0
        self.chunks_transferred_count: int = 0

    def register_chunk_holder(self, chunk_hash: str, node_id: str):
        if chunk_hash not in self.chunk_holders:
            self.chunk_holders[chunk_hash] = set()
        self.chunk_holders[chunk_hash].add(node_id)

    def register_file_chunks(self, chunks: List[ChunkInfo], node_id: str):
        for c in chunks:
            self.register_chunk_holder(c.hash, node_id)

    async def fetch_chunk(
        self,
        chunk_hash: str,
        rel_path: str,
        peer_candidates: List[PeerInfo]
    ) -> Optional[bytes]:
        """
        Fetches a chunk from one of the available peers holding it,
        prioritizing known chunk holders.
        """
        if not peer_candidates:
            return None

        # Prefer peers recorded as holding this chunk
        known_holders = self.chunk_holders.get(chunk_hash, set())
        preferred = [p for p in peer_candidates if p.node_id in known_holders and p.status == "ALIVE"]
        others = [p for p in peer_candidates if p.node_id not in known_holders and p.status == "ALIVE"]
        ordered_peers = preferred + others

        req_msg = SyncMessage(
            msg_type=MessageType.CHUNK_REQ,
            sender_id=self.node_id,
            payload={
                "chunk_hash": chunk_hash,
                "rel_path": rel_path
            },
            timestamp=asyncio.get_event_loop().time()
        )

        for peer in ordered_peers:
            try:
                resp = await AsyncTCPTransport.send_message(peer.host, peer.port, req_msg, timeout=3.0)
                if resp and resp.msg_type == MessageType.CHUNK_RESP:
                    payload = resp.payload
                    if payload.get("found"):
                        b64_data = payload.get("data", "")
                        chunk_bytes = base64.b64decode(b64_data)
                        
                        # Update metrics
                        self.total_bytes_transferred += len(chunk_bytes)
                        self.chunks_transferred_count += 1
                        self.register_chunk_holder(chunk_hash, peer.node_id)
                        self.register_chunk_holder(chunk_hash, self.node_id)
                        return chunk_bytes
            except Exception:
                continue

        return None

    async def fetch_chunks_parallel(
        self,
        needed_chunk_hashes: List[str],
        rel_path: str,
        peers: List[PeerInfo]
    ) -> Dict[str, bytes]:
        """
        Fetches multiple missing chunks in parallel across available cluster peers.
        """
        chunk_map: Dict[str, bytes] = {}
        if not needed_chunk_hashes:
            return chunk_map

        tasks = []
        for ch_hash in needed_chunk_hashes:
            tasks.append(self.fetch_chunk(ch_hash, rel_path, peers))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for ch_hash, res in zip(needed_chunk_hashes, results):
            if isinstance(res, bytes):
                chunk_map[ch_hash] = res
            else:
                print(f"[{self.node_id}] Failed to fetch chunk {ch_hash}: {res}")

        return chunk_map
