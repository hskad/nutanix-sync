import os
import shutil
import asyncio
import time
import logging
from typing import List, Dict, Optional, Any
from cluster_sim.node import SyncNode
from network.protocol import PeerInfo

logger = logging.getLogger("Orchestrator")

class ClusterOrchestrator:
    """
    Manages a cluster of 2 to 100+ simulated or real SyncNodes.
    Coordinates topology, gossip bootstrapping, chaos injection,
    and cluster-wide consistency monitoring.
    """
    def __init__(self, base_sandbox_dir: str = "./cluster_sandbox"):
        self.base_sandbox_dir = os.path.abspath(base_sandbox_dir)
        self.nodes: List[SyncNode] = []
        self.is_running = False
        self._sync_history: List[dict] = []

    async def initialize_cluster(self, node_count: int = 5, clean_existing: bool = True):
        """Spawns node_count SyncNodes and connects them via gossip mesh."""
        await self.shutdown_cluster()

        if clean_existing and os.path.exists(self.base_sandbox_dir):
            try:
                shutil.rmtree(self.base_sandbox_dir, ignore_errors=True)
            except Exception:
                pass

        os.makedirs(self.base_sandbox_dir, exist_ok=True)
        self.nodes = []

        logger.info(f"Booting cluster with {node_count} nodes...")

        # 1. Instantiate nodes
        for i in range(node_count):
            node_id = f"node-{i:02d}"
            sync_dir = os.path.join(self.base_sandbox_dir, node_id)
            node = SyncNode(
                node_id=node_id,
                sync_dir=sync_dir,
                host="127.0.0.1",
                port=0,  # dynamic OS port allocation
                enable_watcher=True
            )
            self.nodes.append(node)

        # 2. Start all nodes concurrently
        await asyncio.gather(*(node.start() for node in self.nodes))

        # 3. Bootstrap gossip topology (partial mesh: connect to 2 nearest neighbors)
        for i, node in enumerate(self.nodes):
            # Connect to neighbor i-1 and i+1 to form a ring, gossip will rapidly form full mesh
            neighbors = [
                self.nodes[(i - 1) % node_count],
                self.nodes[(i + 1) % node_count]
            ]
            if node_count > 4:
                # Add one random leap to ensure O(log N) diameter
                leap = self.nodes[(i + node_count // 2) % node_count]
                neighbors.append(leap)

            for neighbor in neighbors:
                if neighbor.node_id != node.node_id:
                    node.gossip.add_peer(PeerInfo(
                        node_id=neighbor.node_id,
                        host=neighbor.host,
                        port=neighbor.port,
                        root_hash=neighbor.get_root_hash(),
                        status="ALIVE"
                    ))

        self.is_running = True
        logger.info(f"Cluster with {node_count} nodes online!")

    async def shutdown_cluster(self):
        """Stops all running nodes."""
        if self.nodes:
            await asyncio.gather(*(node.stop() for node in self.nodes), return_exceptions=True)
            self.nodes = []
        self.is_running = False

    def get_node(self, node_index_or_id: Any) -> Optional[SyncNode]:
        if isinstance(node_index_or_id, int):
            if 0 <= node_index_or_id < len(self.nodes):
                return self.nodes[node_index_or_id]
        elif isinstance(node_index_or_id, str):
            for n in self.nodes:
                if n.node_id == node_index_or_id:
                    return n
        return None

    async def inject_file(self, node_index: int, rel_path: str, content: bytes) -> dict:
        """Writes a file to a specific node, kicking off cluster-wide sync."""
        node = self.get_node(node_index)
        if not node:
            raise ValueError(f"Node {node_index} not found")

        abs_path = os.path.join(node.sync_dir, rel_path)
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        with open(abs_path, "wb") as f:
            f.write(content)

        # Force immediate chunk & broadcast
        manifest = node.chunker.chunk_file(abs_path, rel_path)
        node.journal.record_local_mutation(rel_path, manifest, is_delete=False)
        node.swarm.register_file_chunks(manifest.chunks, node.node_id)
        await node._broadcast_local_mutation(rel_path, manifest, is_delete=False)

        return {
            "source_node": node.node_id,
            "rel_path": rel_path,
            "size": len(content),
            "chunks_count": len(manifest.chunks),
            "hash": manifest.full_hash
        }

    async def mutate_file_delta(self, node_index: int, rel_path: str, mutation_bytes: bytes, offset: int = 100) -> dict:
        """
        Mutates a small slice of a file to demonstrate delta-sync and bandwidth savings.
        """
        node = self.get_node(node_index)
        if not node:
            raise ValueError(f"Node {node_index} not found")

        abs_path = os.path.join(node.sync_dir, rel_path)
        if not os.path.exists(abs_path):
            raise FileNotFoundError(f"File {rel_path} does not exist on {node.node_id}")

        with open(abs_path, "r+b") as f:
            f.seek(offset)
            f.write(mutation_bytes)

        manifest = node.chunker.chunk_file(abs_path, rel_path)
        node.journal.record_local_mutation(rel_path, manifest, is_delete=False)
        node.swarm.register_file_chunks(manifest.chunks, node.node_id)
        await node._broadcast_local_mutation(rel_path, manifest, is_delete=False)

        return {
            "node_id": node.node_id,
            "rel_path": rel_path,
            "mutation_size": len(mutation_bytes),
            "new_hash": manifest.full_hash
        }

    async def inject_conflict(self, node_a_idx: int, node_b_idx: int, rel_path: str, content_a: bytes, content_b: bytes) -> dict:
        """
        Simulates concurrent conflicting writes on two nodes during partition.
        """
        node_a = self.get_node(node_a_idx)
        node_b = self.get_node(node_b_idx)
        if not node_a or not node_b:
            raise ValueError("Invalid node indices")

        abs_a = os.path.join(node_a.sync_dir, rel_path)
        abs_b = os.path.join(node_b.sync_dir, rel_path)
        os.makedirs(os.path.dirname(abs_a), exist_ok=True)
        os.makedirs(os.path.dirname(abs_b), exist_ok=True)

        with open(abs_a, "wb") as f: f.write(content_a)
        with open(abs_b, "wb") as f: f.write(content_b)

        manifest_a = node_a.chunker.chunk_file(abs_a, rel_path)
        manifest_b = node_b.chunker.chunk_file(abs_b, rel_path)

        node_a.journal.record_local_mutation(rel_path, manifest_a)
        node_b.journal.record_local_mutation(rel_path, manifest_b)

        # Cross-broadcast
        await node_a._broadcast_local_mutation(rel_path, manifest_a, is_delete=False)
        await node_b._broadcast_local_mutation(rel_path, manifest_b, is_delete=False)

        return {
            "status": "CONFLICT_INJECTED",
            "node_a": node_a.node_id,
            "node_b": node_b.node_id,
            "rel_path": rel_path
        }

    async def kill_node(self, node_index: int) -> dict:
        node = self.get_node(node_index)
        if node and node._running:
            await node.stop()
            return {"status": "KILLED", "node_id": node.node_id}
        return {"status": "ALREADY_OFFLINE"}

    async def revive_node(self, node_index: int) -> dict:
        node = self.get_node(node_index)
        if node and not node._running:
            await node.start()
            return {"status": "REVIVED", "node_id": node.node_id}
        return {"status": "ALREADY_ONLINE"}

    def get_cluster_state(self) -> dict:
        """Returns comprehensive cluster metrics, consistency check, and topology data."""
        nodes_data = []
        root_hashes = set()
        total_transferred = 0
        total_saved = 0
        total_conflicts = 0
        alive_count = 0

        for node in self.nodes:
            summary = node.get_status_summary()
            is_alive = node._running
            if is_alive:
                alive_count += 1
                if summary["root_hash"]:
                    root_hashes.add(summary["root_hash"])
            total_transferred += summary["bytes_transferred"]
            total_saved += summary["bytes_saved"]
            total_conflicts += summary["conflicts_count"]

            nodes_data.append({
                "node_id": node.node_id,
                "host": node.host,
                "port": node.port,
                "status": "ONLINE" if is_alive else "OFFLINE",
                "root_hash": summary["root_hash"],
                "root_hash_short": summary["root_hash_short"],
                "files_count": summary["files_count"],
                "conflicts_count": summary["conflicts_count"],
                "peers": summary["peers"],
                "bytes_transferred": summary["bytes_transferred"],
                "bytes_saved": summary["bytes_saved"],
                "recent_mutations": summary["recent_mutations"],
                "files": summary["files"],
                "conflicts": summary["conflicts"]
            })

        # Consistency analysis: all online nodes must have identical non-empty root hash
        online_root_hashes = [n["root_hash"] for n in nodes_data if n["status"] == "ONLINE" and n["root_hash"]]
        is_consistent = (len(online_root_hashes) == alive_count and len(set(online_root_hashes)) == 1) if alive_count > 0 else False
        bandwidth_saved_pct = 0.0
        total_gross = total_transferred + total_saved
        if total_gross > 0:
            bandwidth_saved_pct = round((total_saved / total_gross) * 100.0, 1)

        # Build topology links
        links = []
        for n_data in nodes_data:
            for p in n_data["peers"]:
                if p["status"] == "ALIVE":
                    links.append({"source": n_data["node_id"], "target": p["node_id"]})

        return {
            "total_nodes": len(self.nodes),
            "alive_nodes": alive_count,
            "offline_nodes": len(self.nodes) - alive_count,
            "is_consistent": is_consistent,
            "distinct_root_hashes": len(root_hashes),
            "total_bytes_transferred": total_transferred,
            "total_bytes_saved": total_saved,
            "bandwidth_saved_pct": bandwidth_saved_pct,
            "total_conflicts": total_conflicts,
            "nodes": nodes_data,
            "topology_links": links
        }
