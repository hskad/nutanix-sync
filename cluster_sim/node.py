import os
import time
import base64
import asyncio
import logging
from typing import Optional, List, Dict, Tuple
from engine.chunker import FileChunker, FileManifest
from engine.merkle import MerkleTree
from engine.vector_clock import VectorClock
from engine.journal import StateJournal, FileEntry
from engine.watcher import DirectoryWatcher
from network.protocol import SyncMessage, MessageType, PeerInfo
from network.transport import AsyncTCPTransport
from network.gossip import GossipManager
from network.swarm import SwarmManager

logger = logging.getLogger("SyncNode")

class SyncNode:
    """
    Complete standalone SyncNode daemon that orchestrates:
    - File system watching & event journaling
    - Merkle tree state verification
    - Vector clock causal conflict resolution
    - Peer discovery & Gossip failure detection
    - P2P Swarm chunk distribution with delta-transfers
    """
    def __init__(
        self,
        node_id: str,
        sync_dir: str,
        host: str = "127.0.0.1",
        port: int = 0,
        chunk_size: int = 64 * 1024,
        enable_watcher: bool = True
    ):
        self.node_id = node_id
        self.sync_dir = os.path.abspath(sync_dir)
        self.host = host
        self.port = port
        self.chunk_size = chunk_size
        self.enable_watcher = enable_watcher

        os.makedirs(self.sync_dir, exist_ok=True)

        # Core engine components
        self.chunker = FileChunker(chunk_size=self.chunk_size)
        self.journal = StateJournal(self.sync_dir, self.node_id)
        self.watcher: Optional[DirectoryWatcher] = None

        # Network components
        self.transport = AsyncTCPTransport(self.host, self.port, message_handler=self.handle_incoming_message)
        self.gossip = GossipManager(
            node_id=self.node_id,
            host=self.host,
            port=self.port,
            get_root_hash_fn=self.get_root_hash,
            on_divergence_cb=self.on_peer_hash_divergence
        )
        self.swarm = SwarmManager(node_id=self.node_id)

        self._running = False
        self._sync_locks: Dict[str, asyncio.Lock] = {}

    def get_root_hash(self) -> str:
        return self.journal.get_merkle_tree().root_hash

    async def start(self):
        """Starts the transport, background gossip, initial disk scan, and watcher."""
        self._running = True
        self.loop = asyncio.get_running_loop()
        await self.transport.start_server()
        self.port = self.transport.port
        self.gossip.port = self.port

        # Initial disk scan to index any existing files
        self.scan_local_directory()

        # Start gossip loop
        self.gossip.start()

        # Start file watcher if enabled
        if self.enable_watcher:
            self.watcher = DirectoryWatcher(
                sync_dir=self.sync_dir,
                chunker=self.chunker,
                journal=self.journal,
                on_change_cb=self._on_watcher_event
            )
            self.watcher.start()

        logger.info(f"Node [{self.node_id}] started on {self.host}:{self.port}, sync_dir: {self.sync_dir}")

    async def stop(self):
        """Clean shutdown of node."""
        self._running = False
        if self.watcher:
            self.watcher.stop()
        await self.gossip.stop()
        await self.transport.stop()
        logger.info(f"Node [{self.node_id}] stopped.")

    def scan_local_directory(self):
        """Indexes all files already in the sync directory at startup."""
        for root, dirs, files in os.walk(self.sync_dir):
            # Exclude internal metadata directory
            if ".nutanix_sync" in root or ".git" in root:
                continue
            for file_name in files:
                if file_name.startswith(".tmp") or ".tmp." in file_name:
                    continue
                abs_path = os.path.join(root, file_name)
                rel_path = os.path.relpath(abs_path, self.sync_dir).replace("\\", "/")
                
                try:
                    manifest = self.chunker.chunk_file(abs_path, rel_path)
                    existing = self.journal.entries.get(rel_path)
                    if not existing or existing.manifest.full_hash != manifest.full_hash:
                        self.journal.record_local_mutation(rel_path, manifest, is_delete=False)
                        self.swarm.register_file_chunks(manifest.chunks, self.node_id)
                except Exception as e:
                    logger.debug(f"Error indexing {abs_path}: {e}")

    def _on_watcher_event(self, rel_path: str, manifest: FileManifest, is_delete: bool):
        """Callback from watchdog file observer when a local file changes."""
        if not self._running or not hasattr(self, 'loop') or not self.loop.is_running():
            return

        self.swarm.register_file_chunks(manifest.chunks, self.node_id)

        # Broadcast mutation to all alive peers for real-time sync
        try:
            asyncio.run_coroutine_threadsafe(
                self._broadcast_local_mutation(rel_path, manifest, is_delete),
                self.loop
            )
        except Exception as e:
            logger.debug(f"Failed to schedule broadcast: {e}")

    async def _broadcast_local_mutation(self, rel_path: str, manifest: FileManifest, is_delete: bool):
        entry = self.journal.entries.get(rel_path)
        if not entry:
            return

        msg = SyncMessage(
            msg_type=MessageType.MUTATION_BROADCAST,
            sender_id=self.node_id,
            payload={
                "rel_path": rel_path,
                "manifest": manifest.to_dict(),
                "vector_clock": entry.vector_clock.to_dict(),
                "is_delete": is_delete
            },
            timestamp=time.time()
        )

        for peer in self.gossip.get_alive_peers():
            try:
                await AsyncTCPTransport.send_message(peer.host, peer.port, msg, timeout=2.0)
            except Exception:
                pass

    def on_peer_hash_divergence(self, peer: PeerInfo):
        """Triggered when gossip notices a peer's Merkle root hash does not match ours."""
        asyncio.create_task(self._reconcile_with_peer(peer))

    async def _reconcile_with_peer(self, peer: PeerInfo):
        """Full Merkle diff reconciliation with a peer."""
        req_msg = SyncMessage(
            msg_type=MessageType.MERKLE_SYNC_REQ,
            sender_id=self.node_id,
            payload={"root_hash": self.get_root_hash()},
            timestamp=time.time()
        )

        resp = await AsyncTCPTransport.send_message(peer.host, peer.port, req_msg, timeout=4.0)
        if not resp or resp.msg_type != MessageType.MERKLE_SYNC_RESP:
            return

        remote_entries = resp.payload.get("entries", {})
        for rel_path, entry_data in remote_entries.items():
            manifest = FileManifest.from_dict(entry_data["manifest"])
            vclock = VectorClock.from_dict(entry_data["vector_clock"])
            is_delete = entry_data.get("deleted", False)
            await self._apply_or_conflict(rel_path, manifest, vclock, peer, is_delete)

    async def handle_incoming_message(self, message: SyncMessage, addr: Tuple[str, int]) -> Optional[SyncMessage]:
        """Dispatch incoming RPCs from peers."""
        msg_type = message.msg_type

        if msg_type == MessageType.GOSSIP_PING:
            return self.gossip.handle_ping(message)

        elif msg_type == MessageType.MUTATION_BROADCAST:
            # Handle real-time push notification of a change
            payload = message.payload
            rel_path = payload["rel_path"]
            manifest = FileManifest.from_dict(payload["manifest"])
            vclock = VectorClock.from_dict(payload["vector_clock"])
            is_delete = payload.get("is_delete", False)
            hop_count = payload.get("hop_count", 0)

            sender_peer = self.gossip.get_peer(message.sender_id) or PeerInfo(
                node_id=message.sender_id,
                host=addr[0],
                port=addr[1]
            )
            asyncio.create_task(self._apply_or_conflict(
                rel_path, manifest, vclock, sender_peer, is_delete, hop_count=hop_count
            ))
            return SyncMessage(
                msg_type=MessageType.GOSSIP_ACK,
                sender_id=self.node_id,
                payload={"status": "RECEIVED"},
                timestamp=time.time()
            )

        elif msg_type == MessageType.MERKLE_SYNC_REQ:
            # Return our active entries for reconciliation
            entries_data = {
                path: {
                    "manifest": entry.manifest.to_dict(),
                    "vector_clock": entry.vector_clock.to_dict(),
                    "deleted": entry.deleted
                }
                for path, entry in self.journal.entries.items()
            }
            return SyncMessage(
                msg_type=MessageType.MERKLE_SYNC_RESP,
                sender_id=self.node_id,
                payload={"entries": entries_data, "root_hash": self.get_root_hash()},
                timestamp=time.time()
            )

        elif msg_type == MessageType.CHUNK_REQ:
            chunk_hash = message.payload["chunk_hash"]
            rel_path = message.payload.get("rel_path", "")
            
            entry = self.journal.entries.get(rel_path)
            if entry and not entry.deleted:
                abs_path = os.path.join(self.sync_dir, rel_path)
                data = self.chunker.read_chunk_by_hash(abs_path, entry.manifest, chunk_hash)
                if data is not None:
                    return SyncMessage(
                        msg_type=MessageType.CHUNK_RESP,
                        sender_id=self.node_id,
                        payload={
                            "found": True,
                            "chunk_hash": chunk_hash,
                            "data": base64.b64encode(data).decode("ascii")
                        },
                        timestamp=time.time()
                    )

            # Search across all entries if not in that specific file
            for p, e in self.journal.entries.items():
                if not e.deleted:
                    abs_path = os.path.join(self.sync_dir, p)
                    data = self.chunker.read_chunk_by_hash(abs_path, e.manifest, chunk_hash)
                    if data is not None:
                        return SyncMessage(
                            msg_type=MessageType.CHUNK_RESP,
                            sender_id=self.node_id,
                            payload={
                                "found": True,
                                "chunk_hash": chunk_hash,
                                "data": base64.b64encode(data).decode("ascii")
                            },
                            timestamp=time.time()
                        )

            return SyncMessage(
                msg_type=MessageType.CHUNK_RESP,
                sender_id=self.node_id,
                payload={"found": False, "chunk_hash": chunk_hash},
                timestamp=time.time()
            )

        return None

    async def _apply_or_conflict(
        self,
        rel_path: str,
        remote_manifest: FileManifest,
        remote_vclock: VectorClock,
        remote_peer: PeerInfo,
        is_delete: bool,
        hop_count: int = 0
    ):
        """Core sync application engine with delta-chunking and conflict branch preservation."""
        if rel_path not in self._sync_locks:
            self._sync_locks[rel_path] = asyncio.Lock()

        async with self._sync_locks[rel_path]:
            action, conflict_path = self.journal.evaluate_remote_change(
                rel_path, remote_manifest, remote_vclock, remote_peer.node_id
            )

            if action == "IGNORE":
                return

            target_rel = conflict_path if action == "CONFLICT" else rel_path
            abs_target = os.path.join(self.sync_dir, target_rel)

            if is_delete and action != "CONFLICT":
                # Handle deletion
                if self.watcher:
                    self.watcher.handler.suppress_path(target_rel)
                if os.path.exists(abs_target):
                    try:
                        os.remove(abs_target)
                    except Exception:
                        pass
                self.journal.commit_remote_applied(rel_path, remote_manifest, remote_vclock, remote_peer.node_id, is_delete=True)
                if self.watcher:
                    self.watcher.handler.unsuppress_path(target_rel)
                return

            # Delta-chunking calculation
            local_entry = self.journal.entries.get(rel_path)
            local_manifest = local_entry.manifest if local_entry else None
            needed_hashes, needed_chunks = self.chunker.compute_delta(local_manifest, remote_manifest)

            # Calculate bandwidth metrics
            bytes_to_fetch = sum(c.size for c in needed_chunks)
            full_file_size = remote_manifest.size
            bandwidth_saved = max(0, full_file_size - bytes_to_fetch)
            self.swarm.total_bytes_saved += bandwidth_saved

            # Parallel chunk fetching from swarm
            alive_peers = self.gossip.get_alive_peers()
            if remote_peer not in alive_peers:
                alive_peers.append(remote_peer)

            fetched_chunks = await self.swarm.fetch_chunks_parallel(needed_hashes, rel_path, alive_peers)
            if len(fetched_chunks) < len(needed_hashes):
                logger.error(f"[{self.node_id}] Could not fetch all chunks for {rel_path}")
                return

            # Assemble file atomically
            if self.watcher:
                self.watcher.handler.suppress_path(target_rel)

            existing_file = abs_target if os.path.exists(abs_target) else (
                os.path.join(self.sync_dir, rel_path) if os.path.exists(os.path.join(self.sync_dir, rel_path)) else None
            )

            try:
                self.chunker.assemble_file(
                    target_path=abs_target,
                    manifest=remote_manifest,
                    chunk_data_map=fetched_chunks,
                    existing_file_path=existing_file
                )

                if action == "CONFLICT":
                    # Also register conflict file in journal
                    conflict_manifest = self.chunker.chunk_file(abs_target, target_rel)
                    self.journal.record_local_mutation(target_rel, conflict_manifest)
                else:
                    self.journal.commit_remote_applied(
                        rel_path, remote_manifest, remote_vclock, remote_peer.node_id, is_delete=False
                    )
            finally:
                if self.watcher:
                    self.watcher.handler.unsuppress_path(target_rel)

            # Forward mutation broadcast to downstream peers (Epidemic Flooding / Rumor Mongering)
            if hop_count < 6:
                entry = self.journal.entries.get(rel_path)
                if entry:
                    fwd_msg = SyncMessage(
                        msg_type=MessageType.MUTATION_BROADCAST,
                        sender_id=self.node_id,
                        payload={
                            "rel_path": rel_path,
                            "manifest": remote_manifest.to_dict(),
                            "vector_clock": entry.vector_clock.to_dict(),
                            "is_delete": is_delete,
                            "hop_count": hop_count + 1
                        },
                        timestamp=time.time()
                    )
                    for peer in self.gossip.get_alive_peers():
                        if peer.node_id != remote_peer.node_id:
                            try:
                                asyncio.create_task(AsyncTCPTransport.send_message(peer.host, peer.port, fwd_msg, timeout=2.0))
                            except Exception:
                                pass

    def get_status_summary(self) -> dict:
        """Returns node diagnostic metrics for dashboard / simulation."""
        tree = self.journal.get_merkle_tree()
        return {
            "node_id": self.node_id,
            "host": self.host,
            "port": self.port,
            "root_hash": tree.root_hash,
            "root_hash_short": tree.root_hash[:12] if tree.root_hash else "EMPTY",
            "files_count": len([e for e in self.journal.entries.values() if not e.deleted]),
            "peers_count": len(self.gossip.get_alive_peers()),
            "peers": [p.to_dict() for p in self.gossip.peers.values()],
            "bytes_transferred": self.swarm.total_bytes_transferred,
            "bytes_saved": self.swarm.total_bytes_saved,
            "conflicts_count": len([c for c in self.journal.conflicts if not c.resolved]),
            "conflicts": [c.to_dict() for c in self.journal.conflicts],
            "files": self.journal.get_active_files(),
            "recent_mutations": self.journal.mutation_log[-10:]
        }
