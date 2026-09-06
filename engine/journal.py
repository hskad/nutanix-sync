import os
import json
import time
import threading
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from engine.chunker import FileManifest
from engine.vector_clock import VectorClock, CausalityRelation
from engine.conflict import ConflictRecord, ConflictResolver
from engine.merkle import MerkleTree

@dataclass
class FileEntry:
    rel_path: str
    manifest: FileManifest
    vector_clock: VectorClock
    deleted: bool = False
    last_modified_by: str = ""

    def to_dict(self) -> dict:
        return {
            "rel_path": self.rel_path,
            "manifest": self.manifest.to_dict(),
            "vector_clock": self.vector_clock.to_dict(),
            "deleted": self.deleted,
            "last_modified_by": self.last_modified_by
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'FileEntry':
        manifest = FileManifest.from_dict(data["manifest"])
        vector_clock = VectorClock.from_dict(data["vector_clock"])
        return cls(
            rel_path=data["rel_path"],
            manifest=manifest,
            vector_clock=vector_clock,
            deleted=data.get("deleted", False),
            last_modified_by=data.get("last_modified_by", "")
        )


class StateJournal:
    """
    StateJournal maintains the node's local index of all synchronized files,
    their vector clocks, chunk manifests, mutation history, and active conflicts.
    """
    def __init__(self, sync_dir: str, node_id: str, state_file_name: str = "state.json"):
        self.sync_dir = os.path.abspath(sync_dir)
        self.node_id = node_id
        self.meta_dir = os.path.join(self.sync_dir, ".nutanix_sync")
        self.state_file = os.path.join(self.meta_dir, state_file_name)
        
        self.entries: Dict[str, FileEntry] = {}
        self.conflicts: List[ConflictRecord] = []
        self.mutation_log: List[dict] = []
        self.resolver = ConflictResolver(self.sync_dir)
        self._save_lock = threading.Lock()

        os.makedirs(self.meta_dir, exist_ok=True)
        self.load_state()

    def load_state(self):
        """Loads state from persistent JSON store if exists."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    for path, entry_data in data.get("entries", {}).items():
                        self.entries[path] = FileEntry.from_dict(entry_data)
                    self.conflicts = [ConflictRecord(**c) for c in data.get("conflicts", [])]
                    self.mutation_log = data.get("mutation_log", [])[-100:]
            except Exception as e:
                print(f"[{self.node_id}] Warning loading state: {e}. Starting fresh state.")

    def save_state(self):
        """Persists state atomically to disk."""
        with self._save_lock:
            os.makedirs(self.meta_dir, exist_ok=True)
            data = {
                "node_id": self.node_id,
                "saved_at": time.time(),
                "entries": {k: v.to_dict() for k, v in self.entries.items()},
                "conflicts": [c.to_dict() for c in self.conflicts],
                "mutation_log": self.mutation_log[-100:]
            }
            tmp_file = f"{self.state_file}.tmp.{self.node_id}.{int(time.time()*1000000)}"
            try:
                with open(tmp_file, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
                os.replace(tmp_file, self.state_file)
            except Exception as e:
                if os.path.exists(tmp_file):
                    try:
                        os.remove(tmp_file)
                    except Exception:
                        pass
                print(f"[{self.node_id}] Error saving state: {e}")

    def record_local_mutation(self, rel_path: str, manifest: FileManifest, is_delete: bool = False) -> FileEntry:
        """
        Invoked when the local file watcher detects a local file change.
        Increments this node's vector clock counter.
        """
        rel_path = rel_path.replace("\\", "/")
        existing = self.entries.get(rel_path)
        
        if existing:
            vclock = existing.vector_clock.increment(self.node_id)
        else:
            vclock = VectorClock({self.node_id: 1})

        manifest.deleted = is_delete
        entry = FileEntry(
            rel_path=rel_path,
            manifest=manifest,
            vector_clock=vclock,
            deleted=is_delete,
            last_modified_by=self.node_id
        )
        self.entries[rel_path] = entry
        
        log_entry = {
            "type": "DELETE" if is_delete else "UPDATE",
            "rel_path": rel_path,
            "node_id": self.node_id,
            "timestamp": time.time(),
            "clock": vclock.to_dict(),
            "hash": manifest.full_hash
        }
        self.mutation_log.append(log_entry)
        self.save_state()
        return entry

    def evaluate_remote_change(self, rel_path: str, remote_manifest: FileManifest, remote_vclock: VectorClock, remote_node_id: str) -> Tuple[str, Optional[str]]:
        """
        Evaluates an incoming update against local state using Vector Clocks.
        Returns:
            - action: 'APPLY' | 'IGNORE' | 'CONFLICT'
            - conflict_rel_path: path to save remote copy if action == 'CONFLICT'
        """
        rel_path = rel_path.replace("\\", "/")
        local_entry = self.entries.get(rel_path)

        if not local_entry:
            # New file seen from remote
            return "APPLY", None

        # Compare vector clocks
        relation = local_entry.vector_clock.compare(remote_vclock)

        if relation == CausalityRelation.BEFORE:
            # Remote is newer (strictly dominates local)
            return "APPLY", None
        elif relation == CausalityRelation.AFTER:
            # Local is strictly newer, ignore older remote update
            return "IGNORE", None
        elif relation == CausalityRelation.EQUAL:
            # Clocks are equal, hashes match
            if local_entry.manifest.full_hash == remote_manifest.full_hash:
                return "IGNORE", None
            return "APPLY", None
        else:
            # Concurrent / CONCURRENT!
            # If the content hash is identical anyway, fast-forward merge clocks
            if local_entry.manifest.full_hash == remote_manifest.full_hash:
                return "APPLY", None

            # Real conflict!
            conflict_path = self.resolver.generate_conflict_rel_path(rel_path, remote_node_id)
            conflict_record = ConflictRecord(
                id=f"{rel_path}_{int(time.time()*1000)}",
                rel_path=rel_path,
                local_hash=local_entry.manifest.full_hash,
                remote_hash=remote_manifest.full_hash,
                local_clock=local_entry.vector_clock.to_dict(),
                remote_clock=remote_vclock.to_dict(),
                remote_node_id=remote_node_id,
                timestamp=time.time(),
                resolved=False,
                conflict_path=conflict_path
            )
            self.conflicts.append(conflict_record)
            self.save_state()
            return "CONFLICT", conflict_path

    def commit_remote_applied(self, rel_path: str, manifest: FileManifest, remote_vclock: VectorClock, source_node_id: str, is_delete: bool = False):
        """Called after remote file chunks have been assembled and verified on disk."""
        rel_path = rel_path.replace("\\", "/")
        local_entry = self.entries.get(rel_path)
        
        if local_entry:
            merged_clock = local_entry.vector_clock.merge(remote_vclock)
        else:
            merged_clock = remote_vclock

        manifest.deleted = is_delete
        entry = FileEntry(
            rel_path=rel_path,
            manifest=manifest,
            vector_clock=merged_clock,
            deleted=is_delete,
            last_modified_by=source_node_id
        )
        self.entries[rel_path] = entry
        
        self.mutation_log.append({
            "type": "REMOTE_DELETE" if is_delete else "REMOTE_APPLY",
            "rel_path": rel_path,
            "node_id": source_node_id,
            "timestamp": time.time(),
            "clock": merged_clock.to_dict(),
            "hash": manifest.full_hash
        })
        self.save_state()

    def get_merkle_tree(self) -> MerkleTree:
        """Builds a MerkleTree from active non-deleted manifests."""
        active_manifests = {
            path: entry.manifest for path, entry in self.entries.items()
            if not entry.deleted
        }
        return MerkleTree(active_manifests)

    def get_active_files(self) -> List[dict]:
        """Returns summary list of active files for dashboard / status."""
        result = []
        for path, entry in self.entries.items():
            if not entry.deleted:
                result.append({
                    "rel_path": path,
                    "size": entry.manifest.size,
                    "chunks": len(entry.manifest.chunks),
                    "full_hash": entry.manifest.full_hash[:12] + "...",
                    "vector_clock": entry.vector_clock.to_dict(),
                    "last_modified_by": entry.last_modified_by,
                    "mtime": entry.manifest.mtime
                })
        return sorted(result, key=lambda x: x["rel_path"])
