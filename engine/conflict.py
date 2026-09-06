import os
import time
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict

@dataclass
class ConflictRecord:
    id: str
    rel_path: str
    local_hash: str
    remote_hash: str
    local_clock: Dict[str, int]
    remote_clock: Dict[str, int]
    remote_node_id: str
    timestamp: float
    resolved: bool = False
    conflict_path: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


class ConflictResolver:
    """
    Manages non-destructive conflict handling when concurrent edits
    occur across partitioned or asynchronous cluster nodes.
    """
    def __init__(self, sync_dir: str):
        self.sync_dir = sync_dir

    def generate_conflict_rel_path(self, rel_path: str, remote_node_id: str) -> str:
        """
        Creates a deterministic non-conflicting filename:
        e.g., docs/report.pdf -> docs/report.conflict_nodeB_1725678900.pdf
        """
        dirname = os.path.dirname(rel_path)
        basename = os.path.basename(rel_path)
        name, ext = os.path.splitext(basename)
        ts = int(time.time())
        conflict_name = f"{name}.conflict_{remote_node_id}_{ts}{ext}"
        return os.path.join(dirname, conflict_name).replace("\\", "/") if dirname else conflict_name
