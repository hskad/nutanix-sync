import hashlib
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from engine.chunker import FileManifest

@dataclass
class MerkleNode:
    name: str
    path: str
    is_dir: bool
    hash: str
    children: Dict[str, 'MerkleNode'] = field(default_factory=dict)
    manifest: Optional[FileManifest] = None

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "path": self.path,
            "is_dir": self.is_dir,
            "hash": self.hash,
            "children": {k: v.to_dict() for k, v in self.children.items()},
            "has_manifest": self.manifest is not None
        }


class MerkleTree:
    """
    Hierarchical Merkle Tree for synchronized directories.
    Enables O(1) state equality checks and O(log M) diff discovery across nodes.
    """
    def __init__(self, manifests: Optional[Dict[str, FileManifest]] = None):
        self.root: MerkleNode = MerkleNode(name="", path="", is_dir=True, hash="")
        if manifests:
            self.build_from_manifests(manifests)

    @staticmethod
    def _hash_strings(strings: List[str]) -> str:
        hasher = hashlib.sha256()
        for s in sorted(strings):
            hasher.update(s.encode("utf-8"))
        return hasher.hexdigest()

    def build_from_manifests(self, manifests: Dict[str, FileManifest]) -> str:
        """
        Builds the tree from a dictionary of rel_path -> FileManifest.
        Returns the root hash.
        """
        root = MerkleNode(name="", path="", is_dir=True, hash="")

        # Sort paths to guarantee deterministic insertion
        for rel_path, manifest in sorted(manifests.items()):
            if manifest.deleted:
                continue
            
            parts = [p for p in rel_path.replace("\\", "/").split("/") if p]
            if not parts:
                continue

            curr = root
            for i, part in enumerate(parts[:-1]):
                dir_path = "/".join(parts[:i + 1])
                if part not in curr.children:
                    curr.children[part] = MerkleNode(name=part, path=dir_path, is_dir=True, hash="")
                curr = curr.children[part]

            file_name = parts[-1]
            curr.children[file_name] = MerkleNode(
                name=file_name,
                path=rel_path.replace("\\", "/"),
                is_dir=False,
                hash=manifest.full_hash,
                manifest=manifest
            )

        # Recursively compute node hashes bottom-up
        def compute_hashes(node: MerkleNode) -> str:
            if not node.is_dir:
                return node.hash

            child_hashes = []
            for child_name, child_node in sorted(node.children.items()):
                ch_hash = compute_hashes(child_node)
                child_hashes.append(f"{child_name}:{ch_hash}")

            node.hash = self._hash_strings(child_hashes)
            return node.hash

        self.root = root
        compute_hashes(self.root)
        return self.root.hash

    @property
    def root_hash(self) -> str:
        return self.root.hash

    @classmethod
    def compute_diff(cls, tree_a: 'MerkleTree', tree_b: 'MerkleTree') -> List[str]:
        """
        Compares two Merkle trees and returns the list of file rel_paths that differ or are missing.
        """
        differing_files: List[str] = []

        def diff_nodes(node_a: Optional[MerkleNode], node_b: Optional[MerkleNode]):
            if node_a is None and node_b is None:
                return
            if node_a is not None and node_b is not None and node_a.hash == node_b.hash:
                return  # Identical subtree!

            # If either node is a file (or missing)
            is_file_a = node_a is not None and not node_a.is_dir
            is_file_b = node_b is not None and not node_b.is_dir

            if is_file_a or is_file_b:
                path = (node_a.path if node_a else None) or (node_b.path if node_b else None)
                if path:
                    differing_files.append(path)
                return

            # Both are directories: find union of children and recurse
            children_a = node_a.children if node_a else {}
            children_b = node_b.children if node_b else {}
            all_keys = set(children_a.keys()) | set(children_b.keys())

            for k in sorted(all_keys):
                diff_nodes(children_a.get(k), children_b.get(k))

        diff_nodes(tree_a.root, tree_b.root)
        return differing_files
