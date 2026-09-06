import os
import hashlib
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass, asdict

DEFAULT_CHUNK_SIZE = 64 * 1024  # 64 KB chunks

@dataclass
class ChunkInfo:
    index: int
    offset: int
    size: int
    hash: str

@dataclass
class FileManifest:
    rel_path: str
    size: int
    mtime: float
    full_hash: str
    chunks: List[ChunkInfo]
    deleted: bool = False

    def to_dict(self) -> dict:
        return {
            "rel_path": self.rel_path,
            "size": self.size,
            "mtime": self.mtime,
            "full_hash": self.full_hash,
            "chunks": [asdict(c) for c in self.chunks],
            "deleted": self.deleted
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'FileManifest':
        chunks = [ChunkInfo(**c) for c in data.get("chunks", [])]
        return cls(
            rel_path=data["rel_path"],
            size=data["size"],
            mtime=data["mtime"],
            full_hash=data["full_hash"],
            chunks=chunks,
            deleted=data.get("deleted", False)
        )


class FileChunker:
    """
    Handles chunking files into fixed/content-based segments and computing SHA-256 hashes.
    Enables delta synchronization where only missing/modified chunks are transmitted.
    """
    def __init__(self, chunk_size: int = DEFAULT_CHUNK_SIZE):
        self.chunk_size = chunk_size

    def hash_bytes(self, data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    def chunk_file(self, file_path: str, rel_path: str) -> FileManifest:
        """Reads a file and breaks it into chunk segments with hashes."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        stat = os.stat(file_path)
        chunks: List[ChunkInfo] = []
        full_hasher = hashlib.sha256()
        offset = 0
        index = 0

        with open(file_path, "rb") as f:
            while True:
                data = f.read(self.chunk_size)
                if not data:
                    break
                full_hasher.update(data)
                chunk_hash = self.hash_bytes(data)
                chunks.append(ChunkInfo(
                    index=index,
                    offset=offset,
                    size=len(data),
                    hash=chunk_hash
                ))
                offset += len(data)
                index += 1

        # Handle 0-byte files
        if offset == 0:
            chunks.append(ChunkInfo(
                index=0,
                offset=0,
                size=0,
                hash=self.hash_bytes(b"")
            ))

        return FileManifest(
            rel_path=rel_path.replace("\\", "/"),
            size=stat.st_size,
            mtime=stat.st_mtime,
            full_hash=full_hasher.hexdigest(),
            chunks=chunks,
            deleted=False
        )

    def compute_delta(self, old_manifest: Optional[FileManifest], new_manifest: FileManifest) -> Tuple[List[str], List[ChunkInfo]]:
        """
        Computes the delta between an existing file manifest and a new manifest.
        Returns:
            - needed_chunk_hashes: list of chunk hashes that are missing in old_manifest
            - required_chunks: list of ChunkInfo objects representing what needs to be fetched
        """
        if old_manifest is None or old_manifest.deleted:
            # Everything is new
            return [c.hash for c in new_manifest.chunks], new_manifest.chunks

        old_hashes = {c.hash for c in old_manifest.chunks}
        needed_hashes = []
        required_chunks = []

        for chunk in new_manifest.chunks:
            if chunk.hash not in old_hashes:
                needed_hashes.append(chunk.hash)
                required_chunks.append(chunk)

        return needed_hashes, required_chunks

    def read_chunk_by_hash(self, file_path: str, manifest: FileManifest, chunk_hash: str) -> Optional[bytes]:
        """Reads specific chunk bytes from disk given its hash and manifest."""
        for chunk in manifest.chunks:
            if chunk.hash == chunk_hash:
                with open(file_path, "rb") as f:
                    f.seek(chunk.offset)
                    return f.read(chunk.size)
        return None

    def assemble_file(self, target_path: str, manifest: FileManifest, chunk_data_map: Dict[str, bytes], existing_file_path: Optional[str] = None) -> bool:
        """
        Reconstructs the file at target_path from chunk_data_map.
        Can reuse existing chunks from existing_file_path if available to save disk writes.
        """
        os.makedirs(os.path.dirname(os.path.abspath(target_path)), exist_ok=True)
        tmp_target = f"{target_path}.tmp.{os.getpid()}"

        try:
            with open(tmp_target, "wb") as out_f:
                for chunk in manifest.chunks:
                    if chunk.hash in chunk_data_map:
                        out_f.write(chunk_data_map[chunk.hash])
                    elif existing_file_path and os.path.exists(existing_file_path):
                        # Read from existing local file
                        with open(existing_file_path, "rb") as old_f:
                            old_f.seek(chunk.offset)
                            out_f.write(old_f.read(chunk.size))
                    else:
                        raise ValueError(f"Missing chunk {chunk.hash} for file {manifest.rel_path}")

            # Verify integrity
            verify_hasher = hashlib.sha256()
            with open(tmp_target, "rb") as f:
                while chunk := f.read(self.chunk_size):
                    verify_hasher.update(chunk)

            if verify_hasher.hexdigest() != manifest.full_hash:
                raise ValueError(f"SHA-256 hash mismatch after assembling {manifest.rel_path}")

            # Atomic rename
            if os.path.exists(target_path):
                os.replace(tmp_target, target_path)
            else:
                os.rename(tmp_target, target_path)

            # Preserve mtime if possible
            try:
                os.utime(target_path, (manifest.mtime, manifest.mtime))
            except Exception:
                pass

            return True
        finally:
            if os.path.exists(tmp_target):
                try:
                    os.remove(tmp_target)
                except Exception:
                    pass
