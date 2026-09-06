import os
import time
import threading
from typing import Callable, Optional, Dict
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent
from engine.chunker import FileChunker, FileManifest
from engine.journal import StateJournal

class SyncFileEventHandler(FileSystemEventHandler):
    """
    Watchdog event handler with debouncing and internal directory exclusion.
    """
    def __init__(
        self,
        sync_dir: str,
        chunker: FileChunker,
        journal: StateJournal,
        on_change_cb: Optional[Callable[[str, FileManifest, bool], None]] = None,
        debounce_seconds: float = 0.3
    ):
        super().__init__()
        self.sync_dir = os.path.abspath(sync_dir)
        self.chunker = chunker
        self.journal = journal
        self.on_change_cb = on_change_cb
        self.debounce_seconds = debounce_seconds

        self._debounce_timers: Dict[str, threading.Timer] = {}
        self._lock = threading.Lock()
        self._suppressed_paths: set = set()

    def suppress_path(self, rel_path: str):
        """Temporarily suppress events for a path being written by the sync engine."""
        with self._lock:
            self._suppressed_paths.add(rel_path.replace("\\", "/"))

    def unsuppress_path(self, rel_path: str):
        with self._lock:
            self._suppressed_paths.discard(rel_path.replace("\\", "/"))

    def _should_ignore(self, path: str) -> bool:
        abs_path = os.path.abspath(path)
        rel = os.path.relpath(abs_path, self.sync_dir).replace("\\", "/")
        
        # Ignore internal metadata, temp files, git, or hidden files
        if ".nutanix_sync" in rel or rel.startswith(".git") or ".tmp." in rel or rel.endswith(".tmp"):
            return True

        with self._lock:
            if rel in self._suppressed_paths:
                return True

        return False

    def _schedule_processing(self, abs_path: str, is_delete: bool = False):
        if self._should_ignore(abs_path):
            return

        rel_path = os.path.relpath(abs_path, self.sync_dir).replace("\\", "/")

        with self._lock:
            if rel_path in self._debounce_timers:
                self._debounce_timers[rel_path].cancel()

            timer = threading.Timer(
                self.debounce_seconds,
                self._process_event,
                args=[abs_path, rel_path, is_delete]
            )
            self._debounce_timers[rel_path] = timer
            timer.daemon = True
            timer.start()

    def _process_event(self, abs_path: str, rel_path: str, is_delete: bool):
        with self._lock:
            self._debounce_timers.pop(rel_path, None)

        if self._should_ignore(abs_path):
            return

        try:
            if is_delete or not os.path.exists(abs_path):
                # Deleted file
                dummy_manifest = FileManifest(
                    rel_path=rel_path,
                    size=0,
                    mtime=time.time(),
                    full_hash="",
                    chunks=[],
                    deleted=True
                )
                self.journal.record_local_mutation(rel_path, dummy_manifest, is_delete=True)
                if self.on_change_cb:
                    self.on_change_cb(rel_path, dummy_manifest, True)
            else:
                if os.path.isdir(abs_path):
                    return  # Directory creation itself doesn't need manifest

                # Chunk file
                manifest = self.chunker.chunk_file(abs_path, rel_path)
                self.journal.record_local_mutation(rel_path, manifest, is_delete=False)
                if self.on_change_cb:
                    self.on_change_cb(rel_path, manifest, False)
        except Exception as e:
            # File might be temporarily locked by an application
            print(f"Error processing watcher event for {rel_path}: {e}")

    def on_created(self, event: FileSystemEvent):
        if not event.is_directory:
            self._schedule_processing(event.src_path, is_delete=False)

    def on_modified(self, event: FileSystemEvent):
        if not event.is_directory:
            self._schedule_processing(event.src_path, is_delete=False)

    def on_deleted(self, event: FileSystemEvent):
        if not event.is_directory:
            self._schedule_processing(event.src_path, is_delete=True)

    def on_moved(self, event: FileSystemEvent):
        if not event.is_directory:
            self._schedule_processing(event.src_path, is_delete=True)
            self._schedule_processing(event.dest_path, is_delete=False)


class DirectoryWatcher:
    """Controls background filesystem observer."""
    def __init__(
        self,
        sync_dir: str,
        chunker: FileChunker,
        journal: StateJournal,
        on_change_cb: Optional[Callable[[str, FileManifest, bool], None]] = None
    ):
        self.sync_dir = os.path.abspath(sync_dir)
        self.handler = SyncFileEventHandler(self.sync_dir, chunker, journal, on_change_cb)
        self.observer = Observer()

    def start(self):
        self.observer.schedule(self.handler, self.sync_dir, recursive=True)
        self.observer.start()

    def stop(self):
        if self.observer.is_alive():
            self.observer.stop()
            self.observer.join(timeout=2.0)
