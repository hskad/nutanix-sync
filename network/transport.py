import asyncio
import struct
import json
import logging
from typing import Callable, Optional, Dict, Tuple
from network.protocol import SyncMessage

logger = logging.getLogger("Transport")

class AsyncTCPTransport:
    """
    High-performance async TCP transport using 4-byte length-prefixed framing.
    Optimized for low-latency node-to-node cluster communication.
    """
    def __init__(
        self,
        host: str,
        port: int,
        message_handler: Optional[Callable[[SyncMessage, Tuple[str, int]], asyncio.Future]] = None
    ):
        self.host = host
        self.port = port
        self.message_handler = message_handler
        self.server: Optional[asyncio.Server] = None
        self._running = False
        self._active_writers = set()

    async def start_server(self):
        self._running = True
        self.server = await asyncio.start_server(self._handle_client, self.host, self.port)
        # Update port if dynamic port 0 was passed
        self.port = self.server.sockets[0].getsockname()[1]
        logger.info(f"Transport listening on {self.host}:{self.port}")

    async def stop(self):
        self._running = False
        for w in list(self._active_writers):
            try:
                w.close()
            except Exception:
                pass
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        await asyncio.sleep(0.05)

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info('peername')
        self._active_writers.add(writer)
        try:
            while self._running:
                # Read 4-byte big-endian length prefix
                length_bytes = await reader.readexactly(4)
                msg_length = struct.unpack("!I", length_bytes)[0]

                # Read message payload
                payload_bytes = await reader.readexactly(msg_length)
                json_str = payload_bytes.decode("utf-8")
                sync_msg = SyncMessage.from_json(json_str)

                # Process message
                response = None
                if self.message_handler:
                    res = self.message_handler(sync_msg, addr)
                    if asyncio.iscoroutine(res):
                        response = await res
                    else:
                        response = res

                # If response returned, send it back immediately on same connection
                if response is not None:
                    if isinstance(response, SyncMessage):
                        resp_data = response.to_json().encode("utf-8")
                    elif isinstance(response, dict):
                        resp_data = json.dumps(response).encode("utf-8")
                    else:
                        resp_data = str(response).encode("utf-8")
                    
                    writer.write(struct.pack("!I", len(resp_data)) + resp_data)
                    await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        except Exception as e:
            logger.debug(f"Client connection closed with {addr}: {e}")
        finally:
            self._active_writers.discard(writer)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    @classmethod
    async def send_message(cls, host: str, port: int, message: SyncMessage, timeout: float = 3.0) -> Optional[SyncMessage]:
        writer = None
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
            
            data = message.to_json().encode("utf-8")
            header = struct.pack("!I", len(data))
            writer.write(header + data)
            await writer.drain()

            # Read response
            length_bytes = await asyncio.wait_for(reader.readexactly(4), timeout=timeout)
            resp_len = struct.unpack("!I", length_bytes)[0]
            resp_bytes = await asyncio.wait_for(reader.readexactly(resp_len), timeout=timeout)
            
            return SyncMessage.from_json(resp_bytes.decode("utf-8"))
        except Exception as e:
            logger.debug(f"Failed to send message to {host}:{port}: {e}")
            return None
        finally:
            if writer is not None:
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass
