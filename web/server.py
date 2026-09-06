import os
import asyncio
import logging
from typing import Optional, List, Dict
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from cluster_sim.orchestrator import ClusterOrchestrator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("WebServer")

app = FastAPI(title="Nutanix-Sync Cluster Manager")

# Global cluster orchestrator
orchestrator = ClusterOrchestrator(base_sandbox_dir="./cluster_sandbox")

# Connected WebSocket clients
active_websockets: List[WebSocket] = []

class SpawnClusterReq(BaseModel):
    node_count: int = 6

class InjectFileReq(BaseModel):
    node_index: int = 0
    rel_path: str = "dataset/enterprise_report.txt"
    content_text: Optional[str] = None
    size_kb: Optional[int] = 128

class MutateDeltaReq(BaseModel):
    node_index: int = 0
    rel_path: str = "dataset/enterprise_report.txt"
    mutation_text: str = "[PATCHED_DELTA_DATA_REVISION_V2]"
    offset: int = 1500

class ConflictReq(BaseModel):
    node_a_idx: int = 0
    node_b_idx: int = 1
    rel_path: str = "critical_config.json"
    content_a: str = '{"database_host": "primary-us-east.cluster", "max_conns": 500}'
    content_b: str = '{"database_host": "replica-eu-west.cluster", "max_conns": 100}'

class NodeActionReq(BaseModel):
    node_index: int

@app.on_event("startup")
async def startup_event():
    # Bootstrap a default 5-node cluster on launch
    logger.info("Initializing default 5-node cluster...")
    await orchestrator.initialize_cluster(node_count=5, clean_existing=True)
    asyncio.create_task(broadcast_state_loop())

@app.on_event("shutdown")
async def shutdown_event():
    await orchestrator.shutdown_cluster()

async def broadcast_state_loop():
    """Streams cluster metrics to all connected WebSockets every 500ms."""
    while True:
        try:
            await asyncio.sleep(0.6)
            if active_websockets and orchestrator.is_running:
                state = orchestrator.get_cluster_state()
                dead_sockets = []
                for ws in active_websockets:
                    try:
                        await ws.send_json(state)
                    except Exception:
                        dead_sockets.append(ws)
                for ds in dead_sockets:
                    if ds in active_websockets:
                        active_websockets.remove(ds)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"Broadcast error: {e}")

@app.get("/api/cluster/state")
async def get_state():
    return orchestrator.get_cluster_state()

@app.post("/api/cluster/spawn")
async def spawn_cluster(req: SpawnClusterReq):
    count = max(2, min(req.node_count, 100))
    await orchestrator.initialize_cluster(node_count=count, clean_existing=True)
    return {"status": "SUCCESS", "node_count": count}

@app.post("/api/cluster/inject_file")
async def inject_file(req: InjectFileReq):
    if req.content_text:
        payload = req.content_text.encode("utf-8")
    else:
        # Generate dummy enterprise payload of requested size
        kb = max(1, req.size_kb or 128)
        header = f"# Nutanix Cluster Storage Payload: {req.rel_path}\n"
        filler = "NUTANIX-ENTERPRISE-DATA-BLOCK-ABC12345\n" * 25
        payload = (header + filler * ((kb * 1024) // len(filler))).encode("utf-8")[:kb * 1024]

    res = await orchestrator.inject_file(req.node_index, req.rel_path, payload)
    return {"status": "SUCCESS", "details": res}

@app.post("/api/cluster/mutate_delta")
async def mutate_delta(req: MutateDeltaReq):
    res = await orchestrator.mutate_file_delta(
        node_index=req.node_index,
        rel_path=req.rel_path,
        mutation_bytes=req.mutation_text.encode("utf-8"),
        offset=req.offset
    )
    return {"status": "SUCCESS", "details": res}

@app.post("/api/cluster/inject_conflict")
async def inject_conflict(req: ConflictReq):
    res = await orchestrator.inject_conflict(
        node_a_idx=req.node_a_idx,
        node_b_idx=req.node_b_idx,
        rel_path=req.rel_path,
        content_a=req.content_a.encode("utf-8"),
        content_b=req.content_b.encode("utf-8")
    )
    return {"status": "SUCCESS", "details": res}

@app.post("/api/cluster/kill_node")
async def kill_node(req: NodeActionReq):
    res = await orchestrator.kill_node(req.node_index)
    return res

@app.post("/api/cluster/revive_node")
async def revive_node(req: NodeActionReq):
    res = await orchestrator.revive_node(req.node_index)
    return res

@app.websocket("/ws/cluster")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_websockets.append(websocket)
    try:
        # Send immediate initial state
        await websocket.send_json(orchestrator.get_cluster_state())
        while True:
            # Keep alive / receive client pings
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in active_websockets:
            active_websockets.remove(websocket)
    except Exception:
        if websocket in active_websockets:
            active_websockets.remove(websocket)

# Serve static web frontend
static_dir = os.path.join(os.path.dirname(__file__), "static")
os.makedirs(static_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")

@app.get("/")
async def root():
    index_file = os.path.join(static_dir, "index.html")
    if os.path.exists(index_file):
        return FileResponse(index_file)
    return JSONResponse({"status": "UI loading..."})
