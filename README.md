# Nutanix-Sync: Distributed Cluster File Synchronization Fabric

[![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Architecture](https://img.shields.io/badge/Architecture-Epidemic%20Gossip%20%2B%20P2P%20Swarm-FF3B00)](#architecture)
[![Tests](https://img.shields.io/badge/Tests-7%20Passing-10B981)](#testing)

> **Built for Nutanix Hackathon (IIT Guwahati) — Project Area 2:**  
> *"Develop mechanism to keep the files in multiple machines of a cluster in sync... scalable and performant and be able to deal with 100s of machines."*

---

## Overview

**Nutanix-Sync** is an enterprise-grade distributed file synchronization engine designed to keep directory states synchronized between two physical machines and seamlessly scale to **clusters of 100+ nodes**.

Instead of relying on centralized master bottlenecks or naive $O(N^2)$ broadcast floods, **Nutanix-Sync** combines:
1. **Content-Defined Delta Chunking & Merkle Trees:** Slices files into 64KB cryptographic chunks. Identifies exact modified blocks in $O(\log M)$, transferring only mutated bytes (**saving 60%–98%+ network egress**).
2. **SWIM-Style Epidemic Gossip Failure Detection:** Decentralized peer discovery and heartbeat monitoring scaling efficiently in $O(\log N)$ rounds.
3. **P2P Swarm Chunk Distribution:** BitTorrent-inspired parallel chunk fetching across peers holding chunks, avoiding single-publisher saturation.
4. **Vector Clock Causality & Conflict Isolation:** Tracks causality across asynchronous writes. When concurrent network partitions occur, non-destructive conflict branches are preserved without silent data loss.
5. **Architectural Minimalist Dashboard:** Real-time glassmorphic / editorial management console with dynamic HTML5 canvas topology, live telemetry, and chaos engineering controls.

---

## Architecture

```
+-----------------------------------------------------------------------------------+
|                        REAL-TIME MANAGEMENT DASHBOARD                             |
|  - Live Topology Canvas (2 to 100+ nodes)       - Merkle Tree Catalog & V-Clocks  |
|  - Bandwidth Savings Counter (Delta vs. Full)   - Chaos & Partition Injection     |
+------------------------------------------^----------------------------------------+
                                           | WebSocket / REST API
+------------------------------------------v----------------------------------------+
|                               SYNC NODE DAEMON                                    |
|                                                                                   |
|  +------------------------+  +------------------------+  +---------------------+  |
|  | File Watcher & Engine  |  | Chunker & Merkle Tree  |  | Vector Clock Engine |  |
|  | (watchdog, debouncing, |  | (64KB chunks, SHA-256  |  | (Causality tracking,|  |
|  |  atomic writes, journal|  |  diff calculation)     |  |  conflict branching)|  |
|  +------------------------+  +------------------------+  +---------------------+  |
|                                                                                   |
|  +-----------------------------------------------------------------------------+  |
|  |                    DISTRIBUTED NETWORKING & TRANSPORT                       |  |
|  |  - Async TCP Socket Transport (4-byte length prefix framing)                |  |
|  |  - Epidemic Gossip & SWIM-style Failure Detector                            |  |
|  |  - P2P Swarm Chunk Distribution (Parallel multi-peer chunk downloads)      |  |
|  +-----------------------------------------------------------------------------+  |
+-----------------------------------------------------------------------------------+
```

---

## Project Structure

```
.
├── engine/
│   ├── chunker.py           # 64KB content chunking, SHA-256 digests & delta calculation
│   ├── merkle.py            # Hierarchical directory hash tree (O(1) checks, O(log M) diffs)
│   ├── vector_clock.py      # Lamport / Vector Clock causal ordering engine
│   ├── conflict.py          # Non-destructive branch resolution on concurrent edits
│   ├── journal.py           # Local file index, mutations log, and state store
│   └── watcher.py           # Watchdog filesystem observer with event debouncing
├── network/
│   ├── protocol.py          # Message schemas (Handshake, Gossip, MerkleSync, Chunks)
│   ├── transport.py         # Low-latency Async TCP transport with 4-byte framing
│   ├── gossip.py            # SWIM-style failure detector & membership discovery
│   └── swarm.py             # Distributed P2P chunk provider & parallel fetcher
├── cluster_sim/
│   ├── node.py              # Full SyncNode daemon coordinating storage & gossip
│   └── orchestrator.py      # Spawns & controls 2 to 100+ virtual cluster nodes
├── web/
│   ├── server.py            # FastAPI + WebSocket real-time cluster manager
│   └── static/              # Minimalist editorial dashboard (Canvas, telemetry, controls)
├── tests/                   # Full test suite (Unit, Integration, Scale)
├── run_node.py              # CLI entry point to run a real node on any directory
├── run_simulation.py        # CLI entry point to launch cluster dashboard (port 8000)
└── requirements.txt         # Dependencies (FastAPI, Uvicorn, Watchdog, Websockets)
```

---

## Quickstart

### 1. Installation

```bash
# Clone the repository
git clone https://github.com/<your-username>/<repo-name>.git
cd <repo-name>

# Install dependencies
python -m pip install -r requirements.txt
```

### 2. Launch the Interactive Cluster Simulation & Dashboard

```bash
python run_simulation.py 8000
```
Open **`http://localhost:8000`** in your browser to explore:
* **Topology Canvas:** Real-time node links, animated gossip vectors, and status.
* **Delta-Sync Benchmark:** Click *"Mutate 64 Bytes"* to see Merkle tree diffing save >60%–98% bandwidth in real time.
* **Split-Brain Simulation:** Injects concurrent divergent edits to demonstrate vector clock branching.
* **Resilience Testing:** Sever random nodes and watch the cluster heal and catch up.

### 3. Run Real Physical Directory Sync (Across 2 Terminals or Machines)

Open two terminal windows:

**Node 1:**
```bash
python run_node.py --id node-alpha --dir ./folder_a --port 9101
```

**Node 2:**
```bash
python run_node.py --id node-beta --dir ./folder_b --port 9102 --peer 127.0.0.1:9101
```

Drop or modify any file in `folder_a` and watch it instantaneously propagate to `folder_b`!

---

## Testing

Run the full automated test suite (Unit, Integration, and 20-Node Scale convergence):

```bash
python -m unittest discover tests
```

---

## License

MIT License. Developed for the Nutanix Hackathon (IIT Guwahati).
