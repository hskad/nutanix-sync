import os
import sys
import time
import asyncio
import argparse
from cluster_sim.node import SyncNode
from network.protocol import PeerInfo

async def main():
    parser = argparse.ArgumentParser(description="Nutanix-Sync: Real Machine File Synchronization Daemon")
    parser.add_argument("--id", type=str, default=f"node-{int(time.time()) % 1000:03d}", help="Unique Node ID")
    parser.add_argument("--dir", type=str, required=True, help="Directory path to keep in sync")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Listening host/IP")
    parser.add_argument("--port", type=int, default=9100, help="Listening TCP port")
    parser.add_argument("--peer", type=str, action="append", default=[], help="Seed peer in host:port format (can specify multiple)")

    args = parser.parse_args()

    print("\n" + "=" * 60)
    print(f"  Starting Nutanix-Sync Node [{args.id}]")
    print(f"  Syncing Directory : {os.path.abspath(args.dir)}")
    print(f"  Listening on      : {args.host}:{args.port}")
    print("=" * 60 + "\n")

    node = SyncNode(
        node_id=args.id,
        sync_dir=args.dir,
        host=args.host,
        port=args.port,
        enable_watcher=True
    )

    await node.start()

    # Add seed peers if provided
    for peer_str in args.peer:
        try:
            p_host, p_port = peer_str.split(":")
            peer_info = PeerInfo(
                node_id=f"peer-{p_port}",
                host=p_host,
                port=int(p_port),
                status="ALIVE"
            )
            node.gossip.add_peer(peer_info)
            print(f"  Connected seed peer -> {p_host}:{p_port}")
        except Exception as e:
            print(f"  Error adding peer '{peer_str}': {e}")

    print("\n[ACTIVE] File watcher running. Modify, add or remove files to sync in real-time.")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            await asyncio.sleep(2.0)
            summary = node.get_status_summary()
            alive_peers = len(node.gossip.get_alive_peers())
            print(f"\r[STATUS] Alive Peers: {alive_peers} | Merkle Root: {summary['root_hash_short']} | Synced Files: {summary['files_count']} | Saved: {summary['bytes_saved']} B", end="", flush=True)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\nShutting down node...")
    finally:
        await node.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
