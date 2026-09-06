import sys
import uvicorn
import logging

if __name__ == "__main__":
    port = 8000
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            pass

    print("\n" + "=" * 65)
    print("  NUTANIX-SYNC: DISTRIBUTED CLUSTER FILE FABRIC")
    print("  Scale 2 to 100+ Nodes • Delta-Chunking • Merkle Gossip Swarm")
    print("=" * 65)
    print(f"  Dashboard available at: http://localhost:{port}")
    print("=" * 65 + "\n")

    uvicorn.run("web.server:app", host="127.0.0.1", port=port, log_level="info")
