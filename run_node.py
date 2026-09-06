import os
import sys
import time
import asyncio
import argparse
from typing import Optional

from cluster_sim.node import SyncNode
from network.protocol import PeerInfo

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.layout import Layout
    from rich.live import Live
    from rich.text import Text
    HAS_RICH = True
except ImportError:
    HAS_RICH = False

def format_bytes(bytes_val: int) -> str:
    if not bytes_val or bytes_val == 0:
        return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB']:
        if abs(bytes_val) < 1024.0:
            return f"{bytes_val:3.1f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.1f} TB"

def make_layout(node: SyncNode, start_time: float) -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=4),
        Layout(name="body", ratio=1),
        Layout(name="footer", size=3)
    )

    summary = node.get_status_summary()
    uptime = int(time.time() - start_time)

    # 1. HEADER
    header_text = Text()
    header_text.append("NUTANIX-SYNC // CLUSTER NODE DAEMON\n", style="bold white")
    header_text.append(f"Node ID: [{node.node_id}]  •  Port: {node.port}  •  Dir: {node.sync_dir}  •  Uptime: {uptime}s", style="dim")
    layout["header"].update(Panel(header_text, style="white on #0c0d10", border_style="#222630"))

    # 2. BODY SPLIT (Left: Peers & Traffic, Right: Files & Causality)
    layout["body"].split_row(
        Layout(name="left", ratio=1),
        Layout(name="right", ratio=1)
    )

    # Left: Active Peers & Traffic
    peers_table = Table(title="Decentralized Gossip Mesh (SWIM)", title_style="bold #ff3b00", border_style="#222630", expand=True)
    peers_table.add_column("Node ID", style="bold white")
    peers_table.add_column("Endpoint", style="dim")
    peers_table.add_column("Status", style="green")
    peers_table.add_column("Merkle Root", style="dim")

    alive_peers = node.gossip.get_alive_peers()
    for p in alive_peers:
        peers_table.add_row(
            p.node_id,
            f"{p.host}:{p.port}",
            "[green]ALIVE[/green]" if p.status == "ALIVE" else "[red]DEAD[/red]",
            p.root_hash[:8] + "..." if p.root_hash else "EMPTY"
        )
    if not alive_peers:
        peers_table.add_row("—", "No peers connected yet", "[yellow]SEARCHING[/yellow]", "—")

    # Traffic stats panel
    gross = summary['bytes_transferred'] + summary['bytes_saved']
    pct = round((summary['bytes_saved'] / gross * 100.0), 1) if gross > 0 else 0.0
    traffic_text = Text()
    traffic_text.append("Wire Transferred : ", style="dim")
    traffic_text.append(f"{format_bytes(summary['bytes_transferred'])}\n", style="bold white")
    traffic_text.append("Bandwidth Saved  : ", style="dim")
    traffic_text.append(f"{format_bytes(summary['bytes_saved'])} ({pct}% reduction)\n", style="bold green" if pct > 0 else "dim")
    traffic_text.append("Active Conflicts : ", style="dim")
    traffic_text.append(f"{summary['conflicts_count']} preserved\n", style="bold red" if summary['conflicts_count'] > 0 else "dim")
    traffic_text.append("Local Merkle Root: ", style="dim")
    traffic_text.append(f"{summary['root_hash_short']}", style="bold cyan")

    left_panel = Layout()
    left_panel.split_column(
        Layout(peers_table, ratio=1),
        Layout(Panel(traffic_text, title="Telemetry & Egress Metrics", border_style="#222630"), size=7)
    )
    layout["body"]["left"].update(left_panel)

    # Right: Tracked Files & Vector Clocks
    files_table = Table(title="Synchronized Physical Files on Disk", title_style="bold white", border_style="#222630", expand=True)
    files_table.add_column("Relative Path", style="bold white")
    files_table.add_column("Size", style="dim")
    files_table.add_column("Chunks", style="dim")
    files_table.add_column("Vector Clock Causality", style="cyan")

    files = summary.get("files", [])
    for f in files:
        files_table.add_row(
            f["rel_path"],
            format_bytes(f["size"]),
            f"{f['chunks']} blk",
            str(f["vector_clock"])
        )
    if not files:
        files_table.add_row("—", "—", "—", "Directory empty (drop a file to sync)")

    layout["body"]["right"].update(files_table)

    # 3. FOOTER
    footer_text = Text(" [WATCHING] Operating system filesystem hooks active (watchdog). Drop, edit, or delete files. Press Ctrl+C to exit.", style="dim")
    layout["footer"].update(Panel(footer_text, border_style="#222630"))

    return layout


async def main():
    parser = argparse.ArgumentParser(description="Nutanix-Sync: Real Machine File Synchronization Daemon")
    parser.add_argument("--id", type=str, default=f"node-{int(time.time()) % 1000:03d}", help="Unique Node ID")
    parser.add_argument("--dir", type=str, required=True, help="Directory path to keep in sync")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Listening host/IP")
    parser.add_argument("--port", type=int, default=9100, help="Listening TCP port")
    parser.add_argument("--peer", type=str, action="append", default=[], help="Seed peer in host:port format")
    parser.add_argument("--no-tui", action="store_true", help="Disable rich TUI and use plain console output")

    args = parser.parse_args()

    node = SyncNode(
        node_id=args.id,
        sync_dir=args.dir,
        host=args.host,
        port=args.port,
        enable_watcher=True
    )

    await node.start()

    # Connect seed peers
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
        except Exception as e:
            print(f"Error adding peer '{peer_str}': {e}")

    start_time = time.time()
    use_tui = HAS_RICH and not args.no_tui and sys.stdout.isatty()

    if use_tui:
        console = Console()
        with Live(make_layout(node, start_time), refresh_per_second=3, screen=True) as live:
            try:
                while True:
                    await asyncio.sleep(0.3)
                    live.update(make_layout(node, start_time))
            except (KeyboardInterrupt, asyncio.CancelledError):
                pass
            finally:
                await node.stop()
    else:
        print("\n" + "=" * 60)
        print(f"  Starting Nutanix-Sync Node [{args.id}]")
        print(f"  Syncing Directory : {os.path.abspath(args.dir)}")
        print(f"  Listening on      : {args.host}:{args.port}")
        print("=" * 60 + "\n")
        print("[ACTIVE] File watcher running. Modify, add or remove files to sync in real-time.")
        print("Press Ctrl+C to stop.\n")

        try:
            while True:
                await asyncio.sleep(2.0)
                summary = node.get_status_summary()
                alive_peers = len(node.gossip.get_alive_peers())
                print(
                    f"\r[STATUS] Alive Peers: {alive_peers} | Merkle Root: {summary['root_hash_short']} | "
                    f"Synced Files: {summary['files_count']} | Saved: {format_bytes(summary['bytes_saved'])}",
                    end="",
                    flush=True
                )
        except (KeyboardInterrupt, asyncio.CancelledError):
            print("\nShutting down node...")
        finally:
            await node.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
