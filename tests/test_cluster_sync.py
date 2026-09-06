import os
import shutil
import tempfile
import warnings
import asyncio
import unittest
from cluster_sim.orchestrator import ClusterOrchestrator

warnings.filterwarnings("ignore", category=ResourceWarning)

class TestClusterSync(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.sandbox = tempfile.mkdtemp()
        self.orchestrator = ClusterOrchestrator(base_sandbox_dir=self.sandbox)

    async def asyncTearDown(self):
        await self.orchestrator.shutdown_cluster()
        await asyncio.sleep(0.05)
        shutil.rmtree(self.sandbox, ignore_errors=True)

    async def test_3_node_sync_propagation(self):
        # 1. Initialize 3 nodes
        await self.orchestrator.initialize_cluster(node_count=3, clean_existing=True)
        self.assertEqual(len(self.orchestrator.nodes), 3)

        # 2. Inject a test file into Node 0
        content = b"Nutanix Distributed Cluster Sync Test Payload 12345\n" * 100
        await self.orchestrator.inject_file(0, "docs/nutanix_spec.txt", content)

        # 3. Give async gossip and swarm a brief moment to propagate
        for _ in range(25):
            await asyncio.sleep(0.2)
            state = self.orchestrator.get_cluster_state()
            if state["is_consistent"] and state["nodes"][0]["files_count"] > 0:
                break

        state = self.orchestrator.get_cluster_state()
        self.assertTrue(state["is_consistent"], "All nodes should reach identical Merkle root hash")

        # Verify file content on node 1 and node 2 disk
        node1_file = os.path.join(self.orchestrator.nodes[1].sync_dir, "docs/nutanix_spec.txt")
        node2_file = os.path.join(self.orchestrator.nodes[2].sync_dir, "docs/nutanix_spec.txt")

        self.assertTrue(os.path.exists(node1_file))
        self.assertTrue(os.path.exists(node2_file))
        with open(node1_file, "rb") as f:
            self.assertEqual(f.read(), content)
        with open(node2_file, "rb") as f:
            self.assertEqual(f.read(), content)

    async def test_delta_bandwidth_savings(self):
        await self.orchestrator.initialize_cluster(node_count=2, clean_existing=True)
        
        # 1. Create a 128 KB file (2 chunks of 64KB)
        big_content = b"A" * (64 * 1024) + b"B" * (64 * 1024)
        await self.orchestrator.inject_file(0, "big_database.bin", big_content)
        await asyncio.sleep(1.0)

        # 2. Mutate only 10 bytes in the second chunk
        await self.orchestrator.mutate_file_delta(0, "big_database.bin", b"MUTATED!!!", offset=65000)
        await asyncio.sleep(1.0)

        state = self.orchestrator.get_cluster_state()
        # Bandwidth saved should be positive because chunk 0 was reused!
        self.assertGreater(state["total_bytes_saved"], 0)

if __name__ == "__main__":
    unittest.main()
