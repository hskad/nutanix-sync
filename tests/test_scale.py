import os
import shutil
import tempfile
import warnings
import asyncio
import unittest
from cluster_sim.orchestrator import ClusterOrchestrator

warnings.filterwarnings("ignore", category=ResourceWarning)

class TestClusterScale(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.sandbox = tempfile.mkdtemp()
        self.orchestrator = ClusterOrchestrator(base_sandbox_dir=self.sandbox)

    async def asyncTearDown(self):
        await self.orchestrator.shutdown_cluster()
        await asyncio.sleep(0.05)
        shutil.rmtree(self.sandbox, ignore_errors=True)

    async def test_20_node_cluster_convergence(self):
        NODE_COUNT = 20
        # 1. Initialize 20 nodes (disable file watcher threads for lightweight virtual simulation)
        for i in range(NODE_COUNT):
            node_id = f"node-{i:02d}"
            sync_dir = os.path.join(self.sandbox, node_id)
            node = self.orchestrator.get_node(node_id)
        
        await self.orchestrator.initialize_cluster(node_count=NODE_COUNT, clean_existing=True)
        self.assertEqual(len(self.orchestrator.nodes), NODE_COUNT)

        # 2. Inject a 256 KB file into node-00
        payload = b"Nutanix Hyperconverged Storage Cluster Block Data \x00\x01\x02" * 5000
        res = await self.orchestrator.inject_file(0, "cluster_manifest.bin", payload)
        self.assertEqual(res["source_node"], "node-00")

        # 3. Wait for epidemic gossip and swarm to converge across all 20 nodes
        converged = False
        for _ in range(40):
            await asyncio.sleep(0.3)
            state = self.orchestrator.get_cluster_state()
            if state["is_consistent"] and state["alive_nodes"] == NODE_COUNT and state["nodes"][0]["files_count"] > 0:
                converged = True
                break

        state = self.orchestrator.get_cluster_state()
        self.assertTrue(converged, f"Cluster of {NODE_COUNT} nodes did not converge in time. Distinct roots: {state['distinct_root_hashes']}")
        self.assertTrue(state["is_consistent"])

        # Check that the file exists and is identical on all 20 nodes
        for i in range(NODE_COUNT):
            file_path = os.path.join(self.orchestrator.nodes[i].sync_dir, "cluster_manifest.bin")
            self.assertTrue(os.path.exists(file_path), f"File missing on node-{i:02d}")
            self.assertEqual(os.path.getsize(file_path), len(payload))

if __name__ == "__main__":
    unittest.main()
