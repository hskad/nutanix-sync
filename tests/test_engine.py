import os
import shutil
import tempfile
import unittest
from engine.chunker import FileChunker, FileManifest
from engine.merkle import MerkleTree
from engine.vector_clock import VectorClock, CausalityRelation
from engine.journal import StateJournal

class TestEngine(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.chunker = FileChunker(chunk_size=1024)  # 1 KB chunks for testing

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_chunker_and_delta(self):
        # 1. Create a 5 KB file
        file_path = os.path.join(self.test_dir, "test.dat")
        data = b"A" * 2048 + b"B" * 2048 + b"C" * 1024
        with open(file_path, "wb") as f:
            f.write(data)

        manifest1 = self.chunker.chunk_file(file_path, "test.dat")
        self.assertEqual(len(manifest1.chunks), 5)
        self.assertEqual(manifest1.size, 5120)

        # 2. Mutate only the middle 50 bytes of block 2
        mutated_data = b"A" * 2048 + b"B" * 500 + b"X" * 50 + b"B" * 1498 + b"C" * 1024
        with open(file_path, "wb") as f:
            f.write(mutated_data)

        manifest2 = self.chunker.chunk_file(file_path, "test.dat")
        needed_hashes, needed_chunks = self.chunker.compute_delta(manifest1, manifest2)

        # Only 1 chunk should need transmission, not all 5!
        self.assertEqual(len(needed_hashes), 1)
        self.assertEqual(len(needed_chunks), 1)

    def test_vector_clock_causality(self):
        vc_a1 = VectorClock({"nodeA": 1})
        vc_a2 = vc_a1.increment("nodeA") # {"nodeA": 2}
        vc_b1 = VectorClock({"nodeB": 1})

        # vc_a1 is before vc_a2
        self.assertEqual(vc_a1.compare(vc_a2), CausalityRelation.BEFORE)
        self.assertEqual(vc_a2.compare(vc_a1), CausalityRelation.AFTER)

        # Concurrent modification: nodeA and nodeB wrote independently
        self.assertEqual(vc_a2.compare(vc_b1), CausalityRelation.CONCURRENT)

        # Merging clocks
        merged = vc_a2.merge(vc_b1) # {"nodeA": 2, "nodeB": 1}
        self.assertEqual(vc_a2.compare(merged), CausalityRelation.BEFORE)
        self.assertEqual(vc_b1.compare(merged), CausalityRelation.BEFORE)

    def test_merkle_tree_diff(self):
        # Tree 1: file1, file2
        file1 = os.path.join(self.test_dir, "file1.txt")
        file2 = os.path.join(self.test_dir, "docs", "file2.txt")
        os.makedirs(os.path.dirname(file2), exist_ok=True)
        with open(file1, "wb") as f: f.write(b"Hello")
        with open(file2, "wb") as f: f.write(b"World")

        m1 = self.chunker.chunk_file(file1, "file1.txt")
        m2 = self.chunker.chunk_file(file2, "docs/file2.txt")

        tree1 = MerkleTree({"file1.txt": m1, "docs/file2.txt": m2})
        tree2 = MerkleTree({"file1.txt": m1, "docs/file2.txt": m2})

        # Identical trees
        self.assertEqual(tree1.root_hash, tree2.root_hash)
        diff = MerkleTree.compute_diff(tree1, tree2)
        self.assertEqual(diff, [])

        # Modify file2 in tree2
        with open(file2, "wb") as f: f.write(b"World Modified")
        m2_mod = self.chunker.chunk_file(file2, "docs/file2.txt")
        tree2_mod = MerkleTree({"file1.txt": m1, "docs/file2.txt": m2_mod})

        self.assertNotEqual(tree1.root_hash, tree2_mod.root_hash)
        diff_mod = MerkleTree.compute_diff(tree1, tree2_mod)
        self.assertEqual(diff_mod, ["docs/file2.txt"])

    def test_journal_conflict_handling(self):
        journal = StateJournal(self.test_dir, "nodeA")
        test_file = os.path.join(self.test_dir, "shared.txt")
        with open(test_file, "wb") as f: f.write(b"Initial text")
        manifest_init = self.chunker.chunk_file(test_file, "shared.txt")
        entry_init = journal.record_local_mutation("shared.txt", manifest_init)

        # Local edit on nodeA
        with open(test_file, "wb") as f: f.write(b"NodeA text")
        manifest_a = self.chunker.chunk_file(test_file, "shared.txt")
        journal.record_local_mutation("shared.txt", manifest_a)

        # Incoming concurrent edit from nodeB
        manifest_b = FileManifest(
            rel_path="shared.txt",
            size=10,
            mtime=12345.0,
            full_hash="different_hash_from_node_b",
            chunks=[]
        )
        clock_b = VectorClock({"nodeB": 1}) # Concurrent with {"nodeA": 2}

        action, conflict_path = journal.evaluate_remote_change("shared.txt", manifest_b, clock_b, "nodeB")
        self.assertEqual(action, "CONFLICT")
        self.assertTrue("conflict_nodeB" in conflict_path)
        self.assertEqual(len(journal.conflicts), 1)

if __name__ == "__main__":
    unittest.main()
