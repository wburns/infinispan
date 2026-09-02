package org.infinispan.persistence.sifs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.util.BufferedSoftBPlusTree;
import org.infinispan.util.InMemoryNodeStore;
import org.infinispan.util.SoftBPlusTree;
import org.testng.annotations.Test;

/**
 * Deterministic write-count validation of SIFS index buffering, exercised over the store's real value type
 * ({@link IndexEntry}) and serializer ({@link Index#INDEX_ENTRY_SERIALIZER}). Because an {@link IndexEntry}
 * always serializes to a fixed 12 bytes, relocating an entry - what the compactor does when it moves a live
 * record to a new file (a {@code MOVED} update) - keeps the key and the serialized size unchanged. Such an
 * update is therefore an in-place value overwrite that {@link BufferedSoftBPlusTree} coalesces, so a storm of
 * relocations of one hot key collapses into a single index node write instead of one write per relocation.
 *
 * @author William Burns
 */
@Test(groups = "unit", testName = "persistence.sifs.SifsIndexBufferingWriteCountTest")
public class SifsIndexBufferingWriteCountTest {

   private static final int MIN_NODE_SIZE = 30;
   private static final int MAX_NODE_SIZE = 120;

   // The index tree persists only IndexEntry values in its leaves and loads each entry's key on demand,
   // exactly as a running SIFS segment does via IndexEntry#loadKey. Keys are recovered here from the entry's
   // (file, offset) location, which is stable across a save/reload of the tree.
   private final Map<Long, byte[]> keyByLocation = new HashMap<>();
   private final SoftBPlusTree.KeyLoader<IndexEntry> keyLoader =
         entry -> keyByLocation.get(location(entry.file, entry.offset));

   private static long location(int file, int offset) {
      return ((long) file << 32) | (offset & 0xffffffffL);
   }

   private static byte[] key(int i) {
      return String.format("key-%05d", i).getBytes(StandardCharsets.UTF_8);
   }

   private void put(SoftBPlusTree<IndexEntry> tree, byte[] key, IndexEntry entry) throws IOException {
      keyByLocation.put(location(entry.file, entry.offset), key.clone());
      tree.put(key, entry);
   }

   private BufferedSoftBPlusTree<IndexEntry> newTree(InMemoryNodeStore store, int flushMutationCount) {
      return new BufferedSoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store,
            Index.INDEX_ENTRY_SERIALIZER, keyLoader, flushMutationCount);
   }

   public void testCompactorMovedStormCoalescesToSingleIndexWrite() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      int flushMutationCount = 10;
      BufferedSoftBPlusTree<IndexEntry> tree = newTree(store, flushMutationCount);

      // Build a multi-level index so the hot key lives in a leaf beneath the root and its overwrites take the
      // in-place coalescing path rather than the single-leaf-root shortcut.
      int keyCount = 200;
      for (int i = 0; i < keyCount; i++) {
         put(tree, key(i), new IndexEntry(0, i * 100, 1));
      }
      tree.flush();
      // After a flush every persisted node is a SoftNode, matching the on-disk index a running segment reads.
      assertTrue(store.writeCount > 0, "Building and flushing the index must persist nodes");

      store.writeCount = 0;
      byte[] hot = key(100);

      // Simulate the compactor relocating the hot entry far more times than flushMutationCount. Every MOVED
      // reuses the same key and a fixed 12-byte IndexEntry, so each is a same-size value overwrite that is
      // coalesced; none advances the flush threshold (dirtyOverwrites holds a single leaf).
      int moves = flushMutationCount * 20;
      IndexEntry latest = null;
      for (int move = 1; move <= moves; move++) {
         latest = new IndexEntry(move, 100_000 + move, 1);
         put(tree, hot, latest);
      }
      assertEquals(0, store.writeCount,
            "A MOVED storm on one key must coalesce and trigger no index write before flush");

      tree.flush();
      assertTrue(store.writeCount > 0 && store.writeCount <= 2,
            "The coalesced MOVED storm must flush as a single in-place index write, was " + store.writeCount);

      // The latest relocation is the location the index serves, both in memory and after a save/reload.
      assertEquals(latest.file, tree.get(hot).file, "In-memory index must serve the latest MOVED location");

      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();
      // Reload from a disk-only snapshot so the served entry is read back from the persisted bytes.
      BufferedSoftBPlusTree<IndexEntry> reloaded = newTree(store.snapshot(), flushMutationCount);
      reloaded.loadTree(rootSpace);
      IndexEntry reloadedEntry = reloaded.get(hot);
      assertEquals(latest.file, reloadedEntry.file, "Reloaded index must serve the latest MOVED file");
      assertEquals(latest.offset, reloadedEntry.offset, "Reloaded index must serve the latest MOVED offset");
   }
}
