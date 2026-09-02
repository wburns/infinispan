package org.infinispan.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;

import org.infinispan.testing.jupiter.tags.Fuzz;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;

/**
 * Differential fuzz test for {@link BufferedSoftBPlusTree}. Jazzer drives a random sequence of operations
 * (put / remove / get / flush / save+reload / GC-clear soft references) against both the buffered tree and a
 * {@link TreeMap} oracle over a small key space, asserting the tree always agrees with the model. This is the
 * randomised counterpart to the deterministic write-count assertions in {@link SoftBPlusTreeTest}: it targets
 * the buffering machinery - deferred writes, coalesced overwrites, pinned dirty nodes surviving GC, and
 * flush/save/reload boundaries - which is where a lost update or stale read would hide.
 *
 * <p>Lives in {@code org.infinispan.util} so it can call the package-private {@link SoftBPlusTree#clearSoftReferences()}
 * that simulates the GC reclaiming soft-referenced nodes.
 *
 * <p>Runs in Jazzer's regression/coverage mode by default (replays the checked-in corpus); the live campaign is
 * enabled with the {@code fuzz} Maven profile. Jazzer skips live fuzzing automatically on unsupported platforms.
 *
 * @author William Burns
 */
@Fuzz
public class SoftBPlusTreeFuzzTest {

   private static final int MIN_NODE_SIZE = 15;
   private static final int MAX_NODE_SIZE = 60;
   // Small key space so the fuzzer densely revisits keys, exercising overwrite coalescing and structural churn.
   private static final int KEY_SPACE = 64;
   // Bound the op count so a single input cannot run unbounded even with a large corpus entry.
   private static final int MAX_OPS = 500;

   // Values encode their key in the high 16 bits, so the key can be reconstructed from the value alone. The
   // index tree persists only values in its leaves and reloads keys on demand, so a value-derived key loader is
   // stateless and stays correct across save/reload without any side table.
   private final SoftBPlusTree.KeyLoader<Integer> keyLoader = value -> key(value >>> 16);

   private static byte[] key(int i) {
      // Two digits keep lexicographic byte order identical to numeric order across the whole key space.
      return String.format("key-%02d", i).getBytes(StandardCharsets.UTF_8);
   }

   @FuzzTest
   void fuzzDifferentialAgainstTreeMap(FuzzedDataProvider data) throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      int flushMutationCount = data.consumeInt(1, 8);
      BufferedSoftBPlusTree<Integer> tree = newTree(store, flushMutationCount);
      TreeMap<Integer, Integer> model = new TreeMap<>();
      int seq = 0;

      int ops = 0;
      while (data.remainingBytes() > 0 && ops++ < MAX_OPS) {
         switch (data.consumeInt(0, 6)) {
            case 0 -> {
               int k = data.consumeInt(0, KEY_SPACE - 1);
               int value = (k << 16) | (seq++ & 0xffff);
               tree.put(key(k), value);
               model.put(k, value);
            }
            case 1 -> {
               int k = data.consumeInt(0, KEY_SPACE - 1);
               assertEquals(model.remove(k), tree.remove(key(k)), "remove mismatch for key " + k);
            }
            case 2 -> {
               int k = data.consumeInt(0, KEY_SPACE - 1);
               assertEquals(model.get(k), tree.get(key(k)), "get mismatch for key " + k);
            }
            case 3 -> tree.flush();
            // Save the whole tree and reload it over the same store, modelling a graceful stop/restart while
            // continuing to mutate afterwards.
            case 4 -> tree = saveAndReload(store, flushMutationCount, tree);
            case 5 -> tree.clearSoftReferences();
            case 6 -> verifyAll(tree, model);
         }
      }

      verifyAll(tree, model);

      // Final round-trip through a disk-only snapshot: everything the model holds must be served from the
      // persisted bytes alone, catching any buffered mutation that never reached disk.
      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();
      BufferedSoftBPlusTree<Integer> reloaded = newTree(store.snapshot(), flushMutationCount);
      reloaded.loadTree(rootSpace);
      verifyAll(reloaded, model);
   }

   /**
    * Persists {@code tree} and reconstructs it over the same store using the full restart protocol the real
    * {@code Index} uses: save the root, write the free-block state past the node data, then on the fresh tree
    * restore the store size, deserialize the free blocks, and load the root. Skipping the store-size / free-block
    * half would make new allocations restart at offset 0 and overwrite still-live nodes.
    */
   private BufferedSoftBPlusTree<Integer> saveAndReload(InMemoryNodeStore store, int flushMutationCount,
         BufferedSoftBPlusTree<Integer> tree) throws IOException {
      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();
      ByteBuffer freeBlocks = tree.serializeFreeBlocks();
      int freeBlocksLen = freeBlocks.remaining();
      long freeBlocksOffset = tree.getStoreSize();
      store.write(freeBlocks, freeBlocksOffset);

      BufferedSoftBPlusTree<Integer> reloaded = newTree(store, flushMutationCount);
      reloaded.setStoreSize(freeBlocksOffset);
      if (freeBlocksLen > 0) {
         reloaded.deserializeFreeBlocks(store.read(freeBlocksOffset, freeBlocksLen));
      }
      reloaded.loadTree(rootSpace);
      return reloaded;
   }

   private void verifyAll(SoftBPlusTree<Integer> tree, TreeMap<Integer, Integer> model) {
      for (int k = 0; k < KEY_SPACE; k++) {
         assertEquals(model.get(k), tree.get(key(k)), "value mismatch for key " + k);
      }
   }

   private BufferedSoftBPlusTree<Integer> newTree(InMemoryNodeStore store, int flushMutationCount) {
      return new BufferedSoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store,
            SoftBPlusTreeTest.INT_SERIALIZER, keyLoader, flushMutationCount);
   }
}
