package org.infinispan.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "util.SoftBPlusTreeTest")
public class SoftBPlusTreeTest {

   private static final int MIN_NODE_SIZE = 15;
   private static final int MAX_NODE_SIZE = 60;

   private final ConcurrentHashMap<Integer, byte[]> keysByValue = new ConcurrentHashMap<>();

   private final SoftBPlusTree.KeyLoader<Integer> keyLoader = keysByValue::get;

   @BeforeMethod
   public void clearKeyMap() {
      keysByValue.clear();
   }

   private void putTracked(SoftBPlusTree<Integer> tree, byte[] key, int value) throws IOException {
      keysByValue.put(value, key.clone());
      tree.put(key, value);
   }

   private static byte[] key(String s) {
      return s.getBytes(StandardCharsets.UTF_8);
   }

   static final SoftBPlusTree.ValueSerializer<Integer> INT_SERIALIZER = new SoftBPlusTree.ValueSerializer<>() {
      @Override
      public void write(Integer value, ByteBuffer buffer) {
         buffer.putInt(value);
      }

      @Override
      public Integer read(ByteBuffer buffer) {
         return buffer.getInt();
      }

      @Override
      public int serializedSize(Integer value) {
         return 4;
      }
   };


   public void testPerNodeSoftening() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      for (int i = 0; i < count; i++) {
         assertEquals(Integer.valueOf(i), tree.get(key(String.format("key-%05d", i))));
      }

      BPlusTree.Node<Integer> rootNode = tree.getRoot();
      if (rootNode instanceof BPlusTree.InnerNode<Integer> root) {
         for (BPlusTree.Node<Integer> child : root.children) {
            assertInstanceOf(SoftBPlusTree.SoftNode.class, child, "Child should be SoftNode after puts");
         }
      }
   }

   public void testSerializeDeserializeNodeRoundTrip() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      BPlusTree.Node<Integer> root = tree.getRoot();
      assertInstanceOf(BPlusTree.InnerNode.class, root, "Root should be InnerNode for large tree");

      ByteBuffer serialized = SoftBPlusTree.serializeNode(root, INT_SERIALIZER);
      BPlusTree.Node<Integer> deserialized = tree.deserializeNode(serialized);

      assertInstanceOf(BPlusTree.InnerNode.class, deserialized, "Deserialized root should be InnerNode");
      BPlusTree.InnerNode<Integer> innerDeserialized = (BPlusTree.InnerNode<Integer>) deserialized;
      BPlusTree.InnerNode<Integer> innerOriginal = (BPlusTree.InnerNode<Integer>) root;
      assertEquals(innerOriginal.children.length, innerDeserialized.children.length);

      for (BPlusTree.Node<Integer> child : innerDeserialized.children) {
         assertInstanceOf(SoftBPlusTree.SoftNode.class, child, "Deserialized children should be SoftNodes");
      }
   }

   public void testSoftenChildrenAndGet() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();

      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      loaded.loadTree(rootSpace);

      BPlusTree.InnerNode<Integer> root = (BPlusTree.InnerNode<Integer>) loaded.getRoot();
      for (BPlusTree.Node<Integer> child : root.children) {
         assertInstanceOf(SoftBPlusTree.SoftNode.class, child, "Child should be SoftNode after load");
      }

      for (int i = 0; i < count; i++) {
         assertEquals(Integer.valueOf(i), loaded.get(key(String.format("key-%05d", i))));
      }
   }

   public void testSoftenChildrenAndPublish() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 100;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      List<Integer> values = tree.<Integer>publish((k, v) -> v).toList().blockingGet();
      assertEquals(count, values.size());
   }

   public void testSoftNodeReloadAfterGC() throws Exception {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();

      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      loaded.loadTree(rootSpace);

      loaded.clearSoftReferences();

      for (int i = 0; i < count; i++) {
         assertEquals(Integer.valueOf(i), loaded.get(key(String.format("key-%05d", i))), "Value for key-" + String.format("%05d", i) + " should reload from store");
      }
   }

   public void testRepeatedReclamationAndReload() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      for (int cycle = 0; cycle < 3; cycle++) {
         tree.clearSoftReferences();
         for (int i = 0; i < count; i++) {
            assertEquals(Integer.valueOf(i), tree.get(key(String.format("key-%05d", i))), "Cycle " + cycle + ": value mismatch");
         }
      }

      tree.clearSoftReferences();
      putTracked(tree, key("key-00050"), 9999);
      tree.clearSoftReferences();
      assertEquals(Integer.valueOf(9999), tree.get(key("key-00050")));
      assertEquals(Integer.valueOf(0), tree.get(key("key-00000")));
   }

   public void testPublishAfterReclamation() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 100;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      tree.clearSoftReferences();

      List<Integer> values = tree.<Integer>publish((k, v) -> v).toList().blockingGet();
      assertEquals(count, values.size());
      for (int i = 0; i < count; i++) {
         assertEquals(Integer.valueOf(i), values.get(i));
      }
   }

   public void testPutAfterSoften() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      putTracked(tree, key("key-00050"), 9999);

      BPlusTree.Node<Integer> rootNode = tree.getRoot();
      if (rootNode instanceof BPlusTree.InnerNode<Integer> root) {
         for (BPlusTree.Node<Integer> child : root.children) {
            assertInstanceOf(SoftBPlusTree.SoftNode.class, child, "All children should be SoftNodes");
         }
      }

      assertEquals(Integer.valueOf(9999), tree.get(key("key-00050")));
      for (int i = 0; i < count; i++) {
         if (i == 50) continue;
         assertEquals(Integer.valueOf(i), tree.get(key(String.format("key-%05d", i))));
      }
   }

   public void testRemoveAfterSoften() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      assertEquals(Integer.valueOf(100), tree.remove(key("key-00100")));
      assertEquals(count - 1, tree.size());
      assertNull(tree.get(key("key-00100")));

      for (int i = 0; i < count; i++) {
         if (i == 100) continue;
         assertEquals(Integer.valueOf(i), tree.get(key(String.format("key-%05d", i))));
      }
   }

   public void testReSoftenAfterModification() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      putTracked(tree, key("key-00050"), 9999);

      BPlusTree.Node<Integer> rootNode = tree.getRoot();
      if (rootNode instanceof BPlusTree.InnerNode<Integer> root) {
         for (BPlusTree.Node<Integer> child : root.children) {
            assertInstanceOf(SoftBPlusTree.SoftNode.class, child, "All children should be SoftNodes after auto re-softening");
         }
      }

      assertEquals(Integer.valueOf(9999), tree.get(key("key-00050")));
   }

   public void testSoftenOnSmallTree() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);

      putTracked(tree, key("a"), 1);
      putTracked(tree, key("b"), 2);

      assertInstanceOf(BPlusTree.LeafNode.class, tree.getRoot(), "Root should still be LeafNode for small tree");

      assertEquals(Integer.valueOf(1), tree.get(key("a")));
      assertEquals(Integer.valueOf(2), tree.get(key("b")));
   }

   public void testSaveLoadAndModify() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();

      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      loaded.loadTree(rootSpace);

      BPlusTree.InnerNode<Integer> root = (BPlusTree.InnerNode<Integer>) loaded.getRoot();
      for (BPlusTree.Node<Integer> child : root.children) {
         assertInstanceOf(SoftBPlusTree.SoftNode.class, child, "Child should be SoftNode");
      }

      putTracked(loaded, key("key-00050"), 9999);
      root = (BPlusTree.InnerNode<Integer>) loaded.getRoot();
      for (BPlusTree.Node<Integer> child : root.children) {
         assertInstanceOf(SoftBPlusTree.SoftNode.class, child, "Child should be SoftNode after modify");
      }

      assertEquals(Integer.valueOf(9999), loaded.get(key("key-00050")));
      assertEquals(Integer.valueOf(0), loaded.get(key("key-00000")));
   }

   public void testMixedOperationsWithSoftening() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      TreeMap<String, Integer> reference = new TreeMap<>();
      java.util.Random rng = new java.util.Random(42L);
      int valueCounter = 10000;

      for (int op = 0; op < 1000; op++) {
         int action = rng.nextInt(4);
         String k = "key-" + rng.nextInt(100);
         switch (action) {
            case 0: // put
               int val = valueCounter++;
               reference.put(k, val);
               putTracked(tree, key(k), val);
               break;
            case 1: // get
               Integer refVal = reference.get(k);
               Integer treeVal = tree.get(key(k));
               assertEquals(refVal, treeVal, "get(" + k + ") mismatch at op " + op);
               break;
            case 2: // remove
               reference.remove(k);
               tree.remove(key(k));
               break;
            case 3: // clear
               reference.clear();
               tree.clear();
               break;
         }
         assertEquals(reference.size(), tree.size(), "size mismatch at op " + op);
      }

      for (Map.Entry<String, Integer> entry : reference.entrySet()) {
         assertEquals(entry.getValue(), tree.get(key(entry.getKey())));
      }
   }

   public void testPublishWithTakeAndSoftNodes() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      List<Integer> values = tree.<Integer>publish((k, v) -> v).take(50).toList().blockingGet();

      assertEquals(50, values.size());
      assertEquals(Integer.valueOf(0), values.get(0));
      assertEquals(Integer.valueOf(49), values.get(49));
   }

   public void testHeavyRemoveWithSoftening() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(15, 60, store, INT_SERIALIZER, keyLoader);
      TreeMap<String, Integer> reference = new TreeMap<>();
      java.util.Random rng = new java.util.Random(12345L);
      int valueCounter = 10000;

      for (int op = 0; op < 3000; op++) {
         int action = rng.nextInt(10);
         String k = String.format("seg/%03d/entry", rng.nextInt(200));
         if (action < 4) {
            int val = valueCounter++;
            reference.put(k, val);
            putTracked(tree, key(k), val);
         } else if (action < 8) {
            reference.remove(k);
            tree.remove(key(k));
         } else {
            Integer refVal = reference.get(k);
            Integer treeVal = tree.get(key(k));
            assertEquals(refVal, treeVal, "get(" + k + ") mismatch at op " + op);
         }
         assertEquals(reference.size(), tree.size(), "size mismatch at op " + op);
      }

      for (Map.Entry<String, Integer> entry : reference.entrySet()) {
         assertEquals(entry.getValue(), tree.get(key(entry.getKey())));
      }
   }

   // --- saveTree / loadTree round-trip tests ---

   public void testSaveLoadTreeRoundTrip() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();

      InMemoryNodeStore loadStore = store.snapshot();
      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, loadStore, INT_SERIALIZER, keyLoader);
      loaded.loadTree(rootSpace);

      for (int i = 0; i < count; i++) {
         assertEquals(Integer.valueOf(i), loaded.get(key(String.format("key-%05d", i))));
      }

      BPlusTree.Node<Integer> root = loaded.getRoot();
      assertInstanceOf(BPlusTree.InnerNode.class, root, "Root should be InnerNode");
      BPlusTree.InnerNode<Integer> inner = (BPlusTree.InnerNode<Integer>) root;
      for (BPlusTree.Node<Integer> child : inner.children) {
         assertInstanceOf(SoftBPlusTree.SoftNode.class, child, "Child should be SoftNode after loadTree");
      }
   }

   public void testSaveLoadTreeSmallLeafRoot() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      putTracked(tree, key("a"), 1);
      putTracked(tree, key("b"), 2);

      assertInstanceOf(BPlusTree.LeafNode.class, tree.getRoot(), "Root should be LeafNode for small tree");

      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();

      InMemoryNodeStore loadStore = store.snapshot();
      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, loadStore, INT_SERIALIZER, keyLoader);
      loaded.loadTree(rootSpace);

      assertEquals(Integer.valueOf(1), loaded.get(key("a")));
      assertEquals(Integer.valueOf(2), loaded.get(key("b")));
   }

   public void testSaveLoadTreeEmpty() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);

      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();
      assertNull(rootSpace, "Empty tree should return null space");

      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      loaded.loadTree(rootSpace);

      assertEquals(0, loaded.size());
      assertNull(loaded.get(key("anything")));

      putTracked(loaded, key("hello"), 42);
      assertEquals(1, loaded.size());
      assertEquals(Integer.valueOf(42), loaded.get(key("hello")));
   }

   public void testSaveLoadTreeThenMutate() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();

      InMemoryNodeStore loadStore = store.snapshot();
      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, loadStore, INT_SERIALIZER, keyLoader);
      loaded.loadTree(rootSpace);

      putTracked(loaded, key("key-00050"), 9999);
      assertEquals(Integer.valueOf(9999), loaded.get(key("key-00050")));
      loaded.remove(key("key-00100"));
      assertNull(loaded.get(key("key-00100")));

      assertEquals(Integer.valueOf(0), loaded.get(key("key-00000")));
      assertEquals(Integer.valueOf(199), loaded.get(key("key-00199")));
   }

   // --- Per-node efficiency tests ---

   public void testWriteCountIsProportionalToHeight() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 1000;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      int totalWritesBuild = store.writeCount;

      store.writeCount = 0;
      putTracked(tree, key("key-00500"), 9999);
      int writesForPut = store.writeCount;

      store.writeCount = 0;
      tree.remove(key("key-00300"));
      int writesForRemove = store.writeCount;

      assertTrue(writesForPut < totalWritesBuild / 5, "Single put (" + writesForPut + ") should be much less than full build (" + totalWritesBuild + ")");
      assertTrue(writesForRemove < totalWritesBuild / 5, "Single remove (" + writesForRemove + ") should be much less than full build (" + totalWritesBuild + ")");
   }

   // --- Buffered (deferred) write tests ---

   private SoftBPlusTree<Integer> buildTree(InMemoryNodeStore store, int count) throws IOException {
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }
      return tree;
   }

   // Flush count high enough that the buffered tree never auto-flushes mid-test, so tests keep full control
   // over when a flush happens (asserting the pre-flush buffered state and driving flush() manually).
   private static final int NO_AUTO_FLUSH = Integer.MAX_VALUE;

   private BufferedSoftBPlusTree<Integer> newBufferedTree(InMemoryNodeStore store) {
      return new BufferedSoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader, NO_AUTO_FLUSH);
   }

   private BufferedSoftBPlusTree<Integer> buildBufferedTree(InMemoryNodeStore store, int count) throws IOException {
      BufferedSoftBPlusTree<Integer> tree = newBufferedTree(store);
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }
      tree.flush();
      return tree;
   }

   public void testDeferredValueOverwriteInPlaceRoundTrip() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      BufferedSoftBPlusTree<Integer> tree = buildBufferedTree(store, 200);

      store.writeCount = 0;
      byte[] hot = key("key-00100");
      for (int v = 1000; v < 1050; v++) {
         putTracked(tree, hot, v);
      }

      assertEquals(0, store.writeCount, "Buffered overwrites must not write before flush");
      assertTrue(tree.isDirty(), "Tree should be dirty with buffered overwrites");
      assertEquals(Integer.valueOf(1049), tree.get(hot), "Reads must see the latest buffered value");

      tree.flush();
      assertFalse(tree.isDirty(), "Tree should be clean after flush");
      assertTrue(store.writeCount <= 2, "Coalesced overwrite should flush in ~1 in-place write, was " + store.writeCount);

      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();
      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      loaded.loadTree(rootSpace);
      assertEquals(Integer.valueOf(1049), loaded.get(hot), "Flushed overwrite must survive reload");
      assertEquals(Integer.valueOf(0), loaded.get(key("key-00000")));
      assertEquals(Integer.valueOf(199), loaded.get(key("key-00199")));
   }

   public void testDeferredOverwriteSurvivesGcClear() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree.NodeSpace rootSpace = buildTree(store, 200).saveTree();

      BufferedSoftBPlusTree<Integer> tree = newBufferedTree(store);
      tree.loadTree(rootSpace);

      byte[] hot = key("key-00100");
      putTracked(tree, hot, 987654);

      // Simulate GC clearing soft references BEFORE the buffered write is flushed. The pinned path
      // must keep the mutated leaf reachable so reads do not see the stale on-disk value.
      tree.clearSoftReferences();
      assertEquals(Integer.valueOf(987654), tree.get(hot),
            "Pinned dirty node must not be reloaded stale before flush");

      tree.flush();
      tree.clearSoftReferences();
      assertEquals(Integer.valueOf(987654), tree.get(hot),
            "After flush the overwrite must be persisted and survive reload");
      assertEquals(Integer.valueOf(101), tree.get(key("key-00101")), "Neighbour value must be intact");
   }

   public void testDeferredStructuralInsertsRoundTrip() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      BufferedSoftBPlusTree<Integer> tree = newBufferedTree(store);

      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }
      assertEquals(0, store.writeCount, "Buffered structural inserts must not write before flush");
      assertTrue(tree.isDirty());

      tree.flush();
      assertFalse(tree.isDirty());
      assertTrue(store.writeCount > 0, "Flush must persist the buffered nodes");

      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();
      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      loaded.loadTree(rootSpace);
      for (int i = 0; i < count; i++) {
         assertEquals(Integer.valueOf(i), loaded.get(key(String.format("key-%05d", i))));
      }
   }

   public void testDeferredCoalescingReducesWritesVsEager() throws IOException {
      int count = 200;
      int overwrites = 500;
      int hotKeys = 20;

      InMemoryNodeStore eagerStore = new InMemoryNodeStore();
      SoftBPlusTree<Integer> eager = buildTree(eagerStore, count);
      eagerStore.writeCount = 0;
      for (int n = 0; n < overwrites; n++) {
         putTracked(eager, key(String.format("key-%05d", n % hotKeys)), 100000 + n);
      }
      int eagerWrites = eagerStore.writeCount;

      InMemoryNodeStore deferredStore = new InMemoryNodeStore();
      BufferedSoftBPlusTree<Integer> deferred = buildBufferedTree(deferredStore, count);
      deferredStore.writeCount = 0;
      for (int n = 0; n < overwrites; n++) {
         putTracked(deferred, key(String.format("key-%05d", n % hotKeys)), 100000 + n);
      }
      assertEquals(0, deferredStore.writeCount, "No writes before flush");
      deferred.flush();
      int deferredWrites = deferredStore.writeCount;

      assertTrue(deferredWrites < eagerWrites / 5,
            "Deferred coalesced writes (" + deferredWrites + ") should be far fewer than eager (" + eagerWrites + ")");

      // Latest value for hot key j is the largest n < overwrites with n % hotKeys == j.
      for (int j = 0; j < hotKeys; j++) {
         int lastN = overwrites - hotKeys + j;
         assertEquals(Integer.valueOf(100000 + lastN), deferred.get(key(String.format("key-%05d", j))));
      }
   }

   public void testDeferredFreedOverwriteLeafDropsBufferedWrite() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      BufferedSoftBPlusTree<Integer> tree = buildBufferedTree(store, 200);

      byte[] hot = key("key-00100");
      putTracked(tree, hot, 555);           // buffered value overwrite (pins path, marks leaf dirty)
      assertEquals(555, tree.remove(hot).intValue()); // structural removal frees that leaf

      tree.flush();
      assertNull(tree.get(hot), "Removed key must be gone despite the earlier buffered overwrite");
      assertEquals(Integer.valueOf(99), tree.get(key("key-00099")));
      assertEquals(Integer.valueOf(101), tree.get(key("key-00101")));

      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();
      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      loaded.loadTree(rootSpace);
      assertNull(loaded.get(hot));
      assertEquals(Integer.valueOf(101), loaded.get(key("key-00101")));
   }

   public void testDeferredMixedDifferentialAgainstModel() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      BufferedSoftBPlusTree<Integer> tree = newBufferedTree(store);

      Map<String, Integer> model = new TreeMap<>();
      int value = 1;
      // Interleave inserts, overwrites, removes and periodic flushes against a small key space.
      for (int round = 0; round < 40; round++) {
         for (int k = 0; k < 32; k++) {
            String ks = String.format("key-%05d", k);
            byte[] kb = key(ks);
            if ((round + k) % 5 == 0 && model.containsKey(ks)) {
               tree.remove(kb);
               model.remove(ks);
            } else {
               int v = value++;
               putTracked(tree, kb, v);
               model.put(ks, v);
            }
         }
         if (round % 7 == 0) {
            tree.flush();
            tree.clearSoftReferences();
         }
         for (Map.Entry<String, Integer> e : model.entrySet()) {
            assertEquals(e.getValue(), tree.get(key(e.getKey())), "Mismatch at round " + round + " key " + e.getKey());
         }
      }

      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();
      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      loaded.loadTree(rootSpace);
      loaded.clearSoftReferences();
      for (Map.Entry<String, Integer> e : model.entrySet()) {
         assertEquals(e.getValue(), loaded.get(key(e.getKey())), "Mismatch after reload for key " + e.getKey());
      }
   }

   /**
    * Exercises heavy deferred-mode structural churn (removes freeing blocks, inserts reusing/extending them)
    * with periodic flush + save, mirroring SIFS operation followed by a graceful restart. After each flush the
    * persisted tree must be self-consistent, so a fresh reload over a store snapshot (disk-only reads, no
    * in-memory SoftReferences to mask corruption) must match the model exactly. The deferred-free ordering bug
    * that motivated the deferred {@code pendingFrees} handling is caught at the SIFS level by the restart suites;
    * this keeps broad tree-level coverage of the free/reuse/reload interaction.
    */
   public void testDeferredStructuralChurnSurvivesReloadFromSnapshot() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      BufferedSoftBPlusTree<Integer> tree = buildBufferedTree(store, 500);
      tree.saveTree();

      Map<String, Integer> model = new TreeMap<>();
      for (int i = 0; i < 500; i++) {
         model.put(String.format("key-%05d", i), i);
      }

      int value = 100000;
      // Heavy interleaved churn with periodic flush + save, mirroring SIFS operation followed by a graceful
      // restart. After each flush the on-disk tree (root + persisted nodes) must be self-consistent, so a
      // fresh reload over a snapshot at that point must match the model exactly.
      for (int round = 0; round < 30; round++) {
         for (int k = 0; k < 500; k += 3) {
            String ks = String.format("key-%05d", k);
            if (tree.remove(key(ks)) != null) {
               model.remove(ks);
            }
         }
         for (int k = 0; k < 500; k += 2) {
            String ks = String.format("key-%05d", k);
            int v = value++;
            putTracked(tree, key(ks), v);
            model.put(ks, v);
         }
         if (round % 5 == 0) {
            tree.flush();
            SoftBPlusTree.NodeSpace saved = tree.saveTree();
            // Reload from a snapshot so every read must come from disk, not surviving in-memory nodes.
            SoftBPlusTree<Integer> reloaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store.snapshot(),
                  INT_SERIALIZER, keyLoader);
            reloaded.loadTree(saved);
            reloaded.clearSoftReferences();
            for (Map.Entry<String, Integer> e : model.entrySet()) {
               assertEquals(e.getValue(), reloaded.get(key(e.getKey())),
                     "Mismatch after reload at round " + round + " for key " + e.getKey());
            }
         }
      }

      tree.flush();
      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();
      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store.snapshot(), INT_SERIALIZER,
            keyLoader);
      loaded.loadTree(rootSpace);
      loaded.clearSoftReferences();
      for (Map.Entry<String, Integer> e : model.entrySet()) {
         assertEquals(e.getValue(), loaded.get(key(e.getKey())), "Mismatch after final reload for key " + e.getKey());
      }
   }

   public void testBufferedAutoFlushesAtMutationCount() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      int flushCount = 50;
      BufferedSoftBPlusTree<Integer> tree =
            new BufferedSoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader, flushCount);

      // Below the threshold nothing is written and the tree stays dirty.
      for (int i = 0; i < flushCount - 1; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }
      assertEquals(0, store.writeCount, "No writes before the mutation count reaches the flush threshold");
      assertTrue(tree.isDirty(), "Tree should be dirty with buffered mutations");

      // The Nth mutation trips the auto-flush.
      putTracked(tree, key(String.format("key-%05d", flushCount - 1)), flushCount - 1);
      assertTrue(store.writeCount > 0, "Reaching the flush threshold must trigger an automatic flush");
      assertFalse(tree.isDirty(), "Tree should be clean right after an automatic flush");

      // Buffered then auto-flushed values must all round-trip through a reload.
      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();
      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      loaded.loadTree(rootSpace);
      loaded.clearSoftReferences();
      for (int i = 0; i < flushCount; i++) {
         assertEquals(Integer.valueOf(i), loaded.get(key(String.format("key-%05d", i))));
      }
   }

   public void testBufferedSameKeyOverwritesDoNotAdvanceFlushThreshold() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      // Persist a multi-node tree then load it so its children are SoftNodes (the real overwrite path).
      SoftBPlusTree.NodeSpace rootSpace = buildTree(store, 200).saveTree();

      int flushCount = 5;
      BufferedSoftBPlusTree<Integer> tree =
            new BufferedSoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader, flushCount);
      tree.loadTree(rootSpace);

      store.writeCount = 0;
      byte[] hot = key("key-00100");
      // Overwrite the SAME key far more than flushCount times. Each coalesces into the same dirty leaf, which
      // counts as a single unit, so the threshold is never reached and no automatic flush happens.
      for (int v = 0; v < flushCount * 10; v++) {
         putTracked(tree, hot, v);
      }
      assertEquals(0, store.writeCount, "Repeated same-key overwrites must not trigger an auto-flush");
      assertTrue(tree.isDirty(), "Repeated same-key overwrites should remain buffered");
      assertEquals(Integer.valueOf(flushCount * 10 - 1), tree.get(hot), "Reads must see the latest buffered value");

      // A manual flush writes the single coalesced leaf, and the latest value survives a reload.
      tree.flush();
      assertFalse(tree.isDirty(), "Tree should be clean after flush");
      assertTrue(store.writeCount > 0 && store.writeCount <= 2,
            "Coalesced overwrite should flush in ~1 in-place write, was " + store.writeCount);

      SoftBPlusTree.NodeSpace newRoot = tree.saveTree();
      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      loaded.loadTree(newRoot);
      assertEquals(Integer.valueOf(flushCount * 10 - 1), loaded.get(hot), "Latest coalesced value must survive reload");
   }

   public void testSoftNodeGetInsertionPoint() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();
      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      loaded.loadTree(rootSpace);
      loaded.clearSoftReferences();

      BPlusTree.InnerNode<Integer> root = (BPlusTree.InnerNode<Integer>) loaded.getRoot();
      SoftBPlusTree.SoftNode<Integer> softChild = (SoftBPlusTree.SoftNode<Integer>) root.children[0];

      int point = softChild.getInsertionPoint(key("key-00000"));
      assertTrue(point >= 0, "Insertion point should be >= 0");
   }

   public void testSoftNodeRightmostKey() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();
      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      loaded.loadTree(rootSpace);
      loaded.clearSoftReferences();

      BPlusTree.InnerNode<Integer> root = (BPlusTree.InnerNode<Integer>) loaded.getRoot();
      int lastIdx = root.children.length - 1;
      SoftBPlusTree.SoftNode<Integer> lastChild = (SoftBPlusTree.SoftNode<Integer>) root.children[lastIdx];

      byte[] rightmost = lastChild.rightmostKey();
      assertEquals("key-00199", new String(rightmost, StandardCharsets.UTF_8), "Rightmost key of last child should be the last key in the tree");
   }

   public void testSoftNodeSplit() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();
      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      loaded.loadTree(rootSpace);
      loaded.clearSoftReferences();

      BPlusTree.InnerNode<Integer> root = (BPlusTree.InnerNode<Integer>) loaded.getRoot();
      SoftBPlusTree.SoftNode<Integer> softChild = (SoftBPlusTree.SoftNode<Integer>) root.children[0];

      List<BPlusTree.Node<Integer>> splitResult = softChild.split(MAX_NODE_SIZE);
      assertFalse(splitResult.isEmpty(), "Split should return at least one node");
   }

   public void testLeafSerializedSize() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      putTracked(tree, key("a"), 1);
      putTracked(tree, key("b"), 2);
      putTracked(tree, key("c"), 3);

      BPlusTree.Node<Integer> root = tree.getRoot();
      assertInstanceOf(BPlusTree.LeafNode.class, root, "Root should be LeafNode for small tree");

      ByteBuffer serialized = SoftBPlusTree.serializeNode(root, INT_SERIALIZER);
      int expectedSize = BPlusTree.INNER_NODE_HEADER_SIZE + 3 * INT_SERIALIZER.serializedSize(1);
      assertEquals(expectedSize, serialized.remaining(), "Leaf serialized size should be header + values");
   }

   public void testFreeBlockReuse() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);

      for (int i = 0; i < 500; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }
      long sizeAfterInsert = tree.getStoreSize();

      for (int i = 0; i < 250; i++) {
         tree.remove(key(String.format("key-%05d", i)));
      }

      for (int i = 500; i < 750; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }
      long sizeAfterReinsert = tree.getStoreSize();

      long growth = sizeAfterReinsert - sizeAfterInsert;
      assertTrue(growth < sizeAfterInsert, "Free block reuse should limit growth. Initial: " + sizeAfterInsert +
            ", after reinsert: " + sizeAfterReinsert + ", growth: " + growth);
   }

   // --- IndexNodeOutdatedException retry tests ---

   public void testGetRetriesOnOutdatedNode() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }
      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();

      AtomicInteger failsRemaining = new AtomicInteger(2);
      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store.snapshot(), INT_SERIALIZER,
            value -> {
               if (failsRemaining.getAndDecrement() > 0) return null;
               return keysByValue.get(value);
            });
      loaded.loadTree(rootSpace);
      loaded.clearSoftReferences();

      assertEquals(Integer.valueOf(100), loaded.get(key("key-00100")));
      assertTrue(failsRemaining.get() < 0, "Should have retried at least twice");
   }

   public void testGetExceedsMaxRetriesOnOutdatedNode() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }
      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();

      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store.snapshot(), INT_SERIALIZER,
            value -> null);
      loaded.loadTree(rootSpace);
      loaded.clearSoftReferences();

      try {
         loaded.get(key("key-00100"));
         throw new AssertionError("Should have thrown IndexNodeOutdatedException");
      } catch (SoftBPlusTree.IndexNodeOutdatedException e) {
         // expected after 11 failed attempts
      }
   }

   public void testPublishRetriesOnOutdatedFunction() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      for (int i = 0; i < 10; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      AtomicInteger attempts = new AtomicInteger(0);
      List<Integer> values = tree.<Integer>publish((k, v) -> {
         if (v == 0 && attempts.getAndIncrement() < 2) {
            throw new SoftBPlusTree.IndexNodeOutdatedException("transient");
         }
         return v;
      }).toList().blockingGet();

      assertEquals(10, values.size());
      assertEquals(Integer.valueOf(0), values.get(0));
      assertEquals(Integer.valueOf(9), values.get(9));
      assertTrue(attempts.get() >= 3, "Should have retried at least twice");
   }

   public void testPublishExceedsMaxRetries() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      for (int i = 0; i < 10; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      try {
         tree.<Integer>publish((k, v) -> {
            throw new SoftBPlusTree.IndexNodeOutdatedException("always outdated");
         }).toList().blockingGet();
         throw new AssertionError("Should have thrown");
      } catch (RuntimeException e) {
         Throwable cause = e;
         while (cause != null && !(cause instanceof IllegalStateException)) {
            cause = cause.getCause();
         }
         assertInstanceOf(IllegalStateException.class, cause, "Should contain IllegalStateException in cause chain, got: " + e);
         assertTrue(cause.getMessage().contains("retry attempts"), "Message should mention retry attempts");
      }
   }

   public void testPublishSkipOnOutdated() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      for (int i = 0; i < 10; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      AtomicInteger outdatedCount = new AtomicInteger(0);
      List<Integer> values = tree.<Integer>publish((k, v) -> {
         if (v == 3 || v == 7) {
            outdatedCount.incrementAndGet();
            throw new SoftBPlusTree.IndexNodeOutdatedException("outdated");
         }
         return v;
      }, true).toList().blockingGet();

      assertEquals(8, values.size(), "Should skip outdated entries");
      assertFalse(values.contains(3), "Should not contain skipped value 3");
      assertFalse(values.contains(7), "Should not contain skipped value 7");
      assertEquals(2, outdatedCount.get(), "Each outdated entry should be encountered once");
   }

   public void testPublishSkipOnOutdatedRetriesNodeResolution() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }
      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();

      AtomicInteger failsRemaining = new AtomicInteger(2);
      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store.snapshot(), INT_SERIALIZER,
            value -> {
               if (failsRemaining.getAndDecrement() > 0) return null;
               return keysByValue.get(value);
            });
      loaded.loadTree(rootSpace);
      loaded.clearSoftReferences();

      List<Integer> values = loaded.publish((k, v) -> v, true).toList().blockingGet();
      assertEquals(count, values.size());
      assertTrue(failsRemaining.get() < 0, "Should have retried node resolution");
   }

   public void testPublishRetriesOnOutdatedNode() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }
      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();

      AtomicInteger failsRemaining = new AtomicInteger(2);
      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store.snapshot(), INT_SERIALIZER,
            value -> {
               if (failsRemaining.getAndDecrement() > 0) return null;
               return keysByValue.get(value);
            });
      loaded.loadTree(rootSpace);
      loaded.clearSoftReferences();

      List<Integer> values = loaded.publish((k, v) -> v).toList().blockingGet();
      assertEquals(count, values.size());
      assertTrue(failsRemaining.get() < 0, "Should have retried");
   }

   /**
    * With minNodeSize=0, tryInPlaceLeafUpdate can persist an empty leaf to disk
    * when the leaf is the leftmost child (index 0) — the leftmost-key-change check
    * is skipped, so the empty leaf bypasses applyModification entirely. Without the
    * guard in tryInPlaceLeafUpdate, subsequent puts that cause an ancestor to split
    * can produce a piece with only empty-leaf descendants, throwing
    * IllegalStateException from copyWith.
    */
   public void testEmptyLeafViaInPlaceUpdateWithMinNodeSizeZero() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(0, 60, store, INT_SERIALIZER, keyLoader);
      String prefix = "RBROOT/page";
      int maxPages = 30;
      int entitiesPerPage = 8;

      // Phase 1: Populate all pages
      for (int page = 0; page < maxPages; page++) {
         for (int e = 0; e < entitiesPerPage; e++) {
            String k = String.format("%s:%02d:entity:%04d", prefix, page, e);
            putTracked(tree, key(k), page * 100 + e);
         }
      }

      // Phase 2: Remove all entries starting from page 0
      // When a leaf at index 0 (leftmost child) is emptied, tryInPlaceLeafUpdate
      // skips the leftmost-key-change check and persists the empty leaf to disk,
      // bypassing applyModification and manageLength entirely
      for (int page = 0; page < 20; page++) {
         for (int e = 0; e < entitiesPerPage; e++) {
            String k = String.format("%s:%02d:entity:%04d", prefix, page, e);
            tree.remove(key(k));
         }
      }

      // Phase 3: Insert more entries to force ancestor splits
      for (int page = 0; page < 5; page++) {
         for (int e = entitiesPerPage; e < 80; e++) {
            String k = String.format("%s:%02d:entity:%04d", prefix, page, e);
            putTracked(tree, key(k), page * 100 + e);
         }
      }
      for (int page = 25; page < 30; page++) {
         for (int e = entitiesPerPage; e < 80; e++) {
            String k = String.format("%s:%02d:entity:%04d", prefix, page, e);
            putTracked(tree, key(k), page * 100 + e);
         }
      }

      // Verify all remaining entries are correct
      // Pages 0-4: entities 0-7 were removed in Phase 2, only 8-79 remain
      for (int page = 0; page < 5; page++) {
         for (int e = entitiesPerPage; e < 80; e++) {
            String k = String.format("%s:%02d:entity:%04d", prefix, page, e);
            assertEquals(Integer.valueOf(page * 100 + e), tree.get(key(k)), "get(" + k + ")");
         }
      }
      for (int page = 25; page < 30; page++) {
         for (int e = 0; e < 80; e++) {
            String k = String.format("%s:%02d:entity:%04d", prefix, page, e);
            assertEquals(Integer.valueOf(page * 100 + e), tree.get(key(k)), "get(" + k + ")");
         }
      }
   }

   public void testSerializeDeserializeFreeBlocksRoundTrip() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);

      for (int i = 0; i < 500; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }
      for (int i = 0; i < 250; i++) {
         tree.remove(key(String.format("key-%05d", i)));
      }

      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();
      long storeSizeBefore = tree.getStoreSize();
      ByteBuffer freeBlocksBuf = tree.serializeFreeBlocks();
      assertTrue(freeBlocksBuf.remaining() > 4, "Free blocks buffer should contain data");

      InMemoryNodeStore loadStore = store.snapshot();
      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, loadStore, INT_SERIALIZER, keyLoader);
      loaded.setStoreSize(storeSizeBefore);
      loaded.deserializeFreeBlocks(freeBlocksBuf);
      loaded.loadTree(rootSpace);

      for (int i = 250; i < 500; i++) {
         assertEquals(Integer.valueOf(i), loaded.get(key(String.format("key-%05d", i))));
      }

      long sizeBeforeReinsert = loaded.getStoreSize();
      for (int i = 0; i < 250; i++) {
         putTracked(loaded, key(String.format("key-%05d", i)), i + 1000);
      }
      long sizeAfterReinsert = loaded.getStoreSize();

      assertTrue(sizeAfterReinsert - sizeBeforeReinsert < sizeBeforeReinsert, "Restored free blocks should be reused, limiting growth. Before: " + sizeBeforeReinsert +
            ", after: " + sizeAfterReinsert);
   }

   // --- Alignment padding coverage test ---

   /**
    * A NodeStore that tracks the high-water mark of writes and throws IOException
    * when reads go past the written extent, simulating FileChannel EOF behavior.
    * This is how {@link org.infinispan.persistence.sifs.Index.IndexFileNodeStore}
    * behaves: FileChannel.read() returns -1 when reading past the file's actual size.
    */
   static class FileSimulatingNodeStore implements SoftBPlusTree.NodeStore {
      private byte[] data = new byte[4096];
      private long fileSize = 0;

      @Override
      public void write(ByteBuffer buf, long offset) {
         int length = buf.remaining();
         ensureCapacity(offset + length);
         buf.get(data, (int) offset, length);
         long writeEnd = offset + length;
         if (writeEnd > fileSize) {
            fileSize = writeEnd;
         }
      }

      @Override
      public ByteBuffer read(long offset, int length) throws IOException {
         if (offset >= fileSize) {
            throw new IOException("Truncated node store at offset " + offset);
         }
         if (offset + length > fileSize) {
            throw new IOException("Truncated node store at offset " + offset
                  + " (requested " + length + " bytes, file size " + fileSize + ")");
         }
         byte[] result = new byte[length];
         System.arraycopy(data, (int) offset, result, 0, length);
         return ByteBuffer.wrap(result);
      }

      @Override
      public void truncate(long size) {
         fileSize = size;
      }

      private void ensureCapacity(long required) {
         if (required > data.length) {
            int newLen = Math.max((int) required, data.length * 2);
            byte[] newData = new byte[newLen];
            System.arraycopy(data, 0, newData, 0, data.length);
            data = newData;
         }
      }
   }

   /**
    * Reproduces the "Truncated node store at offset" bug. When a node's serialized
    * size is not a multiple of the block alignment, writeNodeToStore() must write the
    * full aligned block (not just the serialized bytes) so the file extends to cover
    * the SoftNode's occupiedSpace. Without this fix, the last block written by
    * persistNewNodes() leaves a gap between serialized data and the aligned block end,
    * causing SoftNode.resolve() to hit EOF after GC clears the SoftReference.
    */
   public void testAlignmentPaddingWrittenToStore() throws IOException {
      short blockAlignment = 64;
      long headerSize = 26;
      FileSimulatingNodeStore store = new FileSimulatingNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE,
            store, INT_SERIALIZER, keyLoader, blockAlignment, headerSize);

      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      tree.clearSoftReferences();

      for (int i = 0; i < count; i++) {
         assertEquals(Integer.valueOf(i), tree.get(key(String.format("key-%05d", i))), "Value for key-" + String.format("%05d", i) + " should resolve from disk");
      }
   }
}
