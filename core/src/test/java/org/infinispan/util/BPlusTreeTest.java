package org.infinispan.util;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

import org.testng.annotations.Test;

@Test(groups = "unit", testName = "util.BPlusTreeTest")
public class BPlusTreeTest {

   private static final int MIN_NODE_SIZE = 15;
   private static final int MAX_NODE_SIZE = 60;

   private BPlusTree<String> createTree() {
      return new BPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE);
   }

   private static byte[] key(String s) {
      return s.getBytes(StandardCharsets.UTF_8);
   }

   public void testEmptyTree() throws IOException {
      BPlusTree<String> tree = createTree();
      assertEquals(0, tree.size());
      assertNull(tree.get(key("anything")));
   }

   public void testSinglePutAndGet() throws IOException {
      BPlusTree<String> tree = createTree();
      assertNull(tree.put(key("hello"), "world"));
      assertEquals(1, tree.size());
      assertEquals("world", tree.get(key("hello")));
      assertNull(tree.get(key("other")));
   }

   public void testPutOverwrite() throws IOException {
      BPlusTree<String> tree = createTree();
      assertNull(tree.put(key("key"), "value1"));
      assertEquals("value1", tree.put(key("key"), "value2"));
      assertEquals(1, tree.size());
      assertEquals("value2", tree.get(key("key")));
   }

   public void testMultiplePuts() throws IOException {
      BPlusTree<String> tree = createTree();
      assertNull(tree.put(key("alpha"), "a"));
      assertNull(tree.put(key("beta"), "b"));
      assertNull(tree.put(key("gamma"), "g"));
      assertEquals(3, tree.size());
      assertEquals("a", tree.get(key("alpha")));
      assertEquals("b", tree.get(key("beta")));
      assertEquals("g", tree.get(key("gamma")));
   }

   public void testRemove() throws IOException {
      BPlusTree<String> tree = createTree();
      tree.put(key("a"), "1");
      tree.put(key("b"), "2");
      tree.put(key("c"), "3");

      assertEquals("2", tree.remove(key("b")));
      assertEquals(2, tree.size());
      assertNull(tree.get(key("b")));
      assertEquals("1", tree.get(key("a")));
      assertEquals("3", tree.get(key("c")));
   }

   public void testRemoveNonExistent() throws IOException {
      BPlusTree<String> tree = createTree();
      tree.put(key("a"), "1");
      assertNull(tree.remove(key("z")));
      assertEquals(1, tree.size());
   }

   public void testRemoveFromEmptyTree() throws IOException {
      BPlusTree<String> tree = createTree();
      assertNull(tree.remove(key("a")));
      assertEquals(0, tree.size());
   }

   public void testClear() throws IOException {
      BPlusTree<String> tree = createTree();
      tree.put(key("a"), "1");
      tree.put(key("b"), "2");
      tree.clear();
      assertEquals(0, tree.size());
      assertNull(tree.get(key("a")));
      assertNull(tree.get(key("b")));
   }

   public void testPublish() throws IOException {
      BPlusTree<String> tree = createTree();
      tree.put(key("c"), "3");
      tree.put(key("a"), "1");
      tree.put(key("b"), "2");

      List<String> keys = tree.<String>publish((k, v) -> new String(k, StandardCharsets.UTF_8))
            .toList().blockingGet();
      assertEquals(3, keys.size());
      assertEquals("a", keys.get(0));
      assertEquals("b", keys.get(1));
      assertEquals("c", keys.get(2));
   }

   public void testPublishSortedValues() throws IOException {
      BPlusTree<Integer> tree = new BPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE);
      int count = 100;
      for (int i = 0; i < count; i++) {
         tree.put(key(String.format("key-%05d", i)), i);
      }

      List<Integer> values = tree.<Integer>publish((k, v) -> v).toList().blockingGet();
      assertEquals(count, values.size());
      for (int i = 1; i < values.size(); i++) {
         assertTrue("Values should follow sorted key order", values.get(i - 1) < values.get(i));
      }
   }

   public void testManyInsertions() throws IOException {
      BPlusTree<Integer> tree = new BPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE);
      int count = 200;
      for (int i = 0; i < count; i++) {
         String k = String.format("key-%05d", i);
         assertNull(tree.put(key(k), i));
      }
      assertEquals(count, tree.size());
      for (int i = 0; i < count; i++) {
         String k = String.format("key-%05d", i);
         assertEquals(Integer.valueOf(i), tree.get(key(k)));
      }
   }

   public void testManyInsertionsReverseOrder() throws IOException {
      BPlusTree<Integer> tree = new BPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE);
      int count = 200;
      for (int i = count - 1; i >= 0; i--) {
         String k = String.format("key-%05d", i);
         assertNull(tree.put(key(k), i));
      }
      assertEquals(count, tree.size());
      for (int i = 0; i < count; i++) {
         String k = String.format("key-%05d", i);
         assertEquals(Integer.valueOf(i), tree.get(key(k)));
      }
   }

   public void testManyInsertionsRandomOrder() throws IOException {
      BPlusTree<Integer> tree = new BPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE);
      int count = 500;
      List<Integer> indices = new ArrayList<>();
      for (int i = 0; i < count; i++) indices.add(i);
      Collections.shuffle(indices, ThreadLocalRandom.current());

      for (int i : indices) {
         String k = String.format("key-%05d", i);
         assertNull(tree.put(key(k), i));
      }
      assertEquals(count, tree.size());
      for (int i = 0; i < count; i++) {
         String k = String.format("key-%05d", i);
         assertEquals(Integer.valueOf(i), tree.get(key(k)));
      }
   }

   public void testInsertAndRemoveAll() throws IOException {
      BPlusTree<Integer> tree = new BPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE);
      int count = 100;
      for (int i = 0; i < count; i++) {
         tree.put(key("k" + i), i);
      }
      assertEquals(count, tree.size());
      for (int i = 0; i < count; i++) {
         assertEquals(Integer.valueOf(i), tree.remove(key("k" + i)));
      }
      assertEquals(0, tree.size());
   }

   public void testInsertAndRemoveRandomOrder() throws IOException {
      BPlusTree<Integer> tree = new BPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE);
      int count = 200;
      List<Integer> indices = new ArrayList<>();
      for (int i = 0; i < count; i++) {
         indices.add(i);
         tree.put(key(String.format("key-%05d", i)), i);
      }
      Collections.shuffle(indices, ThreadLocalRandom.current());
      for (int i : indices) {
         assertEquals(Integer.valueOf(i), tree.remove(key(String.format("key-%05d", i))));
      }
      assertEquals(0, tree.size());
   }

   public void testPublishSortedOrder() throws IOException {
      BPlusTree<Integer> tree = new BPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE);
      int count = 100;
      List<Integer> indices = new ArrayList<>();
      for (int i = 0; i < count; i++) indices.add(i);
      Collections.shuffle(indices, ThreadLocalRandom.current());
      for (int i : indices) {
         tree.put(key(String.format("key-%05d", i)), i);
      }

      List<String> keys = tree.<String>publish((k, v) -> new String(k, StandardCharsets.UTF_8))
            .toList().blockingGet();

      for (int i = 1; i < keys.size(); i++) {
         assertTrue("Keys should be in sorted order: " + keys.get(i - 1) + " vs " + keys.get(i),
               keys.get(i - 1).compareTo(keys.get(i)) < 0);
      }
   }

   public void testPrefixKeys() throws IOException {
      BPlusTree<String> tree = createTree();
      tree.put(key("a"), "1");
      tree.put(key("ab"), "2");
      tree.put(key("abc"), "3");
      tree.put(key("abcd"), "4");

      assertEquals("1", tree.get(key("a")));
      assertEquals("2", tree.get(key("ab")));
      assertEquals("3", tree.get(key("abc")));
      assertEquals("4", tree.get(key("abcd")));
      assertEquals(4, tree.size());
   }

   public void testEmptyKey() throws IOException {
      BPlusTree<String> tree = createTree();
      tree.put(new byte[0], "empty");
      tree.put(key("a"), "a");
      assertEquals("empty", tree.get(new byte[0]));
      assertEquals("a", tree.get(key("a")));
   }

   public void testSingleByteKeys() throws IOException {
      BPlusTree<Integer> tree = new BPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE);
      for (int i = 0; i < 256; i++) {
         tree.put(new byte[]{(byte) i}, i);
      }
      assertEquals(256, tree.size());
      for (int i = 0; i < 256; i++) {
         assertEquals(Integer.valueOf(i), tree.get(new byte[]{(byte) i}));
      }
   }

   public void testMixedInsertRemoveGet() throws IOException {
      for (long seed : new long[]{12345L, 99999L, 42L, 0L, 987654321L}) {
         mixedInsertRemoveGet(seed, MIN_NODE_SIZE, MAX_NODE_SIZE);
      }
   }

   public void testMixedInsertRemoveGetSmallNodes() throws IOException {
      for (long seed : new long[]{12345L, 99999L, 42L, 0L, 987654321L}) {
         mixedInsertRemoveGet(seed, 15, 30);
      }
   }

   private void mixedInsertRemoveGet(long seed, int minNode, int maxNode) throws IOException {
      java.util.Random rng = new java.util.Random(seed);
      BPlusTree<Integer> tree = new BPlusTree<>(minNode, maxNode);
      TreeMap<String, Integer> reference = new TreeMap<>();

      for (int op = 0; op < 2000; op++) {
         int action = rng.nextInt(3);
         String k = "key-" + rng.nextInt(100);
         switch (action) {
            case 0: // put
               int val = rng.nextInt(10000);
               Integer oldRef = reference.put(k, val);
               Integer oldTree = tree.put(key(k), val);
               assertEquals("seed=" + seed + " put(" + k + ") old value mismatch at op " + op, oldRef, oldTree);
               break;
            case 1: // get
               Integer refVal = reference.get(k);
               Integer treeVal = tree.get(key(k));
               assertEquals("seed=" + seed + " get(" + k + ") mismatch at op " + op, refVal, treeVal);
               break;
            case 2: // remove
               Integer removedRef = reference.remove(k);
               Integer removedTree = tree.remove(key(k));
               assertEquals("seed=" + seed + " remove(" + k + ") mismatch at op " + op, removedRef, removedTree);
               break;
         }
         assertEquals("seed=" + seed + " size mismatch at op " + op, reference.size(), tree.size());
      }

      for (Map.Entry<String, Integer> entry : reference.entrySet()) {
         assertEquals(entry.getValue(), tree.get(key(entry.getKey())));
      }
   }

   public void testCompareFunction() {
      assertEquals(0, BPlusTree.compare(key("abc"), key("abc")));

      int cmp = BPlusTree.compare(key("abc"), key("abd"));
      assertTrue("abc < abd should give positive result", cmp > 0);
      assertEquals(3, cmp);

      cmp = BPlusTree.compare(key("abd"), key("abc"));
      assertTrue("abd > abc should give negative result", cmp < 0);
      assertEquals(-3, cmp);

      cmp = BPlusTree.compare(key("ab"), key("abc"));
      assertTrue("ab < abc (prefix) should give positive result", cmp > 0);
      assertEquals(3, cmp);

      cmp = BPlusTree.compare(key("abc"), key("ab"));
      assertTrue("abc > ab should give negative result", cmp < 0);
      assertEquals(-3, cmp);
   }

   public void testLargeNodeSizes() throws IOException {
      BPlusTree<Integer> tree = new BPlusTree<>(100, 4096);
      int count = 1000;
      for (int i = 0; i < count; i++) {
         tree.put(key(String.format("key-%08d", i)), i);
      }
      assertEquals(count, tree.size());
      for (int i = 0; i < count; i++) {
         assertEquals(Integer.valueOf(i), tree.get(key(String.format("key-%08d", i))));
      }
   }

   public void testSmallNodeSizes() throws IOException {
      BPlusTree<Integer> tree = new BPlusTree<>(15, 30);
      int count = 100;
      for (int i = 0; i < count; i++) {
         tree.put(key(String.format("key-%05d", i)), i);
      }
      assertEquals(count, tree.size());
      for (int i = 0; i < count; i++) {
         assertEquals(Integer.valueOf(i), tree.get(key(String.format("key-%05d", i))));
      }
   }

   public void testRepeatedClearAndInsert() throws IOException {
      BPlusTree<Integer> tree = new BPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE);
      for (int round = 0; round < 5; round++) {
         for (int i = 0; i < 50; i++) {
            tree.put(key("key-" + i), i);
         }
         assertEquals(50, tree.size());
         tree.clear();
         assertEquals(0, tree.size());
      }
   }

   public void testJoinOnRemoveLeftmostChild() throws IOException {
      BPlusTree<Integer> tree = new BPlusTree<>(15, 30);
      int count = 60;
      for (int i = 0; i < count; i++) {
         tree.put(key(String.format("key-%05d", i)), i);
      }

      // Remove the smallest keys to trigger underflow in the leftmost child
      for (int i = 0; i < 20; i++) {
         assertEquals(Integer.valueOf(i), tree.remove(key(String.format("key-%05d", i))));
      }
      assertEquals(count - 20, tree.size());
      for (int i = 20; i < count; i++) {
         assertEquals(Integer.valueOf(i), tree.get(key(String.format("key-%05d", i))));
      }
   }

   public void testJoinOnRemoveRightmostChild() throws IOException {
      BPlusTree<Integer> tree = new BPlusTree<>(15, 30);
      int count = 60;
      for (int i = 0; i < count; i++) {
         tree.put(key(String.format("key-%05d", i)), i);
      }

      // Remove the largest keys to trigger underflow in the rightmost child
      for (int i = count - 1; i >= 40; i--) {
         assertEquals(Integer.valueOf(i), tree.remove(key(String.format("key-%05d", i))));
      }
      assertEquals(40, tree.size());
      for (int i = 0; i < 40; i++) {
         assertEquals(Integer.valueOf(i), tree.get(key(String.format("key-%05d", i))));
      }
   }

   public void testJoinOnRemoveMiddleChild() throws IOException {
      BPlusTree<Integer> tree = new BPlusTree<>(15, 30);
      int count = 100;
      for (int i = 0; i < count; i++) {
         tree.put(key(String.format("key-%05d", i)), i);
      }

      // Remove keys from the middle range to force underflow and join of middle children
      for (int i = 30; i < 60; i++) {
         assertEquals(Integer.valueOf(i), tree.remove(key(String.format("key-%05d", i))));
      }
      assertEquals(count - 30, tree.size());
      for (int i = 0; i < count; i++) {
         if (i >= 30 && i < 60) {
            assertNull(tree.get(key(String.format("key-%05d", i))));
         } else {
            assertEquals(Integer.valueOf(i), tree.get(key(String.format("key-%05d", i))));
         }
      }
   }

   public void testJoinCascadesToHigherLevels() throws IOException {
      // Very small nodes force a deeper tree and multi-level join cascades
      BPlusTree<Integer> tree = new BPlusTree<>(15, 30);
      int count = 200;
      for (int i = 0; i < count; i++) {
         tree.put(key(String.format("key-%05d", i)), i);
      }

      // Remove most entries to force deep tree collapse
      TreeMap<String, Integer> reference = new TreeMap<>();
      for (int i = 0; i < count; i++) {
         reference.put(String.format("key-%05d", i), i);
      }
      java.util.Random rng = new java.util.Random(12345L);
      List<String> keys = new ArrayList<>(reference.keySet());
      Collections.shuffle(keys, rng);
      for (int i = 0; i < 180; i++) {
         String k = keys.get(i);
         tree.remove(key(k));
         reference.remove(k);
      }

      assertEquals(reference.size(), tree.size());
      for (Map.Entry<String, Integer> entry : reference.entrySet()) {
         assertEquals(entry.getValue(), tree.get(key(entry.getKey())));
      }
   }

   // --- Inner node join tests (large minNodeSize) ---

   public void testJoinInnerNodes() throws IOException {
      // With minNodeSize=30, a 2-child inner node (length ~28 with 1-byte keys) underflows,
      // triggering inner-inner joins during cascading rebalancing.
      BPlusTree<Integer> tree = new BPlusTree<>(30, 80);
      for (int i = 0; i < 256; i++) {
         tree.put(new byte[]{(byte) i}, i);
      }
      for (int i = 0; i < 220; i++) {
         assertEquals(Integer.valueOf(i), tree.remove(new byte[]{(byte) i}));
      }
      assertEquals(36, tree.size());
      for (int i = 220; i < 256; i++) {
         assertEquals(Integer.valueOf(i), tree.get(new byte[]{(byte) i}));
      }
   }

   public void testJoinInnerNodesFromMiddle() throws IOException {
      // Remove middle entries so both siblings of an underflowed node are near-full,
      // triggering the both-exceed branch and re-split after join.
      BPlusTree<Integer> tree = new BPlusTree<>(30, 80);
      for (int i = 0; i < 256; i++) {
         tree.put(new byte[]{(byte) i}, i);
      }
      for (int i = 80; i < 200; i++) {
         tree.remove(new byte[]{(byte) i});
      }
      assertEquals(136, tree.size());
      for (int i = 0; i < 80; i++) {
         assertEquals(Integer.valueOf(i), tree.get(new byte[]{(byte) i}));
      }
      for (int i = 200; i < 256; i++) {
         assertEquals(Integer.valueOf(i), tree.get(new byte[]{(byte) i}));
      }
   }

   public void testJoinInnerNodesRandomRemoval() throws IOException {
      // Random removal order with large minNodeSize exercises all join paths:
      // inner-inner join, single-child unwrap, both-exceed, and re-split.
      BPlusTree<Integer> tree = new BPlusTree<>(30, 80);
      TreeMap<Integer, Integer> reference = new TreeMap<>();
      for (int i = 0; i < 256; i++) {
         tree.put(new byte[]{(byte) i}, i);
         reference.put(i, i);
      }
      List<Integer> keys = new ArrayList<>(reference.keySet());
      Collections.shuffle(keys, new java.util.Random(42L));
      for (int k : keys) {
         assertEquals(Integer.valueOf(k), tree.remove(new byte[]{(byte) k}));
         reference.remove(k);
         assertEquals(reference.size(), tree.size());
      }
      assertEquals(0, tree.size());
   }

   public void testMixedOpsWithInnerNodeJoin() throws IOException {
      for (long seed : new long[]{12345L, 42L, 0L, 987654321L, 99999L}) {
         mixedInsertRemoveGetByteKeys(seed, 30, 80);
      }
   }

   private void mixedInsertRemoveGetByteKeys(long seed, int minNode, int maxNode) throws IOException {
      java.util.Random rng = new java.util.Random(seed);
      BPlusTree<Integer> tree = new BPlusTree<>(minNode, maxNode);
      TreeMap<Integer, Integer> reference = new TreeMap<>();

      for (int op = 0; op < 3000; op++) {
         int action = rng.nextInt(3);
         int k = rng.nextInt(256);
         byte[] bk = new byte[]{(byte) k};
         switch (action) {
            case 0:
               int val = rng.nextInt(10000);
               Integer oldRef = reference.put(k, val);
               Integer oldTree = tree.put(bk, val);
               assertEquals("seed=" + seed + " put(" + k + ") old value mismatch at op " + op, oldRef, oldTree);
               break;
            case 1:
               Integer refVal = reference.get(k);
               Integer treeVal = tree.get(bk);
               assertEquals("seed=" + seed + " get(" + k + ") mismatch at op " + op, refVal, treeVal);
               break;
            case 2:
               Integer removedRef = reference.remove(k);
               Integer removedTree = tree.remove(bk);
               assertEquals("seed=" + seed + " remove(" + k + ") mismatch at op " + op, removedRef, removedTree);
               break;
         }
         assertEquals("seed=" + seed + " size mismatch at op " + op, reference.size(), tree.size());
      }
   }

   // --- Publish tests ---

   public void testPublishWithTake() throws IOException {
      BPlusTree<Integer> tree = new BPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE);
      for (int i = 0; i < 100; i++) {
         tree.put(key(String.format("key-%05d", i)), i);
      }

      List<Integer> values = tree.<Integer>publish((k, v) -> v).take(10).toList().blockingGet();
      assertEquals(10, values.size());
      assertEquals(Integer.valueOf(0), values.get(0));
      assertEquals(Integer.valueOf(9), values.get(9));
   }

   public void testPublishEmptyTree() throws IOException {
      BPlusTree<String> tree = createTree();

      List<String> values = tree.<String>publish((k, v) -> v).toList().blockingGet();
      assertTrue(values.isEmpty());
   }

   public void testPublishSkipNull() throws IOException {
      BPlusTree<Integer> tree = new BPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE);
      for (int i = 0; i < 20; i++) {
         tree.put(key(String.format("key-%05d", i)), i);
      }

      List<Integer> odds = tree.<Integer>publish((k, v) -> v % 2 != 0 ? v : null)
            .toList().blockingGet();
      assertEquals(10, odds.size());
      for (int v : odds) {
         assertTrue("Expected odd value", v % 2 != 0);
      }
   }

   public void testPublishAllEntries() throws IOException {
      BPlusTree<Integer> tree = new BPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE);
      int total = 200;
      for (int i = 0; i < total; i++) {
         tree.put(key(String.format("key-%05d", i)), i);
      }

      List<Integer> allValues = tree.<Integer>publish((k, v) -> v).toList().blockingGet();
      assertEquals(total, allValues.size());
      for (int i = 0; i < total; i++) {
         assertEquals(Integer.valueOf(i), allValues.get(i));
      }
   }

   public void testRemoveAllEntriesTinyNodes() throws IOException {
      BPlusTree<Integer> tree = new BPlusTree<>(10, 28);
      int count = 500;
      List<String> inserted = new ArrayList<>();
      for (int i = 0; i < count; i++) {
         String k = String.format("prefix/segment/%05d/key", i);
         tree.put(key(k), i);
         inserted.add(k);
      }
      assertEquals(count, tree.size());
      Collections.shuffle(inserted, new java.util.Random(77L));
      for (int i = 0; i < count; i++) {
         tree.remove(key(inserted.get(i)));
         assertEquals(count - i - 1, tree.size());
      }
      assertEquals(0, tree.size());
   }

   public void testMixedOpsLongKeysTinyNodes() throws IOException {
      for (long seed : new long[]{1L, 2L, 3L, 77L, 9999L}) {
         java.util.Random rng = new java.util.Random(seed);
         BPlusTree<Integer> tree = new BPlusTree<>(10, 28);
         TreeMap<String, Integer> reference = new TreeMap<>();
         for (int op = 0; op < 5000; op++) {
            int action = rng.nextInt(3);
            String k = String.format("ns/%03d/obj", rng.nextInt(200));
            byte[] bk = key(k);
            switch (action) {
               case 0:
                  int val = rng.nextInt(10000);
                  Integer oldRef = reference.put(k, val);
                  Integer oldTree = tree.put(bk, val);
                  assertEquals("seed=" + seed + " put op " + op, oldRef, oldTree);
                  break;
               case 1:
                  assertEquals("seed=" + seed + " get op " + op, reference.get(k), tree.get(bk));
                  break;
               case 2:
                  Integer removedRef = reference.remove(k);
                  Integer removedTree = tree.remove(bk);
                  assertEquals("seed=" + seed + " remove op " + op, removedRef, removedTree);
                  break;
            }
            assertEquals("seed=" + seed + " size mismatch at op " + op, reference.size(), tree.size());
         }
      }
   }

   public void testPublishRetryOnFirstEntryOutdated() throws IOException {
      BPlusTree<Integer> tree = new BPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE);
      for (int i = 0; i < 10; i++) {
         tree.put(key(String.format("key-%05d", i)), i);
      }

      java.util.concurrent.atomic.AtomicInteger attempts = new java.util.concurrent.atomic.AtomicInteger(0);
      List<Integer> values = tree.<Integer>publish((k, v) -> {
         if (v == 0 && attempts.getAndIncrement() < 2) {
            throw new BPlusTree.IndexNodeOutdatedException("transient");
         }
         return v;
      }).toList().blockingGet();

      assertEquals(10, values.size());
      assertEquals(Integer.valueOf(0), values.get(0));
      assertEquals(Integer.valueOf(9), values.get(9));
      assertTrue("Should have retried at least twice", attempts.get() >= 3);
   }
}
