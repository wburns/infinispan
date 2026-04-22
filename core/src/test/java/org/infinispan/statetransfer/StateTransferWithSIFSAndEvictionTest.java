package org.infinispan.statetransfer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.testing.Testing.tmpDirectory;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.reactive.publisher.impl.LocalPublisherManager;
import org.infinispan.reactive.publisher.impl.SegmentAwarePublisherSupplier;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.reactivestreams.Publisher;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Tests state transfer when a cluster has a distributed cache with soft index file store,
 * eviction enabled, and nodes are added after data is populated.
 *
 * @author William Burns
 * @since 16.2
 */
@Test(groups = "functional", testName = "statetransfer.StateTransferWithSIFSAndEvictionTest")
public class StateTransferWithSIFSAndEvictionTest extends MultipleCacheManagersTest {
   private static final int INITIAL_CLUSTER_SIZE = 1;
   private static final int ADDITIONAL_NODES = 4;
   private static final String CACHE_NAME = "testCache";
   private static final int MAX_ENTRIES = 1_000;
   private static final int TOTAL_ENTRIES = 250_000;

   @Override
   protected void createCacheManagers() throws Throwable {
      // Create initial cluster
      for (int i = 0; i < INITIAL_CLUSTER_SIZE; i++) {
         createStatefulCacheManager("node-" + i);
      }
      waitForClusterToForm(CACHE_NAME);
   }

   private void createStatefulCacheManager(String id) {
      String stateDirectory = tmpDirectory(this.getClass().getSimpleName(), id);
      Util.recursiveFileRemove(stateDirectory);

      GlobalConfigurationBuilder global = defaultGlobalConfigurationBuilder();
      global.globalState().enabled(true)
            .configurationStorage(ConfigurationStorage.OVERLAY)
            .persistentLocation(stateDirectory);

      ConfigurationBuilder config = new ConfigurationBuilder();
      config.clustering().cacheMode(CacheMode.DIST_SYNC);
      config.clustering().hash().numOwners(2);
      config.clustering().hash().numSegments(6);

      // Configure eviction
      config.memory()
            .maxCount(MAX_ENTRIES);

      // Configure SoftIndexFileStore
      config.persistence().addSoftIndexFileStore()
            .maxNodeSize(128)
            .dataLocation(Paths.get(stateDirectory, "data").toString())
            .indexLocation(Paths.get(stateDirectory, "index").toString());

      EmbeddedCacheManager manager = addClusterEnabledCacheManager(global, null);
      manager.defineConfiguration(CACHE_NAME, config.build());
   }

   /**
    * Counts entries per segment using the LocalPublisherManager.
    *
    * @param cache the cache to count entries in
    * @return a map of segment ID to entry count
    */
   private Map<Integer, Long> countEntriesPerSegment(Cache<Integer, String> cache) {
      LocalPublisherManager<Integer, String> lpm = TestingUtil.extractComponent(cache, LocalPublisherManager.class);
      int numSegments = cache.getCacheConfiguration().clustering().hash().numSegments();

      Map<Integer, Long> segmentCounts = new HashMap<>();

      for (int segment = 0; segment < numSegments; segment++) {
         IntSet singleSegment = IntSets.immutableSet(segment);
         SegmentAwarePublisherSupplier<?> publisher = lpm.entryPublisher(
               singleSegment,
               null,
               null,
               0L,
               DeliveryGuarantee.AT_LEAST_ONCE,
               Function.identity()
         );

         // Use publisherWithoutSegments() to get only values, not segment completion notifications
         Publisher<?> segmentPublisher = publisher.publisherWithoutSegments();
         long count = Flowable.fromPublisher(segmentPublisher).count().blockingGet();
         segmentCounts.put(segment, count);
      }

      return segmentCounts;
   }

   public void testStateTransferWithEvictionAndStore() throws Exception {
      // Insert 5 million entries into the cache
      log.infof("Inserting %d entries into cache", TOTAL_ENTRIES);
      Cache<Integer, String> cache0 = cache(0, CACHE_NAME);

      for (int i = 0; i < TOTAL_ENTRIES; i++) {
         cache0.put(i, "value-" + i);
         if (i % 100_000 == 0) {
            log.fatalf("Inserted %d entries", i);
         }
      }

      log.info("All entries inserted, verifying size");

      // Assert all 5 million entries are present using segment-by-segment counting
      log.info("Counting entries per segment before adding nodes");
      Map<Integer, Long> segmentCountsBefore = countEntriesPerSegment(cache0);
      log.infof("Verified %s entries present in cluster", segmentCountsBefore);

      // Add 5 more nodes all at the same time
      log.infof("Adding %d new nodes to the cluster", ADDITIONAL_NODES);
      List<Future<EmbeddedCacheManager>> futures = new ArrayList<>(ADDITIONAL_NODES);

      for (int i = 0; i < ADDITIONAL_NODES; i++) {
         final int nodeIndex = INITIAL_CLUSTER_SIZE + i;
         Future<EmbeddedCacheManager> future = fork(() -> {
            createStatefulCacheManager("node-" + nodeIndex);
            return manager(nodeIndex);
         });
         futures.add(future);
      }

      // Wait for all nodes to be created
      for (Future<EmbeddedCacheManager> future : futures) {
         future.get();
      }

      // Wait for the cluster to form with all nodes
      log.infof("Waiting for cluster to form with all %d nodes", INITIAL_CLUSTER_SIZE + ADDITIONAL_NODES);
      waitForClusterToForm(CACHE_NAME);

      // Assert that all entries are still present using segment-by-segment counting
      log.info("Verifying size and distribution after cluster expansion");
      int numNodes = INITIAL_CLUSTER_SIZE + ADDITIONAL_NODES;
      int numOwners = cache0.getCacheConfiguration().clustering().hash().numOwners();
      int numSegments = cache0.getCacheConfiguration().clustering().hash().numSegments();

      // Collect segment counts from all nodes
      List<Map<Integer, Long>> allNodeSegmentCounts = new ArrayList<>();
      for (int i = 0; i < numNodes; i++) {
         Cache<Integer, String> cache = cache(i, CACHE_NAME);
         log.infof("Counting entries per segment on node %d", i);
         Map<Integer, Long> segmentCounts = countEntriesPerSegment(cache);
         allNodeSegmentCounts.add(segmentCounts);
      }

      log.infof("Verified %d entries still present after adding %d nodes with proper segment distribution",
            TOTAL_ENTRIES, ADDITIONAL_NODES);

      // Sum all segment counts across all nodes and verify total
      log.info("Verifying total segment counts across all nodes:");
      Map<Integer, Long> aggregatedSegmentCounts = new HashMap<>();
      long grandTotal = 0;

      for (int segment = 0; segment < numSegments; segment++) {
         long totalForSegment = 0;
         for (Map<Integer, Long> nodeCounts : allNodeSegmentCounts) {
            totalForSegment += nodeCounts.get(segment);
         }
         aggregatedSegmentCounts.put(segment, totalForSegment);
         grandTotal += totalForSegment;
      }

      Map<Integer, Long> expected = segmentCountsBefore.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue() << 1));

      // If there are missing entries, identify specific missing keys
      if (!aggregatedSegmentCounts.equals(expected)) {
         log.errorf("Segment counts do not match! Expected: %s, Actual: %s", expected, aggregatedSegmentCounts);

         // Find segments with missing entries and show which nodes own them
         IntSet segmentsWithMissingEntries = IntSets.mutableEmptySet();
         for (Map.Entry<Integer, Long> entry : expected.entrySet()) {
            int segment = entry.getKey();
            long expectedCount = entry.getValue();
            long actualCount = aggregatedSegmentCounts.getOrDefault(segment, 0L);
            if (actualCount < expectedCount) {
               long missing = expectedCount - actualCount;

               // Identify which nodes own this segment
               List<Integer> nodesOwningSegment = new ArrayList<>();
               for (int nodeIndex = 0; nodeIndex < numNodes; nodeIndex++) {
                  long nodeCount = allNodeSegmentCounts.get(nodeIndex).get(segment);
                  if (nodeCount > 0) {
                     nodesOwningSegment.add(nodeIndex);
                  }
               }

               log.errorf("Segment %d is missing %d entries (expected %d, actual %d) - owned by nodes: %s",
                     segment, missing, expectedCount, actualCount, nodesOwningSegment);

               // Show per-node counts for this segment
               for (int nodeIndex = 0; nodeIndex < numNodes; nodeIndex++) {
                  long nodeCount = allNodeSegmentCounts.get(nodeIndex).get(segment);
                  log.errorf("  Node %d has %d entries for segment %d", nodeIndex, nodeCount, segment);
               }

               segmentsWithMissingEntries.set(segment);
            }
         }

         // Find a specific missing key for each problematic segment
         if (!segmentsWithMissingEntries.isEmpty()) {
            KeyPartitioner keyPartitioner = TestingUtil.extractComponent(cache0, KeyPartitioner.class);
            log.errorf("Searching for missing keys in segments: %s", segmentsWithMissingEntries);

            for (int segment : segmentsWithMissingEntries) {
               log.infof("Looking for missing keys in segment %d...", segment);
               boolean foundMissingKey = false;

               // Iterate through originally inserted keys to find ones that map to this segment
               for (int key = 0; key < TOTAL_ENTRIES && !foundMissingKey; key++) {
                  // Convert key to storage format before calculating segment
                  Object storageKey = cache0.getAdvancedCache().getKeyDataConversion().toStorage(key);
                  int keySegment = keyPartitioner.getSegment(storageKey);

                  if (keySegment == segment) {
                     // Check if this key exists in each node's cache (memory + store, without loading)
                     List<Integer> nodesWithKeyInMemory = new ArrayList<>();
                     List<Integer> nodesWithKeyInStore = new ArrayList<>();
                     List<Integer> nodesMissingKey = new ArrayList<>();

                     for (int nodeIndex = 0; nodeIndex < numNodes; nodeIndex++) {
                        Cache<Integer, String> cache = cache(nodeIndex, CACHE_NAME);
                        DataContainer dataContainer = cache.getAdvancedCache().getDataContainer();
                        PersistenceManager persistenceManager = TestingUtil.extractComponent(cache, PersistenceManager.class);

                        boolean inMemory = false;
                        boolean inStore = false;

                        // Check memory without triggering load
                        InternalCacheEntry memoryEntry = dataContainer.peek(storageKey);
                        if (memoryEntry != null) {
                           inMemory = true;
                           nodesWithKeyInMemory.add(nodeIndex);
                        }

                        // Check store without loading into memory
                        MarshallableEntry storeEntry = persistenceManager.loadFromAllStores(storageKey, true, true).toCompletableFuture().join();
                        if (storeEntry != null) {
                           inStore = true;
                           nodesWithKeyInStore.add(nodeIndex);
                        }

                        if (!inMemory && !inStore) {
                           nodesMissingKey.add(nodeIndex);
                        }
                     }

                     int nodesWithKey = nodesWithKeyInMemory.size() + nodesMissingKey.stream()
                           .filter(n -> nodesWithKeyInStore.contains(n))
                           .mapToInt(n -> 1).sum();

                     List<Integer> allNodesWithKey = new ArrayList<>(nodesWithKeyInMemory);
                     for (int n : nodesWithKeyInStore) {
                        if (!allNodesWithKey.contains(n)) {
                           allNodesWithKey.add(n);
                        }
                     }

                     if (allNodesWithKey.size() < numOwners) {
                        log.errorf("FOUND MISSING KEY: key=%d (storageKey=%s, segment=%d)",
                              key, storageKey, segment);
                        log.errorf("  Present in memory on nodes: %s", nodesWithKeyInMemory);
                        log.errorf("  Present in store on nodes: %s", nodesWithKeyInStore);
                        log.errorf("  Missing from nodes: %s (expected on %d nodes total)",
                              nodesMissingKey, numOwners);
                        foundMissingKey = true;
                     }
                  }
               }

               if (!foundMissingKey) {
                  log.warnf("Could not find a specific missing key for segment %d (all checked keys were present)", segment);
               }
            }
         }
      }

      assertThat(aggregatedSegmentCounts).isEqualTo(expected);

      // Verify each segment appears on exactly numOwners nodes
      log.info("Verifying segment ownership distribution:");
      for (int segment = 0; segment < numSegments; segment++) {
         int nodesWithSegment = 0;
         long totalEntriesInSegment = 0;

         for (Map<Integer, Long> nodeCounts : allNodeSegmentCounts) {
            long count = nodeCounts.get(segment);
            if (count > 0) {
               nodesWithSegment++;
               totalEntriesInSegment += count;
            }
         }

         if (nodesWithSegment != numOwners) {
            log.warnf("  Segment %d present on %d nodes (expected %d), total entries: %d",
                  segment, nodesWithSegment, numOwners, totalEntriesInSegment);
         }
      }

      log.infof("Successfully verified state transfer: all %d entries present with %d copies each",
            TOTAL_ENTRIES, numOwners);

      long expectedTotal = (long) TOTAL_ENTRIES * numOwners;
      log.infof("  Total entries across all nodes and segments: %d", grandTotal);
      log.infof("  Expected total (entries × owners): %d × %d = %d", TOTAL_ENTRIES, numOwners, expectedTotal);

      // Assert the totals match
      assertThat(grandTotal).as("Total segment counts across all nodes should equal entries × owners")
            .isEqualTo(expectedTotal);
   }
}
