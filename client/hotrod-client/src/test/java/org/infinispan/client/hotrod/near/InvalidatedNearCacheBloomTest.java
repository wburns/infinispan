package org.infinispan.client.hotrod.near;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.NearCacheConfiguration;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCache;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.util.BloomFilter;
import org.infinispan.commons.util.MurmurHash3BloomFilter;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.near.InvalidatedNearCacheBloomTest")
public class InvalidatedNearCacheBloomTest extends SingleHotRodServerTest {

   private static final int NEAR_CACHE_SIZE = 4;

   private StorageType storageType;
   private AssertsNearCache<Integer, String> assertClient;

   private final BloomFilter<byte[]> bloomFilter = MurmurHash3BloomFilter.createFilter(NEAR_CACHE_SIZE << 2);

   private InvalidatedNearCacheBloomTest storageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   @Factory
   public Object[] factory() {
      return new Object[]{
            new InvalidatedNearCacheBloomTest().storageType(StorageType.HEAP),
            new InvalidatedNearCacheBloomTest().storageType(StorageType.OFF_HEAP),
      };
   }

   @BeforeMethod
   void beforeMethod() {
      assertClient.expectNoNearEvents();
      // All tests rely upon having the bits set for the key 1
      bloomFilter.addToFilter(assertClient.remote.keyToBytes(1));
   }

   @AfterMethod
   void resetBloomFilter() throws InterruptedException {
      assertClient.expectNoNearEvents(50, TimeUnit.MILLISECONDS);

      ((InvalidatedNearRemoteCache) assertClient.remote).clearNearCache();
      CompletionStages.join(((InvalidatedNearRemoteCache) assertClient.remote).updateBloomFilter());
      // Don't let the clear leak
      assertClient.events.clear();
   }

   @Override
   protected String parameters() {
      return "[storageType-" + storageType + "]";
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      org.infinispan.configuration.cache.ConfigurationBuilder cb = hotRodCacheConfiguration();
      cb.memory().storage(storageType);
      return TestCacheManagerFactory.createCacheManager(cb);
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      assertClient = createAssertClient();
      return assertClient.manager;
   }

   private <K, V> AssertsNearCache<K, V> createAssertClient() {
      ConfigurationBuilder builder = clientConfiguration();
      return AssertsNearCache.create(this.cache(), builder);
   }

   private ConfigurationBuilder clientConfiguration() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
      builder.remoteCache("")
            .nearCacheMode(NearCacheMode.INVALIDATED)
            .nearCacheMaxEntries(NEAR_CACHE_SIZE)
            .nearCacheUseBloomFilter(true);
      return builder;
   }

   public void testSingleKeyFilter() {
      assertClient.put(1, "v1").expectNearPreemptiveRemove(1);
      assertClient.put(1, "v2").expectNearPreemptiveRemove(1);
      assertClient.get(1, "v2").expectNearGetMissWithValue(1, "v2");
      assertClient.get(1, "v2").expectNearGetValue(1, "v2");
      assertClient.remove(1).expectNearRemove(1);
      assertClient.get(1, null).expectNearGetMiss(1);
   }

   public void testMultipleKeyFilterConflictButNoRead() {
      assertClient.put(1, "v1").expectNearPreemptiveRemove(1);

      int conflictKey = findNextKey(bloomFilter, 1, true);
      assertClient.put(conflictKey, "v1").expectNearPreemptiveRemove(conflictKey);
      assertClient.put(conflictKey, "v2").expectNearPreemptiveRemove(conflictKey);
   }

   public void testMultipleKeyFilterConflict() {
      assertClient.put(1, "v1").expectNearPreemptiveRemove(1);
      assertClient.get(1, "v1").expectNearGetMissWithValue(1, "v1");

      int conflictKey = findNextKey(bloomFilter, 1, true);
      // This is a create thus no remove is sent
      assertClient.put(conflictKey, "v1").expectNearPreemptiveRemove(conflictKey);
      // This conflicts with our original key thus it will send a remove despite nothing being removed
      assertClient.put(conflictKey, "v2").expectNearRemove(conflictKey);
      assertClient.get(1, "v1").expectNearGetValue(1, "v1");
      // The server may send additional async remove events due to bloom filter collision.
      // Consume them here so they don't leak into the next test.
      drainAsyncEvents();
   }

   public void testMultipleKeyFilterNoConflict() {
      assertClient.put(1, "v1").expectNearPreemptiveRemove(1);
      assertClient.get(1, "v1").expectNearGetMissWithValue(1, "v1");

      int nonConflictKey = findNextKey(bloomFilter, 1, false);
      // Both of the following never send a remove event back as the key wasn't present in bloom filter
      assertClient.put(nonConflictKey, "v1").expectNearPreemptiveRemove(nonConflictKey);
      assertClient.put(nonConflictKey, "v2").expectNearPreemptiveRemove(nonConflictKey);
   }

   public void testServerBloomFilterUpdate() throws InterruptedException {
      assertClient.put(1, "v1").expectNearPreemptiveRemove(1);
      assertClient.get(1, "v1").expectNearGetMissWithValue(1, "v1");

      int nonConflictKey = findNextKey(bloomFilter, 1, false);

      assertClient.put(nonConflictKey, "v1").expectNearPreemptiveRemove(nonConflictKey);
      assertClient.get(nonConflictKey, "v1").expectNearGetMissWithValue(nonConflictKey, "v1");

      boolean serverBloomFilterUpdated = false;
      for (int i = 0; i < 10; ++i) {
         assertClient.put(nonConflictKey, "v1");
         // The cache will always preemptively invalidate the value if necessary
         MockNearCacheService.MockEvent event = assertClient.events.poll(10, TimeUnit.SECONDS);
         assertNotNull(event);
         assertInstanceOf(MockNearCacheService.MockRemoveEvent.class, event);
         assertEquals(nonConflictKey, ((MockNearCacheService.MockRemoveEvent<?>) event).key);

         // Eventually this be null - which means our cache has been updated on the server side
         event = assertClient.events.poll(100, TimeUnit.MILLISECONDS);
         if (event == null) {
            serverBloomFilterUpdated = true;
            break;
         }
         assertInstanceOf(MockNearCacheService.MockRemoveEvent.class, event);
         assertEquals(nonConflictKey, ((MockNearCacheService.MockRemoveEvent<?>) event).key);

         Thread.sleep(10);
      }

      assertTrue(serverBloomFilterUpdated, "The server bloom filter was never updated and we got remove events every time");
   }

   public void testMissedInvalidationsAndThresholdBehavior() {
      NearCacheService<Integer, String> nearCacheService = assertClient.nearCacheService.get();
      assertNotNull(nearCacheService);
      assertEquals(0, nearCacheService.getMissedInvalidations());
      int threshold = nearCacheService.getBloomFilterUpdateThreshold();
      assertEquals(3, threshold);

      // 1. Local remove on a key not in near cache must NOT increment missedInvalidations
      boolean removed = nearCacheService.remove(998);
      assertFalse(removed);
      assertEquals(0, nearCacheService.getMissedInvalidations());

      // 2. Server invalidation that didn't remove anything increments missedInvalidations
      nearCacheService.serverInvalidation(999);
      assertEquals(1, nearCacheService.getMissedInvalidations());

      // 3. Clear resets missedInvalidations
      nearCacheService.clear();
      assertClient.expectNearClear();
      assertEquals(0, nearCacheService.getMissedInvalidations());
   }

   private void drainAsyncEvents() {
      eventually("Expected all async near cache events to be drained",
            () -> assertClient.events.poll(50, TimeUnit.MILLISECONDS) == null);
   }

   public void testBloomFilterUpdateThresholdFormula() {
      NearCacheConfiguration config4 = new NearCacheConfiguration(NearCacheMode.INVALIDATED, 4, true);
      NearCacheService<Integer, String> service4 = NearCacheService.create(config4, null);
      assertEquals(3, service4.getBloomFilterUpdateThreshold());

      NearCacheConfiguration config16 = new NearCacheConfiguration(NearCacheMode.INVALIDATED, 16, true);
      NearCacheService<Integer, String> service16 = NearCacheService.create(config16, null);
      assertEquals(4, service16.getBloomFilterUpdateThreshold());

      NearCacheConfiguration config100 = new NearCacheConfiguration(NearCacheMode.INVALIDATED, 100, true);
      NearCacheService<Integer, String> service100 = NearCacheService.create(config100, null);
      assertEquals(9, service100.getBloomFilterUpdateThreshold());
   }

   int findNextKey(BloomFilter<byte[]> filter, int originalValue, boolean present) {
      while (true) {
         byte[] testKey = assertClient.remote.keyToBytes(++originalValue);
         if (present == filter.possiblyPresent(testKey)) {
            return originalValue;
         }
      }
   }
}
