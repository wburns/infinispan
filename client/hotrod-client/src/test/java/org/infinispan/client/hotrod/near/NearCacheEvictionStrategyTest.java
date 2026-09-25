package org.infinispan.client.hotrod.near;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.NearCacheEvictionStrategy;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.near.MockNearCacheService.MockEvent;
import org.infinispan.client.hotrod.near.MockNearCacheService.MockRemoveEvent;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.near.NearCacheEvictionStrategyTest")
public class NearCacheEvictionStrategyTest extends SingleHotRodServerTest {

   private AssertsNearCache<Integer, String> assertClient;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      org.infinispan.configuration.cache.ConfigurationBuilder cb = hotRodCacheConfiguration();
      cb.memory().storage(StorageType.HEAP);
      return TestCacheManagerFactory.createCacheManager(cb);
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      assertClient = createClient(NearCacheEvictionStrategy.BATCH_DELETE, 1, 10);
      return assertClient.manager;
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() {
      super.clearContent();
      if (assertClient != null) {
         assertClient.stop();
         assertClient = null;
      }
   }

   private AssertsNearCache<Integer, String> createClient(NearCacheEvictionStrategy strategy, int batchSize, int threshold) {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
      builder.remoteCache("")
            .nearCacheMode(NearCacheMode.INVALIDATED)
            .nearCacheMaxEntries(2)
            .nearCacheUseBloomFilter(true)
            .nearCacheEvictionStrategy(strategy)
            .nearCacheEvictionBatchSize(batchSize)
            .nearCacheEvictionThreshold(threshold);
      return AssertsNearCache.create(cache(), builder);
   }

   public void testBatchDeleteStrategy() throws InterruptedException {
      assertClient = createClient(NearCacheEvictionStrategy.BATCH_DELETE, 1, 10);
      RemoteCache<Integer, String> remote = assertClient.remote;

      remote.put(1, "v1");
      remote.put(2, "v2");
      remote.put(3, "v3");
      assertClient.resetEvents();

      // Read key 1 and key 2 to cache them locally
      assertClient.get(1, "v1").expectNearGetMissWithValue(1, "v1");
      assertClient.get(2, "v2").expectNearGetMissWithValue(2, "v2");

      // Verify key 1 and key 2 are hits in local near cache
      assertClient.get(1, "v1").expectNearGetValue(1, "v1");
      assertClient.get(2, "v2").expectNearGetValue(2, "v2");

      // Read key 3 to cause eviction in near cache (capacity is 2)
      assertClient.get(3, "v3").expectNearGetMissWithValue(3, "v3");

      // Wait for near cache size to be bounded
      eventually(() -> assertClient.nearCacheSize() <= 2);

      // Determine which key was evicted and which was retained
      Integer evictedKey = assertClient.getNearCacheEntry(1) == null ? 1 : 2;
      Integer retainedKey = evictedKey == 1 ? 2 : 1;

      // Drain any queued events
      assertClient.resetEvents();

      // Write directly to server for retained key: should receive invalidation
      cache().put(retainedKey, "v-updated");
      MockEvent event = assertClient.events.poll(10, TimeUnit.SECONDS);
      assertNotNull(event);
      assertTrue(event instanceof MockRemoveEvent, "Expected MockRemoveEvent");
      assertEquals(retainedKey, ((MockRemoveEvent<?>) event).key);

      // Write directly to server for evicted key: should NOT receive invalidation
      cache().put(evictedKey, "v-evicted-update");
      assertClient.expectNoNearEvents(50, TimeUnit.MILLISECONDS);
   }

   public void testClearOnThresholdStrategy() throws InterruptedException {
      // Threshold is 2 evictions
      assertClient = createClient(NearCacheEvictionStrategy.CLEAR_ON_THRESHOLD, 32, 2);
      RemoteCache<Integer, String> remote = assertClient.remote;

      remote.put(1, "v1");
      remote.put(2, "v2");
      remote.put(3, "v3");
      remote.put(4, "v4");
      assertClient.resetEvents();

      // Read keys to populate near cache and trigger evictions
      remote.get(1);
      remote.get(2);
      remote.get(3);
      remote.get(4);

      boolean hasClearEvent = assertClient.events.stream().anyMatch(e -> e instanceof MockNearCacheService.MockClearEvent);
      assertTrue(hasClearEvent, "Expected MockClearEvent when eviction threshold is reached");

      assertClient.resetEvents();

      // Write directly to server for key 1 (cleared): should NOT receive invalidation
      cache().put(1, "v-cleared-1");
      assertClient.expectNoNearEvents(50, TimeUnit.MILLISECONDS);

      // Write directly to server for key 4 (cached after clear): SHOULD receive invalidation
      cache().put(4, "v-retained-4");
      MockEvent event = assertClient.events.poll(10, TimeUnit.SECONDS);
      assertNotNull(event);
      assertTrue(event instanceof MockRemoveEvent, "Expected MockRemoveEvent for key 4");
      assertEquals(4, ((MockRemoveEvent<?>) event).key);
   }
}
