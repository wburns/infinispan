package org.infinispan.client.hotrod.near;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.NearCacheEvictionStrategy;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.near.MockNearCacheService.MockEvent;
import org.infinispan.client.hotrod.near.MockNearCacheService.MockRemoveEvent;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.near.ClusterNearCacheEvictionStrategyTest")
public class ClusterNearCacheEvictionStrategyTest extends MultiHotRodServersTest {
   private static final int CLUSTER_MEMBERS = 2;
   private static final int NEAR_CACHE_SIZE = 2;

   List<AssertsNearCache<Integer, String>> assertClients = new ArrayList<>(CLUSTER_MEMBERS);

   AssertsNearCache<Integer, String> client0;
   AssertsNearCache<Integer, String> client1;

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(CLUSTER_MEMBERS, getCacheConfiguration());

      client0 = assertClients.get(0);
      client1 = assertClients.get(1);
   }

   @BeforeMethod
   void beforeMethod() {
      assertClients.forEach(AssertsNearCache::expectNoNearEvents);
   }

   @AfterMethod
   void afterMethod() {
      caches().forEach(Cache::clear);
      assertClients.forEach(AssertsNearCache::resetEvents);
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      for (AssertsNearCache<Integer, String> assertsNearCache : assertClients) {
         try {
            assertsNearCache.expectNoNearEvents(500, TimeUnit.MILLISECONDS);
         } catch (InterruptedException e) {
            throw new AssertionError(e);
         }
      }
      assertClients.forEach(AssertsNearCache::stop);
      assertClients.clear();

      super.destroy();
   }

   private ConfigurationBuilder getCacheConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(1);
      return hotRodCacheConfiguration(builder);
   }

   @Override
   protected RemoteCacheManager createClient(int i) {
      AssertsNearCache<Integer, String> asserts = createAssertClient(NearCacheEvictionStrategy.BATCH_DELETE, 1, 10);
      assertClients.add(asserts);
      return asserts.manager;
   }

   private AssertsNearCache<Integer, String> createAssertClient(NearCacheEvictionStrategy strategy, int batchSize, int threshold) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      for (HotRodServer server : servers)
         clientBuilder.addServer().host("127.0.0.1").port(server.getPort());
      clientBuilder.remoteCache("").nearCacheMode(NearCacheMode.INVALIDATED)
            .nearCacheMaxEntries(NEAR_CACHE_SIZE)
            .nearCacheUseBloomFilter(true)
            .nearCacheEvictionStrategy(strategy)
            .nearCacheEvictionBatchSize(batchSize)
            .nearCacheEvictionThreshold(threshold);
      return AssertsNearCache.create(cache(0), clientBuilder);
   }

   public void testClusterBatchDeleteEviction() throws InterruptedException {
      // client0 populates keys 1 and 2
      client0.remote.put(1, "v1");
      client0.remote.put(2, "v2");
      client0.remote.put(3, "v3");
      client0.resetEvents();

      client0.get(1, "v1").expectNearGetMissWithValue(1, "v1");
      client0.get(2, "v2").expectNearGetMissWithValue(2, "v2");

      // Reading key 3 forces eviction of either key 1 or key 2 in client0's near cache (capacity 2)
      client0.get(3, "v3").expectNearGetMissWithValue(3, "v3");

      eventually(() -> client0.nearCacheSize() <= 2);

      Integer evictedKey = client0.getNearCacheEntry(1) == null ? 1 : 2;
      Integer retainedKey = evictedKey == 1 ? 2 : 1;

      client0.resetEvents();

      // client1 updates the retained key: client0 MUST receive an invalidation
      client1.remote.put(retainedKey, "v-retained-from-client1");
      MockEvent event = client0.events.poll(10, TimeUnit.SECONDS);
      assertNotNull(event);
      assertTrue(event instanceof MockRemoveEvent, "Expected MockRemoveEvent");
      assertEquals(retainedKey, ((MockRemoveEvent<?>) event).key);

      // client1 updates the evicted key: client0 must NOT receive an invalidation
      client1.remote.put(evictedKey, "v-evicted-from-client1");
      client0.expectNoNearEvents(50, TimeUnit.MILLISECONDS);
   }

   public void testClusterClearOnThreshold() throws InterruptedException {
      AssertsNearCache<Integer, String> thresholdClient = createAssertClient(NearCacheEvictionStrategy.CLEAR_ON_THRESHOLD, 32, 2);
      try {
         thresholdClient.remote.put(10, "v10");
         thresholdClient.remote.put(20, "v20");
         thresholdClient.remote.put(30, "v30");
         thresholdClient.remote.put(40, "v40");
         thresholdClient.resetEvents();

         thresholdClient.remote.get(10);
         thresholdClient.remote.get(20);
         thresholdClient.remote.get(30);
         thresholdClient.remote.get(40);

         boolean hasClearEvent = thresholdClient.events.stream().anyMatch(e -> e instanceof MockNearCacheService.MockClearEvent);
         assertTrue(hasClearEvent, "Expected MockClearEvent when eviction threshold is reached");
         thresholdClient.resetEvents();

         // Updates from client1 across the cluster for cleared key 10 should NOT invalidate thresholdClient
         client1.remote.put(10, "v-new-10");
         thresholdClient.expectNoNearEvents(50, TimeUnit.MILLISECONDS);

         // Updates from client1 across the cluster for retained key 40 SHOULD invalidate thresholdClient
         client1.remote.put(40, "v-new-40");
         MockEvent event = thresholdClient.events.poll(10, TimeUnit.SECONDS);
         assertNotNull(event);
         assertTrue(event instanceof MockRemoveEvent, "Expected MockRemoveEvent for key 40");
         assertEquals(40, ((MockRemoveEvent<?>) event).key);
      } finally {
         thresholdClient.stop();
      }
   }
}
