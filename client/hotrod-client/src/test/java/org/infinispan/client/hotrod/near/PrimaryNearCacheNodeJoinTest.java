package org.infinispan.client.hotrod.near;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCache;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.near.PrimaryNearCacheNodeJoinTest")
public class PrimaryNearCacheNodeJoinTest extends MultiHotRodServersTest {

   private final List<RemoteCacheManager> managers = new ArrayList<>();

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(2, getCacheConfiguration());
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      managers.forEach(RemoteCacheManager::stop);
      managers.clear();
      super.destroy();
   }

   private ConfigurationBuilder getCacheConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(1);
      return hotRodCacheConfiguration(builder);
   }

   private RemoteCacheManager createNearClient(boolean bloom) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      for (HotRodServer server : servers)
         clientBuilder.addServer().host("127.0.0.1").port(server.getPort());

      clientBuilder.clientIntelligence(ClientIntelligence.HASH_DISTRIBUTION_AWARE);
      clientBuilder.remoteCache("")
            .nearCacheMode(NearCacheMode.INVALIDATED)
            .nearCacheMaxEntries(100)
            .nearCacheUseBloomFilter(bloom);

      RemoteCacheManager rcm = new RemoteCacheManager(clientBuilder.build());
      managers.add(rcm);
      return rcm;
   }

   private SocketAddress serverSocketAddress(int serverIndex) {
      return new java.net.InetSocketAddress(server(serverIndex).getHost(), server(serverIndex).getPort());
   }

   private Integer findKeyForServer(InvalidatedNearRemoteCache<Integer, String> inrc, SocketAddress targetServer) {
      for (int i = 0; i < 10000; i++) {
         if (InvalidatedNearRemoteCache.sameAddress(inrc.getPrimaryOwner(i), targetServer)) {
            return i;
         }
      }
      throw new IllegalStateException("Could not find key for server " + targetServer);
   }

   public void testNodeJoinWithNearCache() {
      doTestNodeJoin(false);
   }

   public void testNodeJoinWithBloomFilterNearCache() {
      doTestNodeJoin(true);
   }

   private void doTestNodeJoin(boolean bloom) {
      RemoteCacheManager clientManager = createNearClient(bloom);
      RemoteCache<Integer, String> clientCache = clientManager.getCache();
      InvalidatedNearRemoteCache<Integer, String> inrc = (InvalidatedNearRemoteCache<Integer, String>) clientCache;
      assertThat(inrc.isPrimaryMode()).isTrue();

      Integer key0 = findKeyForServer(inrc, serverSocketAddress(0));
      Integer key1 = findKeyForServer(inrc, serverSocketAddress(1));

      clientCache.put(key0, "val0");
      clientCache.put(key1, "val1");

      assertThat(clientCache.get(key0)).isEqualTo("val0");
      assertThat(clientCache.get(key1)).isEqualTo("val1");

      assertThat(inrc.getNearCacheService().get(key0)).isNotNull();
      assertThat(inrc.getNearCacheService().get(key1)).isNotNull();

      // Add a 3rd server to the cluster
      HotRodServer server2 = addHotRodServer(getCacheConfiguration());
      server2.getCacheManager().getCache();
      TestingUtil.waitForNoRebalance(caches());

      try {
         SocketAddress addr2 = new java.net.InetSocketAddress(server2.getHost(), server2.getPort());
         eventually(() -> inrc.getServerListenerState(addr2) != null && inrc.getServerListenerState(addr2).isActive());

         Integer key2 = findKeyForServer(inrc, addr2);
         clientCache.put(key2, "val2");
         assertThat(clientCache.get(key2)).isEqualTo("val2");
         assertThat(inrc.getNearCacheService().get(key2)).isNotNull();

         // Direct update on server 2 sends invalidation to near cache
         server2.getCacheManager().getCache().put(key2, "val2-updated");
         eventually(() -> inrc.getNearCacheService().get(key2) == null);
         assertThat(clientCache.get(key2)).isEqualTo("val2-updated");
      } finally {
         HotRodClientTestingUtil.killServers(server2);
         servers.remove(server2);
         TestingUtil.killCacheManagers(server2.getCacheManager());
         cacheManagers.remove(server2.getCacheManager());
         TestingUtil.waitForNoRebalance(caches());
      }
   }
}
