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
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.near.PrimaryNearCacheTest")
public class PrimaryNearCacheTest extends MultiHotRodServersTest {

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

   private RemoteCacheManager createNearClient(ClientIntelligence intelligence, boolean bloom) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      for (HotRodServer server : servers)
         clientBuilder.addServer().host("127.0.0.1").port(server.getPort());

      clientBuilder.clientIntelligence(intelligence);
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

   public void testPrimaryNearCacheOperations() {
      RemoteCacheManager clientManager = createNearClient(ClientIntelligence.HASH_DISTRIBUTION_AWARE, false);
      RemoteCache<Integer, String> clientCache = clientManager.getCache();
      assertThat(clientCache).isInstanceOf(InvalidatedNearRemoteCache.class);
      InvalidatedNearRemoteCache<Integer, String> inrc = (InvalidatedNearRemoteCache<Integer, String>) clientCache;
      assertThat(inrc.isPrimaryMode()).isTrue();

      Integer key0 = findKeyForServer(inrc, serverSocketAddress(0));
      Integer key1 = findKeyForServer(inrc, serverSocketAddress(1));

      clientCache.put(key0, "val0");
      clientCache.put(key1, "val1");

      // Reads populate near cache
      assertThat(clientCache.get(key0)).isEqualTo("val0");
      assertThat(clientCache.get(key1)).isEqualTo("val1");

      assertThat(inrc.getNearCacheService().get(key0)).isNotNull();
      assertThat(inrc.getNearCacheService().get(key1)).isNotNull();

      // Update from server directly
      server(0).getCacheManager().getCache().put(key0, "val0-updated");
      eventually(() -> inrc.getNearCacheService().get(key0) == null);

      // key1 on server 1 remains cached
      assertThat(inrc.getNearCacheService().get(key1)).isNotNull();
   }

   public void testPrimaryNearCacheOperationsWithBloomFilter() {
      RemoteCacheManager clientManager = createNearClient(ClientIntelligence.HASH_DISTRIBUTION_AWARE, true);
      RemoteCache<Integer, String> clientCache = clientManager.getCache();
      assertThat(clientCache).isInstanceOf(InvalidatedNearRemoteCache.class);
      InvalidatedNearRemoteCache<Integer, String> inrc = (InvalidatedNearRemoteCache<Integer, String>) clientCache;
      assertThat(inrc.isPrimaryMode()).isTrue();

      Integer key0 = findKeyForServer(inrc, serverSocketAddress(0));
      Integer key1 = findKeyForServer(inrc, serverSocketAddress(1));

      clientCache.put(key0, "val0");
      clientCache.put(key1, "val1");

      // Reads populate near cache
      assertThat(clientCache.get(key0)).isEqualTo("val0");
      assertThat(clientCache.get(key1)).isEqualTo("val1");

      assertThat(inrc.getNearCacheService().get(key0)).isNotNull();
      assertThat(inrc.getNearCacheService().get(key1)).isNotNull();

      // Update from server directly
      server(0).getCacheManager().getCache().put(key0, "val0-updated");
      eventually(() -> inrc.getNearCacheService().get(key0) == null);

      // key1 on server 1 remains cached
      assertThat(inrc.getNearCacheService().get(key1)).isNotNull();
   }

   public void testSegmentInvalidationOnServerStop() {
      RemoteCacheManager clientManager = createNearClient(ClientIntelligence.HASH_DISTRIBUTION_AWARE, false);
      RemoteCache<Integer, String> clientCache = clientManager.getCache();
      InvalidatedNearRemoteCache<Integer, String> inrc = (InvalidatedNearRemoteCache<Integer, String>) clientCache;
      assertThat(inrc.isPrimaryMode()).isTrue();

      Integer key0 = findKeyForServer(inrc, serverSocketAddress(0));
      Integer key1 = findKeyForServer(inrc, serverSocketAddress(1));

      clientCache.put(key0, "v0");
      clientCache.put(key1, "v1");

      assertThat(clientCache.get(key0)).isEqualTo("v0");
      assertThat(clientCache.get(key1)).isEqualTo("v1");

      assertThat(inrc.getNearCacheService().get(key0)).isNotNull();
      assertThat(inrc.getNearCacheService().get(key1)).isNotNull();

      // Stop server 1 - should only invalidate segments for server 1, not server 0!
      HotRodClientTestingUtil.killServers(server(1));

      eventually(() -> inrc.getNearCacheService().get(key1) == null);

      // Key 0 on surviving server 0 MUST STILL BE CACHED!
      assertThat(inrc.getNearCacheService().get(key0))
            .as("Key on surviving server should remain cached in near cache")
            .isNotNull();
      assertThat(clientCache.get(key0)).isEqualTo("v0");
   }

   public void testFallbackWhenNotHashAware() {
      RemoteCacheManager clientManager = createNearClient(ClientIntelligence.TOPOLOGY_AWARE, false);
      RemoteCache<Integer, String> clientCache = clientManager.getCache();
      InvalidatedNearRemoteCache<Integer, String> inrc = (InvalidatedNearRemoteCache<Integer, String>) clientCache;
      assertThat(inrc.isPrimaryMode()).isFalse();
   }
}
