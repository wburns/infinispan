package org.infinispan.client.hotrod.stress;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Tests putAll performance with large and small data sets
 *
 * @author William Burns
 * @since 7.2
 */
@Test
public abstract class AbstractPutAllPerfTest extends MultipleCacheManagersTest {

   protected HotRodServer[] hotrodServers;
   protected RemoteCacheManager remoteCacheManager;
   protected RemoteCache<Object, Object> remoteCache;

   abstract protected int numberOfHotRodServers();

   abstract protected ConfigurationBuilder clusterConfig();

   protected final long millisecondsToRun = TimeUnit.SECONDS.toMillis(15);

   @Override
   protected void createCacheManagers() throws Throwable {
      final int numServers = numberOfHotRodServers();
      hotrodServers = new HotRodServer[numServers];

      createCluster(hotRodCacheConfiguration(clusterConfig()), numberOfHotRodServers());

      for (int i = 0; i < numServers; i++) {
         EmbeddedCacheManager cm = cacheManagers.get(i);
         hotrodServers[i] = HotRodClientTestingUtil.startHotRodServer(cm);
      }

      String servers = HotRodClientTestingUtil.getServersString(hotrodServers);

      remoteCacheManager = new RemoteCacheManager(servers);
      remoteCache = remoteCacheManager.getCache();
   }

   @AfterClass(alwaysRun = true)
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotrodServers);
   }

   protected void runTest(int size, int iterations, String name) {
      Map<Integer, byte[]> map = new HashMap<Integer, byte[]>();
      Random random = new Random();
      long totalTime = 0;
      for (int i = 0; i < iterations; ++i) {
         map.clear();
         remoteCache.clear();
         for (int j = 0; j < size; ++j) {
            int value = random.nextInt(Integer.MAX_VALUE);
            byte[] valueArray = new byte[1024];
            random.nextBytes(valueArray);
            map.put(value, valueArray);
         }
         long begin = System.nanoTime();
         remoteCache.putAll(map);
         totalTime += System.nanoTime() - begin;
      }
      long millisecondTotal = TimeUnit.NANOSECONDS.toMillis(totalTime);
      System.out.println(name + " - Performed " + iterations + " in " + millisecondTotal + " ms generating " +
              iterations / TimeUnit.MILLISECONDS.toSeconds(millisecondTotal) + " ops/sec");
   }

   public void test5Input() {
      runTest(5, 20000, "test5Input");
   }

   public void test500Input() {
      runTest(500, 200, "test500Input");
   }

   public void test50000Input() {
      runTest(50000, 5, "test50000Input");
   }
}
