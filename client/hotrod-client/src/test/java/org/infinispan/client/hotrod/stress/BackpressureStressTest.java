package org.infinispan.client.hotrod.stress;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ExhaustedAction;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;
import org.testng.annotations.Test;

@Test(groups = "stress", testName = "client.hotrod.stress.BackpressureStressTest", timeOut = 15*60*1000)
public class BackpressureStressTest extends MultiHotRodServersTest {

   private static final Log log = LogFactory.getLog(BackpressureStressTest.class);

   static final int NUM_SERVERS = 2;
   static final int NUM_OWNERS = 2;

   static final int NUM_CLIENTS = 1;
   static final int NUM_THREADS_PER_CLIENT = 6;
//   static final int NUM_THREADS_PER_CLIENT = 36;

   static final int NUM_OPERATIONS = 1_000_000; // per thread, per client
//   static final int NUM_OPERATIONS = 300; // per thread, per client
//   static final int NUM_OPERATIONS = 600; // per thread, per client

   static ExecutorService EXEC = Executors.newCachedThreadPool();

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(NUM_SERVERS, getCacheConfiguration());
   }

   private ConfigurationBuilder getCacheConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(NUM_OWNERS);
      return hotRodCacheConfiguration(builder);
   }

   RemoteCacheManager getRemoteCacheManager(int port) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(port);
      builder.connectionPool().maxActive(1).exhaustedAction(ExhaustedAction.WAIT);
      RemoteCacheManager rcm = new InternalRemoteCacheManager(builder.build());
      rcm.getCache();
      return rcm;
   }

   Map<String, RemoteCacheManager> createClients() {
      Map<String, RemoteCacheManager> remotecms = new HashMap<>(NUM_CLIENTS);
      for (int i = 0; i < NUM_CLIENTS; i++)
         remotecms.put("c" + i, getRemoteCacheManager(server(0).getPort()));

      return remotecms;
   }

   public void testAddClientListenerDuringOperations() {
      TestResourceTracker.testThreadStarted(this.getTestName());
      CyclicBarrier barrier = new CyclicBarrier((NUM_CLIENTS * NUM_THREADS_PER_CLIENT) + 1);
      AggregateCompletionStage<Void> aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
      Map<String, RemoteCacheManager> remotecms = createClients();

      for (Entry<String, RemoteCacheManager> e : remotecms.entrySet()) {
         RemoteCache<String, String> remote = e.getValue().getCache();

         for (int i = 0; i < NUM_THREADS_PER_CLIENT; i++) {
            Supplier<Void> call = new Put(barrier, remote, servers);
            aggregateCompletionStage.dependsOn(CompletableFuture.supplyAsync(call, EXEC));
         }
      }

      barrierAwait(barrier); // wait for all threads to be ready
      barrierAwait(barrier); // wait for all threads to finish

      try {
         aggregateCompletionStage.freeze().toCompletableFuture()
               .get(10, TimeUnit.MINUTES);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
         throw new AssertionError(e);
      }
   }

   static class Put implements Supplier<Void> {
      static final AtomicInteger offset = new AtomicInteger();

      final CyclicBarrier barrier;
      final RemoteCache<String, String> remote;
      final List<HotRodServer> servers;
      final String key;

      public Put(CyclicBarrier barrier,
            RemoteCache<String, String> remote, List<HotRodServer> servers) {
         this.barrier = barrier;
         this.remote = remote;
         this.servers = servers;
         this.key = "my-test-key";
      }


      @Override
      public Void get() {
         barrierAwait(barrier);
         String largeString = ("Put-" + offset.getAndIncrement()).repeat(100_000);
         try {
            for (int i = 0; i < NUM_OPERATIONS; i++) {
               remote.putAsync(key, largeString);
            }
            return null;
         } finally {
            barrierAwait(barrier);
         }
      }
   }

   static int barrierAwait(CyclicBarrier barrier) {
      try {
         return barrier.await();
      } catch (InterruptedException | BrokenBarrierException e) {
         throw new AssertionError(e);
      }
   }
}
