package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;
import org.testng.annotations.Test;

/**
 * Tests Hot Rod logic when interacting with distributed caches with a client that may send too many requests
 */
@Test(groups = "functional", testName = "server.hotrod.HotRodBackpressureTest")
public class HotRodBackpressureTest extends HotRodMultiNodeTest {

   @Override
   protected String cacheName() {
      return "HotRodBackpressureTest";
   }

   @Override
   protected ConfigurationBuilder createCacheConfig() {
      ConfigurationBuilder cfg = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      return cfg;
   }

   public void testALotOfAsyncPuts(Method m) throws ExecutionException, InterruptedException, TimeoutException {
      HotRodClient client1 = clients().get(0);
      byte[] key = "key".getBytes(StandardCharsets.UTF_8);
      byte[] value = "extremelylargevalue".repeat(1_000_000).getBytes(StandardCharsets.UTF_8);
      AggregateCompletionStage<Void> aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
      for (int i = 0; i < 10_000; ++i) {
         aggregateCompletionStage.dependsOn(client1.putAsync(key, -1, -1, value));
      }

      aggregateCompletionStage.freeze().toCompletableFuture().get(10, TimeUnit.SECONDS);
   }
}
