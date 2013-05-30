package org.infinispan.replication;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.testng.annotations.Test;

/**
 * Test class to ensure that asymmetric replicated caches work properly
 *
 * @author wburns
 * @since 4.0
 */
@Test
public class AsymmetricSyncReplTest extends MultipleCacheManagersTest {
   private static final String CACHENAME = "Asymmetric";
   private final static String KEY = "key";
   private final static String VALUE = "value";

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultConfig = new ConfigurationBuilder();
      defaultConfig
            .clustering()
               .cacheMode(CacheMode.REPL_SYNC)
            .transaction()
               .transactionMode(TransactionMode.TRANSACTIONAL)
               .transactionManagerLookup(new DummyTransactionManagerLookup());
      createCluster(defaultConfig, 2);

      manager(1).defineConfiguration(CACHENAME, new ConfigurationBuilder().build());
      waitForClusterToForm();
   }

//   public void testSimplePutAndRemove() throws InterruptedException {
//      Cache<Object, Object> cache = cache(0, CACHENAME);
//      final Object someValue = new Object();
//      for (int i = 0; i < 100; ++i) {
//         cache.put(someValue, someValue);
//         Thread.sleep(100);
//         cache.remove(someValue);
//      }
//   }

   public void testSomethingElse() {
      Cache<Object, Object> cache = cache(0, CACHENAME);
      cache.put(KEY, VALUE);
      cache.remove(KEY);
   }
}
