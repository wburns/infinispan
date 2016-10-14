package org.infinispan.commands;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import io.netty.buffer.PooledByteBufAllocator;

/**
 */
@Test(groups = "functional", testName = "api.ByteArrayCacheTest")
public class OffHeapSingleNodeTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      // If key equivalence is set, it will also be used for value
      builder.storeAsBinary().enable().storeValuesAsBinary(false);
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   public void testByteArrayGet() throws InterruptedException {
      Map<byte[], byte[]> map = cache();

//      int cacheSize = 2_000_000;
//      int cacheSize = 400_001;
      int cacheSize = 400_001;
//      int cacheSize = 750_000;
      for (int i = 0; i < cacheSize; ++i) {
         byte[] key = randomBytes(KEY_SIZE);
         byte[] prev = map.put(key, randomBytes(VALUE_SIZE));
         if (prev != null) {
            System.out.println("Replaced a value!");
            System.currentTimeMillis();
         }
      }
      System.out.println(PooledByteBufAllocator.DEFAULT.dumpStats());
      int mapSize = map.size();
      assertEquals(cacheSize, mapSize);
      System.out.println("Completed!");
//      System.out.println("Size = " + map.entrySet().stream().mapToInt(e -> e.getKey().length + e.getValue().length).sum());
      Thread.sleep(TimeUnit.DAYS.toMillis(1));
   }

   private final static int KEY_SIZE = 20;
   private final static int VALUE_SIZE = 4000;

   public byte[] randomBytes(int size) {
      byte[] bytes = new byte[size];
      random.nextBytes(bytes);
      return bytes;
   }

   private final Random random = new Random();
}
