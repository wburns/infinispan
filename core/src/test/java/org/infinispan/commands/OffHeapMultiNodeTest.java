package org.infinispan.commands;

import static org.testng.AssertJUnit.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "commands.OffHeapMultiNodeTest")
public class OffHeapMultiNodeTest extends MultipleCacheManagersTest {
   protected int numberOfKeys = 10;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dcc.storeAsBinary().enable().storeValuesAsBinary(false);
      createCluster(dcc, 4);
      waitForClusterToForm();
   }

   public void testPutMapCommand() {
      for (int i = 0; i < numberOfKeys; ++i) {
         assert cache(0).get("key" + i) == null;
         assert cache(1).get("key" + i) == null;
         assert cache(2).get("key" + i) == null;
         assert cache(3).get("key" + i) == null;
      }

      Map<String, String> map = new HashMap<String, String>();
      for (int i = 0; i < numberOfKeys; ++i) {
         map.put("key" + i, "value" + i);
      }

      cache(0).putAll(map);

      for (int i = 0; i < numberOfKeys; ++i) {
         assertEquals("value" + i, cache(0).get("key" + i));
         final int finalI = i;
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return cache(1).get("key" + finalI).equals("value" + finalI);
            }
         });
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return cache(2).get("key" + finalI).equals("value" + finalI);
            }
         });
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return cache(3).get("key" + finalI).equals("value" + finalI);
            }
         });
      }
   }
}
