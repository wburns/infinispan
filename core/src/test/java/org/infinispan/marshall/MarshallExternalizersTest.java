package org.infinispan.marshall;

import static org.infinispan.test.TestingUtil.k;
import static org.testng.AssertJUnit.assertEquals;

import java.lang.reflect.Method;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.PojoWithSerializeWith;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "marshall.MarshallExternalizersTest")
public class MarshallExternalizersTest extends MultipleCacheManagersTest {

   private static final String CACHE_NAME = MarshallExternalizersTest.class.getName();

   @Override
   protected void createCacheManagers() throws Throwable {
      CacheContainer cm1 = TestCacheManagerFactory.createClusteredCacheManager();
      CacheContainer cm2 = TestCacheManagerFactory.createClusteredCacheManager();
      registerCacheManager(cm1, cm2);
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      defineConfigurationOnAllManagers(CACHE_NAME, cfg);
      waitForClusterToForm(CACHE_NAME);
   }

   public void testReplicateMarshallableByPojo(Method m) {
      PojoWithSerializeWith pojo = new PojoWithSerializeWith(17, k(m));
      doReplicatePojo(m, pojo);
   }

   @Test(dependsOnMethods = "testReplicateMarshallableByPojo")
   public void testReplicateMarshallableByPojoToNewJoiningNode(Method m) {
      PojoWithSerializeWith pojo = new PojoWithSerializeWith(85, k(m));
      doReplicatePojoToNewJoiningNode(m, pojo);
   }

   protected void doReplicatePojo(Method m, Object o) {
      Cache cache1 = manager(0).getCache(CACHE_NAME);
      Cache cache2 = manager(1).getCache(CACHE_NAME);
      cache1.put(k(m), o);
      assertEquals(o, cache2.get(k(m)));
   }

   protected void doReplicatePojoToNewJoiningNode(Method m, Object o) {
      Cache cache1 = manager(0).getCache(CACHE_NAME);
      EmbeddedCacheManager cm = createCacheManager();
      try {
         Cache cache3 = cm.getCache(CACHE_NAME);
         cache1.put(k(m), o);
         assertEquals(o, cache3.get(k(m)));
      } finally {
         cm.stop();
      }
   }

   private EmbeddedCacheManager createCacheManager() {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager();
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      cm.defineConfiguration(CACHE_NAME, cfg.build());
      return cm;
   }
}
