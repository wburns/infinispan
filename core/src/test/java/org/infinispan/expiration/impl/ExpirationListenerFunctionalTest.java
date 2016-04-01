package org.infinispan.expiration.impl;

import org.infinispan.Cache;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachelistener.event.CacheEntryExpiredEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

@Test(groups = "functional", testName = "expiration.impl.ExpirationListenerFunctionalTest")
public class ExpirationListenerFunctionalTest extends ExpirationFunctionalTest {

   protected ExpiredCacheListener listener;

   public class TestClass extends ExpiredCacheListener {
      private final Cache cache;

      public TestClass(Cache cache) {
         this.cache = cache;
      }

      @Override
      public void handle(CacheEntryExpiredEvent e) {
         super.handle(e);
         cache.put(e.getKey(), e.getValue());
      }
   };
   protected ExpirationManager manager;

   @Override
   protected void afterCacheCreated(EmbeddedCacheManager cm) {
      listener = new TestClass(cache);
      cache.addListener(listener);
      manager = TestingUtil.extractComponent(cache, ExpirationManager.class);
   }

   @AfterMethod
   public void resetListener() {
      listener.reset();
   }

   @Override
   public void testSimpleExpirationLifespan() throws Exception {
      super.testSimpleExpirationLifespan();
      manager.processExpiration();
      assertExpiredEvents(SIZE);
   }

   @Override
   public void testSimpleExpirationMaxIdle() throws Exception {
      super.testSimpleExpirationMaxIdle();
      manager.processExpiration();
      assertExpiredEvents(SIZE);
   }

   private void assertExpiredEvents(int count) {
      assertEquals(count, listener.getInvocationCount());
      listener.getEvents().forEach(event -> {
         assertEquals(Event.Type.CACHE_ENTRY_EXPIRED, event.getType());
         assertEquals(cache, event.getCache());
         assertFalse(event.isPre());
         assertNotNull(event.getKey());
         assertNotNull(event.getValue());
         assertNotNull(event.getMetadata());
      });
   }
}
