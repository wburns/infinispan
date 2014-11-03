package org.infinispan.notifications.cachelistener;

import org.infinispan.Cache;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.distribution.MagicKey;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Tests to make sure that event origin locality is set properly for various owners in a DIST cache
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.OriginLocalityListenerDistTest")
public class OriginLocalityListenerDistTest extends BaseDistFunctionalTest<MagicKey, Object> {

   public OriginLocalityListenerDistTest() {
      // this way we can extend this class and change the cache mode without a problem
      l1CacheEnabled = false;
   }

   // If we aren't an owner we shouldn't get the event
   public void testOriginForNonOwner() {
      CacheListener listener = new CacheListener();
      cache(0, cacheName).addListener(listener);

      cache(0, cacheName).put(new MagicKey(cache(1, cacheName), cache(2, cacheName)), "some-value");

      assertEquals(0, listener.events.size());
   }

   public void testOriginForBackupOwner() {
      assertEventLocal(cache(0, cacheName), new MagicKey(cache(1, cacheName), cache(0, cacheName)), true);
   }

   public void testOriginForPrimaryOwner() {
      assertEventLocal(cache(0, cacheName), new MagicKey(cache(0, cacheName), cache(1, cacheName)), true);
   }

   protected void assertEventLocal(Cache<? super MagicKey, Object> cache, MagicKey key, boolean shouldBeLocal) {
      CacheListener listener = getListener();
      cache.addListener(listener);

      cache.put(key, "some-value");

      boolean clusteredListener = listener.getClass().getAnnotation(Listener.class).clustered();

      assertEquals(clusteredListener ? 1 : 2, listener.events.size());

      assertEquals(shouldBeLocal, ((CacheEntryEvent)listener.events.get(0)).isOriginLocal());
      if (!clusteredListener) {
         assertEquals(shouldBeLocal, ((CacheEntryEvent)listener.events.get(1)).isOriginLocal());
      }
   }

   protected CacheListener getListener() {
      return new CacheListener();
   }
}
