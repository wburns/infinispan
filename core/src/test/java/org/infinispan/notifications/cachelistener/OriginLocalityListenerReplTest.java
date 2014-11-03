package org.infinispan.notifications.cachelistener;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.distribution.MagicKey;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Tests to make sure that event origin locality is set properly for various owners in a REPL cache
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.OriginLocalityListenerReplTest")
public class OriginLocalityListenerReplTest extends OriginLocalityListenerDistTest {
   @Override
   protected ConfigurationBuilder buildConfiguration() {
      ConfigurationBuilder builder = super.buildConfiguration();
      builder.clustering().cacheMode(CacheMode.REPL_SYNC);
      return builder;
   }

   // REPL should always receive is local as true no matter the owner
   public void testOriginForNonOwner() {
      assertEventLocal(cache(0, cacheName), new MagicKey(cache(1, cacheName), cache(0, cacheName)), true);
   }
}
