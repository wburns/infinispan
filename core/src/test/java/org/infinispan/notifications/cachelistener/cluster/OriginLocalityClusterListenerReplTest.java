package org.infinispan.notifications.cachelistener.cluster;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheListener;
import org.infinispan.notifications.cachelistener.OriginLocalityListenerDistTest;
import org.testng.annotations.Test;

/**
 * An origin locality test but using a cluster listener for a repl cache
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.cluster.OriginLocalityClusterListenerReplTest")
public class OriginLocalityClusterListenerReplTest extends OriginLocalityListenerDistTest {

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
