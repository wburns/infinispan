package org.infinispan.notifications.cachelistener.cluster;

import org.infinispan.distribution.MagicKey;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheListener;
import org.infinispan.notifications.cachelistener.OriginLocalityListenerDistTest;
import org.testng.annotations.Test;

/**
 * An origin locality test but using a cluster listener for a dist cache
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.cluster.OriginLocalityClusterListenerDistTest")
public class OriginLocalityClusterListenerDistTest extends OriginLocalityListenerDistTest {

   // In a cluster listener we still receive the notification
   public void testOriginForNonOwner() {
      assertEventLocal(cache(0, cacheName), new MagicKey(cache(1, cacheName), cache(0, cacheName)), true);
   }

   @Override
   protected CacheListener getListener() {
      return new ClusterCacheListener();
   }

   @Listener(clustered = true)
   protected class ClusterCacheListener extends CacheListener {

   }
}
