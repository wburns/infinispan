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
@Test(groups = "functional", testName = "notifications.cachelistener.cluster.OriginLocalityClusterListenerDistTxTest")
public class OriginLocalityClusterListenerDistTxTest extends OriginLocalityClusterListenerDistTest {

   public OriginLocalityClusterListenerDistTxTest() {
      tx = true;
   }
}
