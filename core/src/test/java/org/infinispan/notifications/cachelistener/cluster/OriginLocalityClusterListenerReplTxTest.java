package org.infinispan.notifications.cachelistener.cluster;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.notifications.cachelistener.OriginLocalityListenerDistTest;
import org.testng.annotations.Test;

/**
 * An origin locality test but using a cluster listener for a repl cache
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.cluster.OriginLocalityClusterListenerReplTest")
public class OriginLocalityClusterListenerReplTxTest extends OriginLocalityListenerDistTest {

   public OriginLocalityClusterListenerReplTxTest() {
      tx = true;
   }
}
