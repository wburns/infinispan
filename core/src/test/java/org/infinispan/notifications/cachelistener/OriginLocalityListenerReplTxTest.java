package org.infinispan.notifications.cachelistener;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.testng.annotations.Test;

/**
 * Tests to make sure that event origin locality is set properly for various owners in a transactional REPL cache
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.OriginLocalityListenerReplTest")
public class OriginLocalityListenerReplTxTest extends OriginLocalityListenerReplTest {
   public OriginLocalityListenerReplTxTest() {
      tx = true;
   }
}
