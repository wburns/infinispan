package org.infinispan.notifications.cachelistener;

import org.infinispan.Cache;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.distribution.MagicKey;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Tests to make sure that event origin locality is set properly for various owners in a transactional DIST cache
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.OriginLocalityListenerDistTxTest")
public class OriginLocalityListenerDistTxTest extends OriginLocalityListenerDistTest {
   public OriginLocalityListenerDistTxTest() {
      tx = true;
   }
}
