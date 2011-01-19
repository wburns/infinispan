package org.infinispan.notifications;

import org.infinispan.config.Configuration;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.event.MergeEvent;
import org.infinispan.remoting.rpc.RpcManagerImpl;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.protocols.DISCARD;
import org.jgroups.stack.ProtocolStack;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "notification.MergeViewTest")
public class MergeViewTest extends MultipleCacheManagersTest {

   private static Log log = LogFactory.getLog(MergeViewTest.class);

   private DISCARD discard;
   private MergeListener ml0;
   private MergeListener ml1;

   @Override
   protected void createCacheManagers() throws Throwable {
      addClusterEnabledCacheManager(Configuration.CacheMode.REPL_SYNC, true);

      ml0 = new MergeListener();
      manager(0).addListener(ml0);

      discard();

      addClusterEnabledCacheManager(Configuration.CacheMode.REPL_SYNC, true);
      ml1 = new MergeListener();
      manager(1).addListener(ml1);

      cache(0).put("k", "v0");
      cache(1).put("k", "v1");
      Thread.sleep(10000);


      assert advancedCache(0).getRpcManager().getTransport().getMembers().size() == 1;
      assert advancedCache(1).getRpcManager().getTransport().getMembers().size() == 1;
   }

   private void discard() throws Exception {
      RpcManagerImpl rpcManager = (RpcManagerImpl) advancedCache(0).getRpcManager();
      JGroupsTransport transport = (JGroupsTransport) rpcManager.getTransport();
      ProtocolStack protocolStack = transport.getChannel().getProtocolStack();

      discard = new DISCARD();
      discard.setDiscardAll(true);
      protocolStack.insertProtocol(discard, ProtocolStack.ABOVE, protocolStack.getTransport().getClass());
   }

   public void testMergeViewHappens() {
      discard.setDiscardAll(false);
      TestingUtil.blockUntilViewsReceived(60000, cache(0), cache(1));

      cache(0).put("k", "v0");
      assertEquals(cache(0).get("k"), "v0");
      assertEquals(cache(1).get("k"), "v0");
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return ml0.isMerged && ml1.isMerged;
         }
      });
   }

   @Listener
   public static class MergeListener {
      volatile boolean isMerged;

      @Merged
      public void viewMerged(MergeEvent vce) {
         log.info("vce = " + vce);
         System.out.println("vce = " + vce);
         isMerged = true;
      }
   }
}
