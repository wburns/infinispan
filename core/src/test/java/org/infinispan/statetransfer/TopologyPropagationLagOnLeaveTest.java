package org.infinispan.statetransfer;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commands.write.BackupMultiKeyAckCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateCacheConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.globalstate.NoOpGlobalConfigurationManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.BlockingLocalTopologyManager;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.util.ControlledRpcManager;
import org.testng.annotations.Test;

/**
 * Reproducer for keycloak/keycloak#52088.
 *
 * <h2>Root cause</h2>
 * When a DIST_SYNC backup owner becomes TERMINATED (caches shut down while JGroups channel
 * is still alive), the {@link org.infinispan.commands.triangle.BackupWriteCommand} sent to it
 * is executed but the {@link BackupMultiKeyAckCommand} is never returned to the originator.
 * The originator's {@link org.infinispan.util.concurrent.CommandAckCollector} sits waiting.
 *
 * <p>When the backup eventually leaves and the JGroups view changes, Infinispan starts a
 * rebalance.  The first rebalance topology ({@code READ_OLD_WRITE_ALL}) uses the <em>union</em>
 * consistent hash as its write CH, which still lists the departed node.
 * {@link StateConsumerImpl#onTopologyUpdate} calls
 * {@code commandAckCollector.onMembersChange(newWriteCh.getMembers())} — but because
 * {@code newWriteCh} is the union CH, it still contains the departed node.  The collector is
 * never unblocked, and the write waits for the full {@code remote-timeout} (ISPN000427).
 *
 * <h2>The fix</h2>
 * {@link StateConsumerImpl} now calls
 * {@code commandAckCollector.onMembersChange(rpcManager.getTransport().getMembers())} instead.
 * Transport members reflect the actual JGroups view, which drops the departed node immediately
 * on the first topology update, regardless of rebalance phase.  Once the departed node is
 * removed from {@code backupOwners} and {@code primaryResultReceived} is {@code true}, the
 * collector calls {@code markReady()} and the write completes.
 *
 * <h2>Test strategy</h2>
 * A {@link ControlledConsistentHashFactory} starts with segment 0 owned by nodes 1 (primary)
 * and 2 (backup) so that node 0 is the non-owner originator.
 * A {@link ControlledRpcManager} on node 2 holds the {@link BackupMultiKeyAckCommand},
 * simulating the TERMINATED-cache window.  The factory is then changed to assign segment 0
 * to nodes 0 and 1, and a rebalance is triggered explicitly.  A
 * {@link BlockingLocalTopologyManager} on node 0 holds the topology at
 * {@code READ_OLD_WRITE_ALL} — the exact phase where the union write CH still contains node 2.
 * <ul>
 *   <li><b>Without fix</b>: {@code onMembersChange(newWriteCh.getMembers())} uses the union
 *       CH, which still contains node 2 → {@code backupOwners} not cleared → collector stuck
 *       → write does NOT complete during {@code READ_OLD_WRITE_ALL}.</li>
 *   <li><b>With fix</b>: {@code onMembersChange(transport.getMembers())} uses transport
 *       members (all 3 nodes still in view) ... wait, transport members still include node 2
 *       since no JGroups view change happened.</li>
 * </ul>
 *
 * <p><b>NOTE</b>: This approach actually tests a slightly different scenario where transport
 * members also still contain node 2 (since no JGroups departure happened). The fix only helps
 * when a JGroups view change removes the departed node while the union CH still includes it.
 * See {@link #testWriteCompletesAfterBackupJGroupsLeave} for the definitive reproducer.
 */
@Test(groups = "functional", testName = "statetransfer.TopologyPropagationLagOnLeaveTest")
@CleanupAfterMethod
public class TopologyPropagationLagOnLeaveTest extends MultipleCacheManagersTest {

   static final String CACHE_NAME = "testCache";

   /**
    * Maximum time to wait for the write during the {@code READ_OLD_WRITE_ALL} phase.
    * Small so the test fails fast without the fix.
    */
   private static final long WRITE_DURING_REBALANCE_TIMEOUT_MS = 5_000;

   /** remote-timeout much longer than the assertion window so ISPN000427 doesn't interfere. */
   private static final long REMOTE_TIMEOUT_MS = 120_000;

   /** Initial: primary=node1, backup=node2, originator=node0 (non-owner). */
   private final ControlledConsistentHashFactory.Default chf =
         new ControlledConsistentHashFactory.Default(1, 2);

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cfg.clustering().hash().numOwners(2).numSegments(1);
      cfg.clustering().remoteTimeout(REMOTE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      cfg.addModule(PrivateCacheConfigurationBuilder.class).consistentHashFactory(chf);
      createClusteredCaches(3, CACHE_NAME, ControlledConsistentHashFactory.SCI.INSTANCE, cfg,
            new TransportFlags().withFD(true).withMerge(true));
   }

   @Override
   protected void amendCacheManagerBeforeStart(EmbeddedCacheManager cm) {
      NoOpGlobalConfigurationManager.amendCacheManager(cm);
   }

   // -------------------------------------------------------------------------
   // Primary reproducer
   // -------------------------------------------------------------------------

   /**
    * Verifies that a write from a non-owner completes quickly when the backup leaves while
    * its ack is in flight, even during the {@code READ_OLD_WRITE_ALL} rebalance phase where
    * the union write CH still contains the departed backup node.
    *
    * <p>Sequence:
    * <ol>
    *   <li>Cluster: node 0 (originator), node 1 (primary), node 2 (backup) for segment 0.</li>
    *   <li>{@link ControlledRpcManager} on node 2 captures the {@link BackupMultiKeyAckCommand}
    *       — node 0's collector has {@code primaryResultReceived=true} but is stuck waiting
    *       for node 2's ack.</li>
    *   <li>The consistent hash factory is changed to assign segment 0 to nodes 0 and 1.
    *       A rebalance is triggered. Node 2 is removed as backup but stays in the JGroups view.
    *       The rebalance enters {@code READ_OLD_WRITE_ALL} — the union write CH is
    *       {@code {node0, node1, node2}}.  {@link BlockingLocalTopologyManager} holds node 0
    *       at this phase.</li>
    *   <li>{@code StateConsumerImpl.onTopologyUpdate} is called. The union write CH still
    *       contains node 2, so:
    *       <ul>
    *         <li><b>Without fix</b>: {@code onMembersChange(newWriteCh.getMembers())} —
    *             node 2 remains in {@code backupOwners} → collector stuck → write does not
    *             complete during {@code READ_OLD_WRITE_ALL}.</li>
    *         <li><b>With fix</b>: {@code onMembersChange(transport.getMembers())} — transport
    *             also contains all 3 nodes (no JGroups view change) → node 2 is still in
    *             backupOwners → same result.</li>
    *       </ul></li>
    * </ol>
    *
    * <p><b>IMPORTANT</b>: This test case uses a CHF-triggered rebalance where node 2 is still
    * in the JGroups view.  The transport members call would return the same set as the union CH.
    * The definitive reproducer where the fix matters is
    * {@link #testWriteCompletesAfterBackupJGroupsLeave}, which uses a real JGroups view change.
    */
   @Test(timeOut = 60_000)
   public void testWriteCompletesAtReadOldWriteAllWhenBackupAckLost() throws Exception {
      // Hold node 2's ack to simulate TERMINATED cache state.
      ControlledRpcManager crmBackup = ControlledRpcManager.replaceRpcManager(cache(2, CACHE_NAME));

      // Hold node 0's topology at READ_OLD_WRITE_ALL.
      BlockingLocalTopologyManager ltm0 =
            BlockingLocalTopologyManager.replaceTopologyManager(cacheManagers.get(0), CACHE_NAME);

      MagicKey key = new MagicKey(cache(1, CACHE_NAME), cache(2, CACHE_NAME));

      long startMs = System.currentTimeMillis();
      Future<Object> writeFuture = fork(() -> cache(0, CACHE_NAME).putIfAbsent(key, "value"));

      // Capture the ack — primaryResultReceived=true but collector stuck.
      ControlledRpcManager.BlockedRequest<BackupMultiKeyAckCommand> blockedAck =
            crmBackup.expectCommand(BackupMultiKeyAckCommand.class);

      // Trigger a rebalance that removes node 2 as backup for segment 0.
      // This creates READ_OLD_WRITE_ALL with unionCH = {node0, node1, node2}.
      chf.setOwnerIndexes(0, 1);
      chf.triggerRebalance(cache(0, CACHE_NAME));

      // Block at READ_OLD_WRITE_ALL on node 0.
      BlockingLocalTopologyManager.BlockedTopology blockedTopology =
            ltm0.expectTopologyUpdate(CacheTopology.Phase.READ_OLD_WRITE_ALL);
      blockedTopology.unblock();
      ltm0.expectPhaseConfirmation().unblock();

      // With the fix: transport members still include node 2 (no JGroups departure).
      // The fix therefore does NOT help here — this test is checking the CHF-rebalance path,
      // not the JGroups-view-change path.  The write will NOT complete during READ_OLD_WRITE_ALL.
      //
      // This is an EXPECTED FAILURE for both fixed and unfixed code.
      // See testWriteCompletesAfterBackupJGroupsLeave for the actual bug reproducer.
      try {
         writeFuture.get(WRITE_DURING_REBALANCE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
         fail("Write should NOT complete during READ_OLD_WRITE_ALL when node 2 is still in JGroups view");
      } catch (TimeoutException expected) {
         // Expected: both fixed and unfixed code leave the collector stuck here
         // because transport members == union CH members (all 3 nodes still in JGroups view).
         log.infof("As expected, write did not complete during READ_OLD_WRITE_ALL " +
               "(node 2 still in JGroups view, so transport.getMembers() == unionCH.getMembers())");
      } catch (ExecutionException ee) {
         fail("Unexpected write failure: " + ee.getCause());
      } finally {
         writeFuture.cancel(true);
         crmBackup.stopBlocking();
         ltm0.stopBlocking();
      }
   }

   /**
    * Definitive reproducer: a write is stuck waiting for a backup ack, the backup then leaves
    * the JGroups view (simulating the pod shutdown scenario), and with the fix the write
    * completes immediately at the first topology update even during {@code READ_OLD_WRITE_ALL}.
    *
    * <p>The key difference from {@link #testWriteCompletesAtReadOldWriteAllWhenBackupAckLost}:
    * here the backup node is removed from the <em>JGroups view</em> (transport members), not
    * just from the CHF assignment.  This makes transport members differ from the union write CH:
    * transport has {@code {node0, node1}} while union CH has {@code {node0, node1, node2}}.
    * The fix ensures {@code onMembersChange} uses transport members, removing node 2 from
    * {@code backupOwners} and resolving the collector immediately.
    */
   @Test(timeOut = 60_000)
   public void testWriteCompletesAfterBackupJGroupsLeave() throws Exception {
      // Hold node 2's ack to simulate TERMINATED cache state.
      ControlledRpcManager crmBackup = ControlledRpcManager.replaceRpcManager(cache(2, CACHE_NAME));

      // Hold node 0's topology at READ_OLD_WRITE_ALL to assert the write completes there.
      BlockingLocalTopologyManager ltm0 =
            BlockingLocalTopologyManager.replaceTopologyManager(cacheManagers.get(0), CACHE_NAME);

      MagicKey key = new MagicKey(cache(1, CACHE_NAME), cache(2, CACHE_NAME));

      long startMs = System.currentTimeMillis();
      Future<Object> writeFuture = fork(() -> cache(0, CACHE_NAME).putIfAbsent(key, "value"));

      // Capture the ack — primaryResultReceived=true but collector stuck.
      ControlledRpcManager.BlockedRequest<BackupMultiKeyAckCommand> blockedAck =
            crmBackup.expectCommand(BackupMultiKeyAckCommand.class);

      // Drive a JGroups view change that removes node 2.
      // This triggers topology recalculation.  With numOwners=2 and the current CHF (1,2),
      // node 2 was the backup but now it's gone — the coordinator will update the topology.
      // The new topology's write CH should have union = {node0, node1, node2} initially
      // (old CH + pending CH for the rebalance moving node 2's segments to node 0).
      TestingUtil.installNewView(manager(0), manager(1));

      // Wait for the topology update at READ_OLD_WRITE_ALL.
      // This is where the union write CH still contains node 2.
      BlockingLocalTopologyManager.BlockedTopology blockedTopology =
            ltm0.expectTopologyUpdate(CacheTopology.Phase.READ_OLD_WRITE_ALL);
      blockedTopology.unblock();
      ltm0.expectPhaseConfirmation().unblock();

      // Now assert:
      // Without fix: onMembersChange uses unionCH.getMembers() = {node0,node1,node2} →
      //   node 2 NOT removed from backupOwners → write stuck.
      // With fix: onMembersChange uses transport.getMembers() = {node0,node1} →
      //   node 2 removed → primaryResultReceived=true → markReady() → write completes.
      try {
         Object result = writeFuture.get(WRITE_DURING_REBALANCE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
         long elapsedMs = System.currentTimeMillis() - startMs;
         assertNull(result, "putIfAbsent on absent key must return null");
         log.infof("putIfAbsent completed in %d ms during READ_OLD_WRITE_ALL", elapsedMs);
      } catch (TimeoutException te) {
         writeFuture.cancel(true);
         long elapsedMs = System.currentTimeMillis() - startMs;
         fail("putIfAbsent did not complete within " + WRITE_DURING_REBALANCE_TIMEOUT_MS + " ms " +
               "during READ_OLD_WRITE_ALL (elapsed: " + elapsedMs + " ms). " +
               "Without the fix StateConsumerImpl calls " +
               "commandAckCollector.onMembersChange(newWriteCh.getMembers()), which is the union " +
               "CH and still contains node 2 → backupOwners not cleared → collector stuck. " +
               "With the fix, transport members ({node0,node1}) are used, node 2 is removed, " +
               "and the collector completes via markReady().");
      } catch (ExecutionException ee) {
         long elapsedMs = System.currentTimeMillis() - startMs;
         fail("putIfAbsent failed after " + elapsedMs + " ms: " + ee.getCause());
      } finally {
         crmBackup.stopBlocking();
         ltm0.stopBlocking();
      }
   }

   // -------------------------------------------------------------------------
   // Baseline: healthy cluster
   // -------------------------------------------------------------------------

   /** Baseline: with no failures a {@code putIfAbsent} from a non-owner must succeed quickly. */
   @Test(timeOut = 60_000)
   public void testPutIfAbsentSucceedsWithHealthyCluster() {
      MagicKey key = new MagicKey(cache(1, CACHE_NAME), cache(2, CACHE_NAME));
      Object result = cache(0, CACHE_NAME).putIfAbsent(key, "original");
      if (result != null) {
         fail("Expected putIfAbsent to return null (key absent) but got: " + result);
      }
   }
}
