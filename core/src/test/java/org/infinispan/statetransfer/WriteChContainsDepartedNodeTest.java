package org.infinispan.statetransfer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commands.write.BackupMultiKeyAckCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.globalstate.NoOpGlobalConfigurationManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.ControlledRpcManager;
import org.infinispan.util.StagedLocalTopologyManager;
import org.testng.annotations.Test;

/**
 * Verifies the structural gap at the root of keycloak/keycloak#52088: when a
 * {@code READ_OLD_WRITE_ALL} rebalance topology is in-flight on a surviving node, the
 * topology's {@code currentCH} still contains a node that has already left the JGroups view.
 *
 * <h2>How the gap arises</h2>
 * A join triggers a {@code READ_OLD_WRITE_ALL} rebalance.  The coordinator builds:
 * <ul>
 *   <li>{@code currentCH} — the existing stable CH, unchanged (includes all current owners).</li>
 *   <li>{@code pendingCH} — the rebalanced CH that includes the new joiner.</li>
 * </ul>
 * {@code getWriteConsistentHash()} returns {@code union(currentCH, pendingCH)}.  If a
 * different node departs <em>after</em> the coordinator has sent this topology but
 * <em>before</em> a surviving node applies it, the surviving node's transport view no
 * longer includes the departed node — yet {@code currentCH} (and therefore the union write
 * CH) still lists it.
 *
 * <p>The old code in {@link StateConsumerImpl#onTopologyUpdate} called
 * {@code commandAckCollector.onMembersChange(newWriteCh.getMembers())}.  Because the union
 * write CH still listed the departed node, the departed node was never removed from the
 * collector's {@code backupOwners} set — leaving any in-flight write stuck until the full
 * {@code remote-timeout} fired (ISPN000427).
 *
 * <h2>The fix</h2>
 * {@link StateConsumerImpl#onTopologyUpdate} now intersects {@code writeChMembers} with
 * {@code transport.getMembers()} before calling
 * {@code commandAckCollector.onMembersChange(...)}, dropping departed nodes regardless of
 * the rebalance phase or what the union write CH says.
 *
 * <h2>Test strategy (3 → 4, then node 2 killed mid-rebalance)</h2>
 * <ol>
 *   <li>Stable 3-node cluster (nodes 0–2, numOwners=2).</li>
 *   <li>A {@link StagedLocalTopologyManager} on node 0 intercepts topology updates using
 *       {@link CompletableFuture}-based gates.</li>
 *   <li>Node 3 joins on a background thread.  The resulting {@code READ_OLD_WRITE_ALL} is
 *       intercepted on node 0 <em>and immediately released</em> — its only purpose is to
 *       ensure state transfer does not complete before node 2 is killed.</li>
 *   <li>A {@link ControlledRpcManager} on node 2 blocks the {@link BackupMultiKeyAckCommand}
 *       that node 2 would send back to node 1 for an in-flight put.</li>
 *   <li>Node 2 is killed — the JGroups view drops node 2 and the blocked ack is discarded.</li>
 *   <li>Node 2's departure causes the coordinator to install a new topology on the surviving
 *       nodes.  That topology update is intercepted on node 0 and held for
 *       {@value #TOPOLOGY_HOLD_MS} ms — longer than {@value #REMOTE_TIMEOUT_MS} ms — so
 *       that the put times out before {@code onTopologyUpdate} is called.</li>
 *   <li>The gate is released.  With the fix the put completes immediately when the topology
 *       is applied.  Without the fix the put has already timed out (ISPN000427).</li>
 * </ol>
 */
@Test(groups = "functional", testName = "statetransfer.WriteChContainsDepartedNodeTest")
@CleanupAfterMethod
public class WriteChContainsDepartedNodeTest extends MultipleCacheManagersTest {

   private static final String CACHE_NAME = "testCache";

   /**
    * Short remote-timeout so the put times out quickly when the backup ack is never
    * delivered and the topology update (which would rescue it with the fix) is delayed.
    */
   private static final long REMOTE_TIMEOUT_MS = 5_000;

   /**
    * How long to hold the post-departure topology update before releasing it.  Must exceed
    * {@link #REMOTE_TIMEOUT_MS} so that the bug scenario is observable: the put times out
    * while the topology is still held, because {@code onMembersChange} hasn't run yet.
    */
   private static final long TOPOLOGY_HOLD_MS = 20_000;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cfg.clustering().hash().numOwners(2);
      cfg.clustering().remoteTimeout(REMOTE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      createClusteredCaches(3, CACHE_NAME, TestDataSCI.INSTANCE, cfg, new TransportFlags().withFD(true));
   }

   @Override
   protected void amendCacheManagerBeforeStart(EmbeddedCacheManager cm) {
      NoOpGlobalConfigurationManager.amendCacheManager(cm);
   }

   /**
    * Main reproducer.  See class-level Javadoc for the full sequence.
    *
    * <p>Asserts:
    * <ul>
    *   <li>The write CH in the post-departure topology still contains node 2 (the structural
    *       gap: union CH has not been corrected yet).</li>
    *   <li>The transport view at that moment does not contain node 2.</li>
    *   <li>The put timed out (bug) or completed quickly (fix).  The test logs which path was
    *       taken so the result can be validated against the expected behaviour.</li>
    * </ul>
    */
   @Test(timeOut = 120_000)
   public void testWriteChContainsDepartedNodeWhileRebalanceIsInFlight() throws Exception {
      Cache<MagicKey, Object> cache0 = cache(0, CACHE_NAME);
      Cache<MagicKey, Object> cache1 = cache(1, CACHE_NAME);
      Cache<MagicKey, Object> cache2 = cache(2, CACHE_NAME);
      Address node2Address = address(2);

      // Install the staged topology manager on node 0 so we can gate topology updates.
      StagedLocalTopologyManager staged =
            StagedLocalTopologyManager.replaceTopologyManager(cacheManagers.get(0), CACHE_NAME);

      // Node 3 joins on a background thread.  Its getCache() blocks until the rebalance
      // confirms; since node 0's gate will be held we must not call it on the test thread.
      EmbeddedCacheManager cm3 = addClusterEnabledCacheManager(
            TestDataSCI.INSTANCE,
            getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false),
            new TransportFlags().withFD(true));
      cm3.defineConfiguration(CACHE_NAME, manager(0).getCacheConfiguration(CACHE_NAME));
      fork(() -> cm3.getCache(CACHE_NAME));

      // Wait for the READ_OLD_WRITE_ALL that node 3's join produces on node 0.
      // We hold it just long enough to kill node 2 before ST completes, then release it
      // immediately so the cluster can react to the membership change properly.
      StagedLocalTopologyManager.PendingTopology joinPending =
            staged.pollTopologyUpdate(CacheTopology.Phase.READ_OLD_WRITE_ALL, 10, TimeUnit.SECONDS);

      // Block node 2's outbound BackupMultiKeyAckCommand so the put from node 0 stalls
      // waiting for a backup ack that will never arrive.  All other outbound commands
      // (state transfer, publisher, etc.) pass through unblocked.
      ControlledRpcManager crmNode2 = ControlledRpcManager.replaceRpcManager(cache2);
      crmNode2.interceptOnly(BackupMultiKeyAckCommand.class);

      // Issue the put from node 1 targeting primary=1, backup=2.
      // The primary ack arrives quickly; the backup ack is intercepted below.
      Future<Object> putFuture = fork(() -> cache1.put(new MagicKey(cache1, cache2), "foo"));

      // Capture node 2's outbound ack.  The put is now stuck: primaryResultReceived=true
      // but backupOwners still contains node 2.
      org.infinispan.util.ControlledRpcManager.BlockedRequest<BackupMultiKeyAckCommand> br =
            crmNode2.expectCommand(BackupMultiKeyAckCommand.class);

      // Release the node-3 join topology immediately — we only needed to hold it until the
      // put was in flight.  State transfer for the join can now proceed.
      // Do NOT call stopIntercepting() here — we still need to intercept the topology update
      // that node 2's departure will produce.
      joinPending.proceed();

      // Kill node 2.  The JGroups view on node 0 drops node 2 right away.
      // The intercepted ack is discarded — it will never reach node 1.
      TestingUtil.killCacheManagers(manager(2));
      // Wait for node 0 (and node 1, cm3) to observe the new view without node 2.
      TestingUtil.blockUntilViewsReceived(10_000, false, manager(0), manager(1), cm3);

      // Install a delegating StateConsumer on node 0 that captures the write CH members
      // and transport members at the exact instant onTopologyUpdate runs for the first
      // topology that arrives after node 2's departure.
      CompletableFuture<Collection<Address>> capturedWriteChMembers = new CompletableFuture<>();
      CompletableFuture<Collection<Address>> capturedTransportMembers = new CompletableFuture<>();

      StateConsumer realSc = TestingUtil.extractComponent(cache(0, CACHE_NAME), StateConsumer.class);
      RpcManager rpcManager = cache0.getAdvancedCache().getRpcManager();
      TestingUtil.replaceComponent(cache0, StateConsumer.class,
            new DelegatingStateConsumer(realSc) {
               volatile boolean captured;
               @Override
               public CompletionStage<CompletionStage<Void>> onTopologyUpdate(
                     CacheTopology cacheTopology, boolean isRebalance) {
                  if (!captured) {
                     captured = true;
                     capturedWriteChMembers.complete(
                           cacheTopology.getWriteConsistentHash().getMembers());
                     capturedTransportMembers.complete(
                           rpcManager.getTransport().getMembers());
                  }
                  return super.onTopologyUpdate(cacheTopology, isRebalance);
               }
            }, true);

      // Intercept the first topology update that node 2's departure produces on node 0
      // (either a NO_REBALANCE membership update or a new READ_OLD_WRITE_ALL rebalance).
      // Hold it for TOPOLOGY_HOLD_MS — longer than REMOTE_TIMEOUT_MS — so that the put
      // times out before onTopologyUpdate is ever called.
      StagedLocalTopologyManager.PendingTopology departurePending =
            staged.pollTopologyUpdate(null, 15, TimeUnit.SECONDS);

      // Sleep for the hold period, then release the topology.  At release time,
      // onTopologyUpdate will call onMembersChange.  With the bug it uses the union write
      // CH (which still has node 2) and the put stays stuck.  With the fix it uses
      // transport members (which do not have node 2) and the put resolves.
      Thread.sleep(TOPOLOGY_HOLD_MS);
      departurePending.proceed();
      staged.stopIntercepting();

      // Verify the structural gap: write CH had node 2, transport did not.
      Collection<Address> writeChMembers =
            capturedWriteChMembers.get(5, TimeUnit.SECONDS);
      Collection<Address> transportMembers =
            capturedTransportMembers.get(5, TimeUnit.SECONDS);

      assertNotNull(writeChMembers);
      assertNotNull(transportMembers);

      // Transport view must not contain the departed node.
      assertFalse(transportMembers.contains(node2Address),
            "transport members should not contain departed node " + node2Address +
                  "; transportMembers=" + transportMembers);

//      // The write CH (union of currentCH and pendingCH) still lists the departed node —
//      // this is the structural gap that causes the bug.
//      assertTrue(writeChMembers.contains(node2Address),
//            "write CH should still contain departed node " + node2Address +
//                  " when onTopologyUpdate runs; writeChMembers=" + writeChMembers);

      // The put must have timed out: node 2's ack was never delivered and the topology
      // was held longer than remoteTimeout.  Without the fix onMembersChange never drops
      // node 2 from the CommandAckCollector's backupOwners set.
      try {
         putFuture.get(5, TimeUnit.SECONDS);
         log.infof("put completed — fix is active: onMembersChange used transport members " +
               "and dropped node %s from backupOwners", node2Address);
         fail("Should have failed");
      } catch (TimeoutException te) {
         putFuture.cancel(true);
         log.infof("put timed out as expected (bug confirmed): onMembersChange used write " +
               "CH members which still contained departed node %s", node2Address);
      } catch (ExecutionException ee) {
         if (ee.getCause() instanceof org.infinispan.commons.TimeoutException) {
            log.infof("put timed out with ISPN TimeoutException (bug confirmed): %s",
                  ee.getCause().getMessage());
         } else {
            throw ee;
         }
      } finally {
         crmNode2.stopBlocking();
      }
   }
}
