package org.infinispan.util;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.TimeoutException;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A controlled replacement for {@link LocalTopologyManager} that intercepts topology updates
 * and rebalance starts using {@link CompletableFuture}-based gates.
 *
 * <p>Unlike {@link BlockingLocalTopologyManager} — which parks a test thread in a
 * {@link BlockingQueue} — this utility returns a pending {@link CompletionStage} from the
 * intercept hook and exposes the gate as a {@link PendingTopology} that the test can
 * inspect and advance without blocking the calling thread.
 *
 * <p>Usage:
 * <pre>{@code
 * StagedLocalTopologyManager staged =
 *       StagedLocalTopologyManager.replaceTopologyManager(cacheManagers.get(0), CACHE_NAME);
 *
 * // ... trigger a membership change ...
 *
 * // poll for the intercepted update (no thread parked)
 * StagedLocalTopologyManager.PendingTopology pending =
 *       staged.pollTopologyUpdate(CacheTopology.Phase.READ_OLD_WRITE_ALL, 10, TimeUnit.SECONDS);
 *
 * // inspect, then let it proceed
 * assertFalse(pending.topology().getCurrentCH().getMembers().contains(departedAddress));
 * pending.proceed();
 *
 * staged.stopIntercepting();
 * }</pre>
 */
public class StagedLocalTopologyManager extends AbstractControlledLocalTopologyManager {

   private static final Log log = LogFactory.getLog(StagedLocalTopologyManager.class);
   private static final int POLL_TIMEOUT_SECONDS = 10;

   private final Address address;
   private final String expectedCacheName;
   /** Queue of topology events waiting to be processed by the test. */
   private final BlockingQueue<PendingTopology> pendingTopologies = new ArrayBlockingQueue<>(64);
   private volatile boolean intercepting = true;

   // -------------------------------------------------------------------------
   // Factory / replacement
   // -------------------------------------------------------------------------

   private StagedLocalTopologyManager(LocalTopologyManager delegate, Address address, String cacheName) {
      super(delegate);
      this.address = address;
      this.expectedCacheName = cacheName;
   }

   /**
    * Replaces the {@link LocalTopologyManager} on the given cache manager and returns the
    * controlling wrapper.  The wrapper intercepts topology updates for {@code cacheName} only.
    */
   public static StagedLocalTopologyManager replaceTopologyManager(EmbeddedCacheManager cm, String cacheName) {
      LocalTopologyManager real = TestingUtil.extractGlobalComponent(cm, LocalTopologyManager.class);
      StagedLocalTopologyManager staged =
            new StagedLocalTopologyManager(real, cm.getAddress(), cacheName);
      TestingUtil.replaceComponent(cm, LocalTopologyManager.class, staged, true);
      return staged;
   }

   // -------------------------------------------------------------------------
   // Intercept hooks
   // -------------------------------------------------------------------------

   @Override
   protected CompletionStage<Void> beforeHandleTopologyUpdate(
         String cacheName, CacheTopology cacheTopology, int viewId) {
      if (!intercepting || !expectedCacheName.equals(cacheName))
         return CompletableFutures.completedNull();
      return enqueue(cacheTopology, viewId, Type.CH_UPDATE);
   }

   @Override
   protected CompletionStage<Void> beforeHandleRebalance(
         String cacheName, CacheTopology cacheTopology, int viewId) {
      if (!intercepting || !expectedCacheName.equals(cacheName))
         return CompletableFutures.completedNull();
      return enqueue(cacheTopology, viewId, Type.REBALANCE_START);
   }

   @Override
   protected CompletionStage<Void> beforeConfirmRebalancePhase(
         String cacheName, int topologyId, Throwable throwable) {
      if (!intercepting || !expectedCacheName.equals(cacheName))
         return CompletableFutures.completedNull();
      return enqueue(null, topologyId, Type.CONFIRMATION);
   }

   private CompletionStage<Void> enqueue(CacheTopology topology, int topologyId, Type type) {
      CompletableFuture<Void> gate = new CompletableFuture<>();
      PendingTopology pending = new PendingTopology(topology, topologyId, type, gate);
      pendingTopologies.add(pending);
      log.debugf("[StagedLTM on %s] Intercepted %s topologyId=%d for cache %s",
            address, type, topologyId, expectedCacheName);
      return gate;
   }

   // -------------------------------------------------------------------------
   // Test-facing API
   // -------------------------------------------------------------------------

   /**
    * Polls for the next intercepted topology update of the given phase.  Does not block the
    * calling thread beyond the specified timeout.
    *
    * @throws TimeoutException if no matching update arrives within the timeout.
    */
   public PendingTopology pollTopologyUpdate(CacheTopology.Phase phase, long timeout, TimeUnit unit)
         throws InterruptedException {
      long deadline = System.nanoTime() + unit.toNanos(timeout);
      while (true) {
         long remaining = deadline - System.nanoTime();
         if (remaining <= 0)
            throw new TimeoutException("Timed out waiting for topology update with phase " + phase + " on " + address);
         PendingTopology pt = pendingTopologies.poll(Math.min(remaining, TimeUnit.SECONDS.toNanos(1)), TimeUnit.NANOSECONDS);
         if (pt == null)
            continue;
         if (pt.type() == Type.CONFIRMATION) {
            // Auto-proceed confirmations unless the caller is waiting for one explicitly.
            log.debugf("[StagedLTM on %s] Auto-proceeding confirmation topologyId=%d", address, pt.topologyId());
            pt.proceed();
            continue;
         }
         if (phase == null || pt.topology().getPhase() == phase)
            return pt;
         // Wrong phase — proceed it automatically and keep looking.
         log.debugf("[StagedLTM on %s] Skipping %s (phase=%s, wanted=%s)",
               address, pt.type(), pt.topology().getPhase(), phase);
         pt.proceed();
      }
   }

   /**
    * Polls for the next intercepted topology update of any phase.
    *
    * @throws TimeoutException if nothing arrives within the default timeout.
    */
   public PendingTopology pollTopologyUpdate() throws InterruptedException {
      return pollTopologyUpdate(null, POLL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
   }

   /**
    * Stops intercepting.  Any topology updates that arrive after this call pass through
    * immediately.  Drains and proceeds any already-queued but unprocessed updates.
    */
   public void stopIntercepting() {
      intercepting = false;
      PendingTopology leftover;
      while ((leftover = pendingTopologies.poll()) != null) {
         log.warnf("[StagedLTM on %s] Proceeding leftover %s at stopIntercepting", address, leftover.type());
         leftover.proceed();
      }
   }

   // -------------------------------------------------------------------------
   // Types
   // -------------------------------------------------------------------------

   public enum Type { CH_UPDATE, REBALANCE_START, CONFIRMATION }

   /**
    * A topology update that has been intercepted and is waiting for the test to call
    * {@link #proceed()} before it is delivered to the real {@link LocalTopologyManager}.
    */
   public static final class PendingTopology {
      private final CacheTopology topology;
      private final int topologyId;
      private final Type type;
      private final CompletableFuture<Void> gate;

      PendingTopology(CacheTopology topology, int topologyId, Type type, CompletableFuture<Void> gate) {
         this.topology = topology;
         this.topologyId = topologyId != -1 ? topologyId : (topology != null ? topology.getTopologyId() : -1);
         this.type = type;
         this.gate = gate;
      }

      /** The topology being delivered (may be {@code null} for confirmations). */
      public CacheTopology topology() {
         return topology;
      }

      public int topologyId() {
         return topologyId;
      }

      public Type type() {
         return type;
      }

      /** Allows the topology update to proceed to the real {@link LocalTopologyManager}. */
      public void proceed() {
         gate.complete(null);
      }

      /** Fails the topology update with the given exception. */
      public void fail(Throwable t) {
         gate.completeExceptionally(t);
      }

      @Override
      public String toString() {
         return "PendingTopology{type=" + type + ", topologyId=" + topologyId +
               ", phase=" + (topology != null ? topology.getPhase() : "N/A") + "}";
      }
   }
}
