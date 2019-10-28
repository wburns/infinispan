package org.infinispan.reactive.publisher.impl;

import static org.infinispan.util.logging.Log.CLUSTER;

import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.PartitionStatusChanged;
import org.infinispan.notifications.cachelistener.event.PartitionStatusChangedEvent;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.reactivestreams.Publisher;

/**
 * Cluster stream manager that also pays attention to partition status and properly closes iterators and throws
 * exceptions when the availability mode changes.
 */
@Scope(Scopes.NAMED_CACHE)
public class PartitionAwareClusterPublisherManager<K, V> extends ClusterPublisherManagerImpl<K, V> {
   volatile AvailabilityMode currentMode = AvailabilityMode.AVAILABLE;

   protected final PartitionListener listener = new PartitionListener();
   @Inject protected Cache<?, ?> cache;

   @Listener
   private class PartitionListener {
      volatile AvailabilityMode currentMode = AvailabilityMode.AVAILABLE;

      @PartitionStatusChanged
      public void onPartitionChange(PartitionStatusChangedEvent<K, ?> event) {
         if (!event.isPre()) {
            currentMode = event.getAvailabilityMode();
         }
      }
   }

   public void start() {
      super.start();
      cache.addListener(listener);
   }

   @Override
   public <R> CompletionStage<R> keyReduction(boolean parallelPublisher, IntSet segments, Set<K> keysToInclude,
         InvocationContext ctx, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<K>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      checkPartitionStatus();
      return super.keyReduction(parallelPublisher, segments, keysToInclude, ctx, includeLoader, deliveryGuarantee, transformer, finalizer);
   }

   @Override
   public <R> CompletionStage<R> entryReduction(boolean parallelPublisher, IntSet segments, Set<K> keysToInclude,
         InvocationContext ctx, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      checkPartitionStatus();
      return super.entryReduction(parallelPublisher, segments, keysToInclude, ctx, includeLoader, deliveryGuarantee, transformer, finalizer);
   }

   @Override
   public <R> SegmentCompletionPublisher<R> keyPublisher(IntSet segments, Set<K> keysToInclude,
         InvocationContext invocationContext, boolean includeLoader, DeliveryGuarantee deliveryGuarantee, int batchSize,
         Function<? super Publisher<K>, ? extends Publisher<R>> transformer) {
      checkPartitionStatus();
      return super.keyPublisher(segments, keysToInclude, invocationContext, includeLoader, deliveryGuarantee, batchSize, transformer);
   }

   @Override
   public <R> SegmentCompletionPublisher<R> entryPublisher(IntSet segments, Set<K> keysToInclude,
         InvocationContext invocationContext, boolean includeLoader, DeliveryGuarantee deliveryGuarantee, int batchSize,
         Function<? super Publisher<CacheEntry<K, V>>, ? extends Publisher<R>> transformer) {
      checkPartitionStatus();
      return super.entryPublisher(segments, keysToInclude, invocationContext, includeLoader, deliveryGuarantee, batchSize, transformer);
   }

   private void checkPartitionStatus() {
      if (isPartitionDegraded()) {
         throw CLUSTER.partitionDegraded();
      }
   }

   private boolean isPartitionDegraded() {
      return currentMode != AvailabilityMode.AVAILABLE;
   }
}
