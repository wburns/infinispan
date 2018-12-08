package org.infinispan.reactive.publisher.impl;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.processors.FlowableProcessor;
import io.reactivex.processors.PublishProcessor;

/**
 * @author wburns
 * @since 10.0
 */
public class ClusterPublisherManagerImpl<K, V> implements ClusterPublisherManager<K, V> {
   protected final static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   protected final static boolean trace = log.isTraceEnabled();

   @Inject private LocalPublisherManager<K, V> localPublisherManager;
   @Inject private DistributionManager distributionManager;
   @Inject private StateTransferLock stateTransferLock;
   @Inject private RpcManager rpcManager;
   @Inject private CommandsFactory commandsFactory;

   // Make sure we don't create one per invocation
   private final KeyComposedType KEY_COMPOSED = new KeyComposedType<>();
   private <R> KeyComposedType<R> keyComposedType() {
      return KEY_COMPOSED;
   }
   // Make sure we don't create one per invocation
   private final EntryComposedType ENTRY_COMPOSED = new EntryComposedType<>();

   private <R> EntryComposedType<R> entryComposedType() {
      return ENTRY_COMPOSED;
   }

   private int maxSegment;

   @Start
   public void start() {
      maxSegment = distributionManager.getReadConsistentHash().getNumSegments();
   }

   @Override
   public <R> CompletionStage<R> keyComposition(boolean parallelStream, IntSet segments, Set<K> keysToInclude,
         Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<K>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      // Needs to be serialized processor as we can write to it from different threads
      FlowableProcessor<R> flowableProcessor = PublishProcessor.<R>create().toSerialized();
      // We apply the finalizer first to ensure they can subscribe to the PublishProcessor before we emit any items
      CompletionStage<R> stage = finalizer.apply(flowableProcessor);

      if (keysToInclude != null) {
         handleSpecificKeys(parallelStream, keysToInclude, keysToExclude, includeLoader, deliveryGuarantee,
               keyComposedType(), transformer, finalizer, flowableProcessor);
      } else {
         IntSet segmentsToComplete = concurrentIntSetFrom(segments, maxSegment);
         startRequestChain(parallelStream, segmentsToComplete, keysToExclude, includeLoader,
               deliveryGuarantee, keyComposedType(), transformer, finalizer, flowableProcessor);
      }
      return stage;
   }

   @Override
   public <R> CompletionStage<R> entryComposition(boolean parallelStream, IntSet segments, Set<K> keysToInclude,
         Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      // Needs to be serialized processor as we can write to it from different threads
      FlowableProcessor<R> flowableProcessor = PublishProcessor.<R>create().toSerialized();
      // We apply the finalizer first to ensure they can subscribe to the PublishProcessor before we emit any items
      CompletionStage<R> stage = finalizer.apply(flowableProcessor);

      if (keysToInclude != null) {
         handleSpecificKeys(parallelStream, keysToInclude, keysToExclude, includeLoader, deliveryGuarantee,
               entryComposedType(), transformer, finalizer, flowableProcessor);
      } else {
         IntSet segmentsToComplete = concurrentIntSetFrom(segments, maxSegment);
         startRequestChain(parallelStream, segmentsToComplete, keysToExclude, includeLoader,
               deliveryGuarantee, entryComposedType(), transformer, finalizer, flowableProcessor);
      }
      return stage;
   }

   private <I, R> void handleSpecificKeys(boolean parallelStream, Set<K> keysToInclude, Set<K> keysToExclude,
         boolean includeLoader, DeliveryGuarantee deliveryGuarantee, ComposedType<K, I, R> composedType,
         Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer,
         FlowableProcessor<R> flowableProcessor) {
      LocalizedCacheTopology topology = distributionManager.getCacheTopology();
      Address localAddress = topology.getLocalAddress();
      Map<Address, Set<K>> keyTargets = keyTargets(topology, keysToInclude, localAddress, keysToExclude);

      AtomicInteger parallelCount = new AtomicInteger(keyTargets.size());

      // This way we only have to allocate 1 per request chain
      BiConsumer<PublisherResultCollector<R>, Throwable> consumer = new KeySpecificConsumer<>(flowableProcessor, parallelCount);

      Set<K> localKeys = keyTargets.remove(localAddress);
      // If any targets left, they are all remote
      if (!keyTargets.isEmpty()) {
         // We submit the remote ones first as they will not block at all, just to send remote tasks
         for (Map.Entry<Address, Set<K>> remoteTarget : keyTargets.entrySet()) {
            Set<K> remoteKeys = remoteTarget.getValue();
            PublisherRequestCommand<K> command = composedType.remoteInvocation(parallelStream, null, remoteKeys,
                  keysToExclude, includeLoader, deliveryGuarantee, transformer, finalizer);
            command.setTopologyId(topology.getTopologyId());
            Address remoteAddress = remoteTarget.getKey();
            CompletionStage<PublisherResultCollector<R>> stage = rpcManager.invokeCommand(remoteAddress, command,
                  new PublisherResultCollector<>(null), rpcManager.getSyncRpcOptions());
            stage.whenComplete(consumer);
         }
      }

      if (localKeys != null) {
         CompletionStage<PublisherResult<R>> localStage = composedType.localInvocation(parallelStream, null,
               localKeys, keysToExclude, includeLoader, deliveryGuarantee, transformer, finalizer);

         // Map to the same collector, so we can reuse the same BiConsumer
         localStage.thenApply(result -> {
            PublisherResultCollector<R> collector = new PublisherResultCollector<>(null);
            collector.address = localAddress;
            collector.results = result;
            return collector;
         }).whenComplete(consumer);
      }
   }

   private IntSet concurrentIntSetFrom(IntSet segments, int maxSegment) {
      if (segments == null) {
         IntSet allSegments = IntSets.concurrentSet(maxSegment);
         for (int i = 0; i < maxSegment; ++i) {
            allSegments.set(i);
         }
         return allSegments;
      } else {
         return IntSets.concurrentCopyFrom(segments, maxSegment);
      }
   }

   private <I, R> void startRequestChain(boolean parallelStream, IntSet segments,
         Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee, ComposedType<K, I, R> composedType,
         Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer,
         FlowableProcessor<R> flowableProcessor) {
      LocalizedCacheTopology topology = distributionManager.getCacheTopology();
      Address localAddress = topology.getLocalAddress();
      Map<Address, IntSet> targets = determineTargets(topology, segments, localAddress);

      // used to determine that last parallel completion, to either complete or retry
      AtomicInteger parallelCount = new AtomicInteger(targets.size());

      IntSet localSegments = targets.remove(localAddress);

      // This way we only have to allocate 1 per request chain
      BiConsumer<PublisherResultCollector<R>, Throwable> consumer = new SegmentSpecificConsumer<>(flowableProcessor,
            parallelCount, topology, parallelStream, segments, keysToExclude, includeLoader, deliveryGuarantee,
            composedType, transformer, finalizer);

      // If any targets left, they are all remote
      if (!targets.isEmpty()) {
         // We submit the remote ones first as they will not block at all, just to send remote tasks
         for (Map.Entry<Address, IntSet> remoteTarget : targets.entrySet()) {
            IntSet remoteSegments = remoteTarget.getValue();
            PublisherRequestCommand<K> command = composedType.remoteInvocation(parallelStream, remoteSegments, null,
                  keysToExclude, includeLoader, deliveryGuarantee, transformer, finalizer);
            command.setTopologyId(topology.getTopologyId());
            Address remoteAddress = remoteTarget.getKey();
            CompletionStage<PublisherResultCollector<R>> stage = rpcManager.invokeCommand(remoteAddress, command,
                  new PublisherResultCollector<>(remoteSegments), rpcManager.getSyncRpcOptions());
            stage.whenComplete(consumer);
         }
      }

      if (localSegments != null) {
         CompletionStage<PublisherResult<R>> localStage = composedType.localInvocation(parallelStream, localSegments,
               null, keysToExclude, includeLoader, deliveryGuarantee, transformer, finalizer);

         // Map to the same collector, so we can reuse the same BiConsumer
         localStage.thenApply(result -> {
            PublisherResultCollector<R> collector = new PublisherResultCollector<>(localSegments);
            collector.address = localAddress;
            collector.results = result;
            return collector;
         }).whenComplete(consumer);
      }
   }

   class SegmentSpecificConsumer<I, R> extends KeySpecificConsumer<R> {
      private final LocalizedCacheTopology topology;
      private final boolean parallelStream;
      private final IntSet segments;
      private final Set<K> keysToExclude;
      private final boolean includeLoader;
      private final DeliveryGuarantee deliveryGuarantee;
      private final ComposedType<K, I, R> composedType;
      private final Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer;
      private final Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer;

      SegmentSpecificConsumer(FlowableProcessor<R> flowableProcessor, AtomicInteger parallelCount,
            LocalizedCacheTopology topology, boolean parallelStream, IntSet segments, Set<K> keysToExclude,
            boolean includeLoader, DeliveryGuarantee deliveryGuarantee, ComposedType<K, I, R> composedType,
            Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
            Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
         super(flowableProcessor, parallelCount);
         this.topology = topology;
         this.parallelStream = parallelStream;
         this.segments = segments;
         this.keysToExclude = keysToExclude;
         this.includeLoader = includeLoader;
         this.deliveryGuarantee = deliveryGuarantee;
         this.composedType = composedType;
         this.transformer = transformer;
         this.finalizer = finalizer;
      }

      @Override
      protected void handleResult(PublisherResultCollector<R> resultCollector) {
         PublisherResult<R> result = resultCollector.results;
         IntSet suspectedSegments = result.getSuspectedSegments();
         if (suspectedSegments != null && !suspectedSegments.isEmpty()) {
            for (PrimitiveIterator.OfInt iter = resultCollector.targetSegments.iterator(); iter.hasNext(); ) {
               int segment = iter.nextInt();
               if (!suspectedSegments.contains(segment)) {
                  segments.remove(segment);
               }
            }
         } else {
            segments.removeAll(resultCollector.targetSegments);
         }

         super.handleResult(resultCollector);
      }

      @Override
      protected void onCompletion() {
         if (segments.isEmpty()) {
            super.onCompletion();
         } else {
            int nextTopology = topology.getTopologyId() + 1;
            if (trace) {
               log.tracef("Retrying segments %s after %d is installed", segments, nextTopology);
            }
            // If we had an issue with segments, we need to wait until the next topology is installed to try again
            stateTransferLock.topologyFuture(topology.getTopologyId() + 1).whenComplete((ign, innerT) -> {
               if (innerT != null) {
                  if (trace) {
                     log.tracef(innerT, "General error encountered when waiting on topology future for publisher request command");
                  }
                  flowableProcessor.onError(innerT);
               } else {
                  // Restart with next set of segments
                  startRequestChain(parallelStream, segments, keysToExclude, includeLoader, deliveryGuarantee,
                        composedType, transformer, finalizer, flowableProcessor);
               }
            });
         }
      }
   }

   class KeySpecificConsumer<R> implements BiConsumer<PublisherResultCollector<R>, Throwable> {
      protected final FlowableProcessor<R> flowableProcessor;
      protected final AtomicInteger parallelCount;

      KeySpecificConsumer(FlowableProcessor<R> flowableProcessor, AtomicInteger parallelCount) {
         this.flowableProcessor = flowableProcessor;
         this.parallelCount = parallelCount;
      }

      @Override
      public void accept(PublisherResultCollector<R> resultCollector, Throwable t) {
         if (t != null) {
            if (trace) {
               log.tracef(t, "General error encountered when executing publisher request command");
            }
            flowableProcessor.onError(t);
         } else {
            handleResult(resultCollector);

            // We were the last one to complete if zero, so we have to either complete or retry remaining segments again
            if (parallelCount.decrementAndGet() == 0) {
               onCompletion();
            }
         }
      }

      protected void handleResult(PublisherResultCollector<R> resultCollector) {
         PublisherResult<R> result = resultCollector.results;
         R actualValue = result.getResult();
         if (actualValue != null) {
            if (trace) {
               log.tracef("Result result was: %s for segments %s from %s", result, resultCollector.targetSegments, resultCollector.address);
            }
            flowableProcessor.onNext(actualValue);
         } else if (trace) {
            log.tracef("Result contained no results, just suspected segments %s from %s", resultCollector.targetSegments, resultCollector.address);
         }
      }

      protected void onCompletion() {
         flowableProcessor.onComplete();
      }
   }

   class PublisherResultCollector<R> implements ResponseCollector<PublisherResultCollector<R>> {
      private final IntSet targetSegments;
      private Address address;
      private PublisherResult<R> results;

      PublisherResultCollector(IntSet targetSegments) {
         this.targetSegments = targetSegments;
      }

      @Override
      public PublisherResultCollector<R> addResponse(Address sender, Response response) {
         address = sender;
         if (response instanceof ValidResponse) {
            // We should only get successful response if it is valid
            results = (PublisherResult<R>) ((ValidResponse) response).getResponseValue();
         } else if (response instanceof ExceptionResponse) {
            handleException(((ExceptionResponse) response).getException());
         } else if (response instanceof CacheNotFoundResponse) {
            handleSuspect();
         } else {
            handleException(new RpcException("Unknown response type: " + response));
         }
         return this;
      }

      void handleException(Throwable t) {
         if (!(t instanceof SuspectException)) {
            if (trace) {
               log.tracef(t, "Exception encountered while requesting segments %s from %s", targetSegments, address);
            }
            // Throw the exception so it is propagated to caller
            if (t instanceof CacheException) {
               throw (CacheException) t;
            }
            throw new CacheException(t);
         }
         handleSuspect();
      }

      void handleSuspect() {
         if (trace) {
            log.tracef("Cache is no longer running for segments %s from %s - must retry", targetSegments, address);
         }
         results = new SimplePublisherResult<>(targetSegments, null);
      }

      @Override
      public PublisherResultCollector<R> finish() {
         throw new IllegalStateException("Should never be invoked!");
      }
   }

   private Map<Address, IntSet> determineTargets(LocalizedCacheTopology topology, IntSet segments, Address localAddress) {
      Map<Address, IntSet> targets = new HashMap<>();
      for (PrimitiveIterator.OfInt iter = segments.iterator(); iter.hasNext(); ) {
         int segment = iter.nextInt();
         Address owner;
         // Prioritize local node even if it is backup
         if (topology.isSegmentReadOwner(segment)) {
            owner = localAddress;
         } else {
            owner = topology.getSegmentDistribution(segment).primary();
         }
         addToMap(targets, owner, segment);
      }
      return targets;
   }

   private void addToMap(Map<Address, IntSet> map, Address owner, int segment) {
      IntSet set = map.get(owner);
      if (set == null) {
         set = IntSets.mutableEmptySet();
         map.put(owner, set);
      }
      set.set(segment);
   }

   private Map<Address, Set<K>> keyTargets(LocalizedCacheTopology topology, Set<K> keys, Address localAddress,
         Set<K> keysToExclude) {
      Map<Address, Set<K>> filteredKeys = null;
      if (keys != null) {
         filteredKeys = new HashMap<>();
         for (K key : keys) {
            if (keysToExclude != null && keysToExclude.contains(key)) {
               continue;
            }
            DistributionInfo distributionInfo = topology.getDistribution(key);
            Address targetAddress;
            if (distributionInfo.isPrimary()) {
               targetAddress = localAddress;
            } else {
               targetAddress = distributionInfo.primary();
            }
            addToMap(filteredKeys, targetAddress, key);
         }
      }
      return filteredKeys;
   }

   private void addToMap(Map<Address, Set<K>> map, Address owner, K key) {
      Set<K> set = map.get(owner);
      if (set == null) {
         set = new HashSet<>();
         map.put(owner, set);
      }
      set.add(key);
   }

   interface ComposedType<K, I, R> {
      CompletionStage<PublisherResult<R>> localInvocation(boolean parallelStream, IntSet segments, Set<K> keysToInclude,
            Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
            Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
            Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer);

      PublisherRequestCommand<K> remoteInvocation(boolean parallelStream, IntSet segments, Set<K> keysToInclude,
            Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
            Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
            Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer);
   }

   private class KeyComposedType<R> implements ComposedType<K, K, R> {

      @Override
      public CompletionStage<PublisherResult<R>> localInvocation(boolean parallelStream, IntSet segments, Set<K> keysToInclude,
            Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
            Function<? super Publisher<K>, ? extends CompletionStage<R>> transformer,
            Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
         return localPublisherManager.keyPublisherOperation(parallelStream, segments, keysToInclude, keysToExclude,
               includeLoader, deliveryGuarantee, transformer, finalizer);
      }

      @Override
      public PublisherRequestCommand<K> remoteInvocation(boolean parallelStream, IntSet segments, Set<K> keysToInclude,
            Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
            Function<? super Publisher<K>, ? extends CompletionStage<R>> transformer,
            Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
         return commandsFactory.buildKeyPublisherCommand(parallelStream, deliveryGuarantee, segments, keysToExclude,
               keysToExclude, includeLoader, transformer, finalizer);
      }
   }

   private class EntryComposedType<R> implements ComposedType<K, CacheEntry<K, V>, R> {

      @Override
      public CompletionStage<PublisherResult<R>> localInvocation(boolean parallelStream, IntSet segments, Set<K> keysToInclude,
            Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
            Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer,
            Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
         return localPublisherManager.entryPublisherOperation(parallelStream, segments, keysToInclude, keysToExclude,
               includeLoader, deliveryGuarantee, transformer, finalizer);
      }

      @Override
      public PublisherRequestCommand<K> remoteInvocation(boolean parallelStream, IntSet segments, Set<K> keysToInclude,
            Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
            Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer,
            Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
         return commandsFactory.buildEntryPublisherCommand(parallelStream, deliveryGuarantee, segments, keysToInclude,
               keysToExclude, includeLoader, transformer, finalizer);
      }
   }
}
