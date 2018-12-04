package org.infinispan.reactive.publisher.impl;

import static org.infinispan.factories.KnownComponentNames.ASYNC_OPERATIONS_EXECUTOR;

import java.lang.invoke.MethodHandles;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.IntConsumer;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.NullCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.stream.StreamMarshalling;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.rxjava.FlowableFromIntSetFunction;
import org.infinispan.util.rxjava.RxJavaInterop;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;

/**
 * @author wburns
 * @since 10.0
 */
public class LocalPublisherManagerImpl<K, V> implements LocalPublisherManager<K, V> {
   private final static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final boolean trace = log.isTraceEnabled();

   @Inject private ComponentRef<Cache<K, V>> cacheComponentRef;
   // This cache should only be used for retrieving entries via Cache#get
   private AdvancedCache<K, V> remoteCache;
   // This cache should be used for iteration purposes or Cache#get that are local only
   private AdvancedCache<K, V> cache;
   private Scheduler asyncScheduler;
   private int maxSegment;
   private boolean hasLoader;

   private final Set<SegmentListener> changeListener = ConcurrentHashMap.newKeySet();

   /**
    * Injects the cache - unfortunately this cannot be in start. Tests will rewire certain components which will in
    * turn reinject the cache, but they won't call the start method! If the latter is fixed we can add this to start
    * method and add @Inject to the variable.
    */
   @Inject
   public void inject(@ComponentName(ASYNC_OPERATIONS_EXECUTOR) ExecutorService asyncOperationsExecutor) {
      this.asyncScheduler = Schedulers.from(asyncOperationsExecutor);
   }

   @Start
   public void start() {
      // We need to unwrap the cache as a local stream should only deal with BOXED values
      // Any mappings will be provided by the originator node in their intermediate operation stack in the operation itself.
      this.remoteCache = AbstractDelegatingCache.unwrapCache(cacheComponentRef.running()).getAdvancedCache();
      // The iteration caches should only deal with local entries.
      // Also the iterations here are always remote initiated
      this.cache = remoteCache.withFlags(Flag.CACHE_MODE_LOCAL, Flag.REMOTE_ITERATION);
      hasLoader = cache.getCacheConfiguration().persistence().usingStores();
      ClusteringConfiguration clusteringConfiguration = cache.getCacheConfiguration().clustering();
      this.maxSegment = clusteringConfiguration.hash().numSegments();
   }

   @Override
   public <R> CompletionStage<PublisherResult<R>> keyPublisherOperation(boolean parallelStream, IntSet segments,
         Set<K> keysToInclude, Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<K>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      if (keysToInclude != null) {
         return handleSpecificKeys(parallelStream, keysToInclude, keysToExclude, deliveryGuarantee, transformer, finalizer);
      }

      AdvancedCache<K, V> cache = getCacheWithFlags(includeLoader);

      Function<K, K> toKeyFunction = Function.identity();
      switch (deliveryGuarantee) {
         case AT_MOST_ONCE:
            CompletionStage<R> stage = atMostOnce(parallelStream, cache.keySet(), keysToExclude, toKeyFunction,
                  segments, transformer, finalizer);
            return stage.thenApply(ignoreSegmentsFunction());
         case AT_LEAST_ONCE:
            return atLeastOnce(parallelStream, cache.keySet(), keysToExclude, toKeyFunction, segments, transformer, finalizer);
         case EXACTLY_ONCE:
            return exactlyOnce(parallelStream, cache.keySet(), keysToExclude, toKeyFunction, segments, transformer, finalizer);
         default:
            throw new UnsupportedOperationException("Unsupported delivery guarantee: " + deliveryGuarantee);
      }
   }

   @Override
   public <R> CompletionStage<PublisherResult<R>> entryPublisherOperation(boolean parallelStream, IntSet segments,
         Set<K> keysToInclude, Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      if (keysToInclude != null) {
         return handleSpecificEntries(parallelStream, keysToInclude, keysToExclude, deliveryGuarantee, transformer, finalizer);
      }

      AdvancedCache<K, V> cache = getCacheWithFlags(includeLoader);

      // We have to cast to Function, since we can't cast our inner generic
      Function<CacheEntry<K, V>, K> toKeyFunction = (Function) StreamMarshalling.entryToKeyFunction();
      switch (deliveryGuarantee) {
         case AT_MOST_ONCE:
            CompletionStage<R> stage = atMostOnce(parallelStream, cache.cacheEntrySet(), keysToExclude, toKeyFunction,
                  segments, transformer, finalizer);
            return stage.thenApply(ignoreSegmentsFunction());
         case AT_LEAST_ONCE:
            return atLeastOnce(parallelStream, cache.cacheEntrySet(), keysToExclude, toKeyFunction, segments, transformer, finalizer);
         case EXACTLY_ONCE:
            return exactlyOnce(parallelStream, cache.cacheEntrySet(), keysToExclude, toKeyFunction, segments, transformer, finalizer);
         default:
            throw new UnsupportedOperationException("Unsupported delivery guarantee: " + deliveryGuarantee);
      }
   }

   @Override
   public void segmentsLost(IntSet lostSegments) {
      if (trace) {
         log.tracef("Notifying listeners of lost segments %s", lostSegments);
      }
      changeListener.forEach(lostSegments::forEach);
   }

   private static Function<Object, PublisherResult<Object>> ignoreSegmentsFunction  = value ->
         new SimplePublisherResult<>(IntSets.immutableEmptySet(), value);

   private static <R> Function<R, PublisherResult<R>> ignoreSegmentsFunction() {
      return (Function) ignoreSegmentsFunction;
   }

   private <I, R> CompletionStage<PublisherResult<R>> exactlyOnce(boolean parallelStream, CacheSet<I> set,
         Set<K> keysToExclude, Function<I, K> toKeyFunction, IntSet segments,
         Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      // This has to be concurrent to allow for different threads to update it (ie. parallel) or even ensure
      // that a state transfer segment lost can see completed
      IntSet concurrentSegments = IntSets.concurrentCopyFrom(segments, maxSegment);
      SegmentListener listener = new SegmentListener(concurrentSegments);
      changeListener.add(listener);

      Flowable<CompletionStage<R>> stageFlowable = new FlowableFromIntSetFunction<>(segments, segment -> {
         // This means the segment was lost before we even tried to process it - so just skip it
         if (listener.segmentsLost.contains(segment)) {
            return CompletableFutures.completedNull();
         }
         Flowable<I> innerFlowable = Flowable.fromPublisher(set.localPublisher(segment))
               // If we complete the iteration try to remove the segment - so it can't be suspected
               .doOnComplete(() -> concurrentSegments.remove(segment));
         if (parallelStream) {
            innerFlowable = innerFlowable.subscribeOn(asyncScheduler);
         }

         if (keysToExclude != null) {
            innerFlowable = innerFlowable.filter(i -> !keysToExclude.contains(toKeyFunction.apply(i)));
         }

         return transformer.apply(innerFlowable).thenCompose(value -> {
            // This means the segment was lost in the middle of processing
            if (listener.segmentsLost.contains(segment)) {
               return CompletableFutures.<R>completedNull();
            }
            return CompletableFuture.completedFuture(value);
         });
      });
      CompletionStage<R> combinedStage = combineStages(stageFlowable, finalizer);
      return handleLostSegments(combinedStage, listener);
   }

   private <R> CompletionStage<PublisherResult<R>> handleSpecificKeys(boolean parallelStream, Set<K> keysToInclude,
         Set<K> keysToExclude, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<K>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      AdvancedCache<K, V> cache = deliveryGuarantee == DeliveryGuarantee.AT_MOST_ONCE ? this.cache : remoteCache;
      return handleSpecificObjects(parallelStream, keysToInclude, keysToExclude, keyFlowable ->
            // Filter out all the keys that aren't in the cache
            keyFlowable.filter(cache::containsKey)
      , transformer, finalizer);
   }

   private <R> CompletionStage<PublisherResult<R>> handleSpecificEntries(boolean parallelStream, Set<K> keysToInclude,
         Set<K> keysToExclude, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      AdvancedCache<K, V> cache = deliveryGuarantee == DeliveryGuarantee.AT_MOST_ONCE ? this.cache : remoteCache;
      return handleSpecificObjects(parallelStream, keysToInclude, keysToExclude, keyFlowable ->
         keyFlowable.map(k -> {
            CacheEntry<K, V> entry = cache.getCacheEntry(k);
            if (entry == null) {
               return NullCacheEntry.<K, V>getInstance();
            }
            return entry;
         }).filter(e -> e != NullCacheEntry.getInstance())
      , transformer, finalizer);
   }

   private <I, R> CompletionStage<PublisherResult<R>> handleSpecificObjects(boolean parallelStream, Set<K> keysToInclude,
         Set<K> keysToExclude, Function<? super Flowable<K>, ? extends Flowable<I>> keyTransformer,
         Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      Flowable<K> keyFlowable = Flowable.fromIterable(keysToInclude);
      if (keysToExclude != null) {
         keyFlowable = keyFlowable.filter(k -> !keysToExclude.contains(k));
      }
      if (parallelStream) {
         // We send 16 keys to each rail to be parallelized
         Flowable<R> stageFlowable = keyFlowable.window(16)
               .flatMap(keys -> {
                  CompletionStage<R> stage = keys.subscribeOn(asyncScheduler)
                        .to(keyTransformer::apply)
                        .to(transformer::apply);
                  return RxJavaInterop.<R>completionStageToPublisher().apply(stage);
               });
         return finalizer.apply(stageFlowable).thenApply(ignoreSegmentsFunction());
      } else {
         return keyFlowable.to(keyTransformer::apply)
               .to(transformer::apply)
               .thenApply(ignoreSegmentsFunction());
      }
   }

   private <I, R> CompletionStage<R> parallelAtMostOnce(CacheSet<I> cacheSet, Set<K> keysToExclude,
         Function<I, K> toKeyFunction, IntSet segments,
         Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      Flowable<CompletionStage<R>> flowable = new FlowableFromIntSetFunction<>(segments, segment -> {
         Flowable<I> innerFlowable = Flowable.fromPublisher(cacheSet.localPublisher(segment))
               .subscribeOn(asyncScheduler);
         if (keysToExclude != null) {
            innerFlowable = innerFlowable.filter(i -> !keysToExclude.contains(toKeyFunction.apply(i)));
         }
         return transformer.apply(innerFlowable);
      });
      return combineStages(flowable, finalizer);
   }

   private <I, R> CompletionStage<R> atMostOnce(boolean parallel, CacheSet<I> set, Set<K> keysToExclude,
         Function<I, K> toKeyFunction, IntSet segments,
         Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      if (parallel) {
         return parallelAtMostOnce(set, keysToExclude, toKeyFunction, segments, transformer, finalizer);
      } else {
         Flowable<I> flowable = Flowable.fromPublisher(set.localPublisher(segments));
         if (keysToExclude != null) {
            flowable = flowable.filter(i -> !keysToExclude.contains(toKeyFunction.apply(i)));
         }
         return transformer.apply(flowable);
      }
   }

   private <I, R> CompletionStage<PublisherResult<R>> atLeastOnce(boolean parallel, CacheSet<I> cacheSet,
         Set<K> keysToExclude, Function<I, K> toKeyFunction, IntSet segments,
         Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      SegmentListener listener = new SegmentListener(segments);
      changeListener.add(listener);
      CompletionStage<R> stage = atMostOnce(parallel, cacheSet, keysToExclude, toKeyFunction, segments, transformer, finalizer);
      return handleLostSegments(stage, listener);
   }

   private <R> CompletionStage<PublisherResult<R>> handleLostSegments(CompletionStage<R> stage, SegmentListener segmentListener) {
      return stage.thenApply(value -> {
         IntSet lostSegments = segmentListener.segmentsLost;
         if (lostSegments.isEmpty()) {
            return LocalPublisherManagerImpl.<R>ignoreSegmentsFunction().apply(value);
         } else {
            return new SimplePublisherResult<>(lostSegments, value);
         }
      }).whenComplete((u, t) -> changeListener.remove(segmentListener));
   }

   private static <R> CompletionStage<R> combineStages(Flowable<CompletionStage<R>> stagePublisher,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      Publisher<R> resultPublisher = stagePublisher.flatMap(stage -> {
         if (stage == CompletableFutures.completedNull()) {
            return Flowable.empty();
         }
         // TODO: do optimization check if stage is already complete - which is guaranteed without a loader
         return RxJavaInterop.<R>completionStageToPublisher().apply(stage);
      });
      return finalizer.apply(resultPublisher);

      // TODO: this code could be more performant if we have a large amount of segments?
      // Doesn't create AsyncProcessor and BiConsumer for each segment
//      UnicastProcessor<R> asyncProcessor = UnicastProcessor.create(estimatedSize);
//      // The total amount and completed amount must be written to in opposite order in the methods to ensure
//      // updates are seen properly between them
//      AtomicLong totalAmount = new AtomicLong();
//      AtomicLong completedCount = new AtomicLong();
//
//      // We create this outside the scope, so we don't allocate one per stage
//      BiConsumer<? super R, ? super Throwable> biConsumer = (value, t) -> {
//         if (t != null) {
//            asyncProcessor.onError(t);
//         } else {
//            asyncProcessor.onNext(value);
//            long completed = completedCount.incrementAndGet();
//            if (completed == totalAmount.get()) {
//               asyncProcessor.onComplete();
//            }
//         }
//      };
//
//      stagePublisher.doOnNext(stage -> stage.whenComplete(biConsumer))
//            .count()
//            .subscribe(count -> {
//               totalAmount.addAndGet(count);
//               if (completedCount.get() == count) {
//                  asyncProcessor.onComplete();
//               }
//            });
//      return finalizer.apply(asyncProcessor);
   }

   private AdvancedCache<K, V> getCacheWithFlags(boolean includeLoader) {
      if (hasLoader && !includeLoader) {
         return cache.withFlags(Flag.SKIP_CACHE_LOAD);
      } else {
         return cache;
      }
   }

   private class SegmentListener implements IntConsumer {
      // This variable should never be modified
      private final IntSet segments;
      private final IntSet segmentsLost;

      SegmentListener(IntSet segments) {
         this.segments = segments;
         // This is a concurrent set for visibility and technically because state transfer could call this concurrently
         this.segmentsLost = IntSets.concurrentSet(maxSegment);
      }

      @Override
      public void accept(int segment) {
         if (segments.contains(segment)) {
            if (trace) {
               log.tracef("Listener %s lost segment %d", this, segment);
            }
            segmentsLost.set(segment);
         }
      }
   }
}
