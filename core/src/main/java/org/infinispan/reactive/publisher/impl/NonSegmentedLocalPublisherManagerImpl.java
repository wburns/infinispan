package org.infinispan.reactive.publisher.impl;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.CacheSet;
import org.infinispan.commons.util.IntSet;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.concurrent.CompletableFutures;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;

/**
 * LocalPublisherManager that handles cases when a non segmented store are present. In this case we optimize
 * the retrieval to not use individual segments as this would cause multiple full scans of the underlying
 * store. In this case we submit a task for all segments requested and process the results concurrently if
 * requested.
 * @author wburns
 * @since 10.0
 */
@Scope(Scopes.NAMED_CACHE)
public class NonSegmentedLocalPublisherManagerImpl<K, V> extends LocalPublisherManagerImpl<K, V> {
   @Inject KeyPartitioner keyPartitioner;

   protected <I, R> Flowable<CompletionStage<R>> exactlyOnceParallel(CacheSet<I> set,
         Set<K> keysToExclude, Function<I, K> toKeyFunction, IntSet segments,
         Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
         SegmentListener listener, IntSet concurrentSegments) {
      Flowable<I> flowable = Flowable.fromPublisher(set.localPublisher(segments))
            // Remove all the segments once we have iterated upon all the data for the segments
            .doOnComplete(() -> concurrentSegments.removeAll(segments));

      if (keysToExclude != null) {
         flowable = flowable.filter(i -> !keysToExclude.contains(toKeyFunction.apply(i)));
      }

      return flowable.groupBy(i -> keyPartitioner.getSegment(toKeyFunction.apply(i)))
            .map(gf -> {
               // Have to request all segments to do in parallel - otherwise groupBy will get starved
               CompletionStage<R> valueStage = gf.observeOn(asyncScheduler)
                     .to(transformer::apply);
               return valueStage.thenCompose(value -> {
                  // This means a segment was lost in the middle of processing
                  if (listener.segmentsLost.contains(gf.getKey())) {
                     return CompletableFutures.completedNull();
                  }
                  return CompletableFuture.completedFuture(value);
               });
            });
   }

   protected <I, R> Flowable<CompletionStage<R>> exactlyOnceSequential(CacheSet<I> set,
         Set<K> keysToExclude, Function<I, K> toKeyFunction, IntSet segments,
         Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
         SegmentListener listener, IntSet concurrentSegments) {
      Flowable<I> flowable = Flowable.fromPublisher(set.localPublisher(segments))
            // Remove all the segments once we have iterated upon all the data for the segments
            .doOnComplete(() -> concurrentSegments.removeAll(segments));

      if (keysToExclude != null) {
         flowable = flowable.filter(i -> !keysToExclude.contains(toKeyFunction.apply(i)));
      }

      return flowable.groupBy(i -> keyPartitioner.getSegment(toKeyFunction.apply(i)))
            .map(gf -> {
               CompletionStage<R> valueStage = gf.to(transformer::apply);
               return valueStage.thenCompose(value -> {
                  // This means a segment was lost in the middle of processing
                  if (listener.segmentsLost.contains(gf.getKey())) {
                     return CompletableFutures.completedNull();
                  }
                  return CompletableFuture.completedFuture(value);
               });
            });
   }
}
