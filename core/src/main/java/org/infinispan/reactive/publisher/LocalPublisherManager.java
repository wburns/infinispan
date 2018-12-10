package org.infinispan.reactive.publisher;

import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.reactive.publisher.impl.PublisherResult;
import org.reactivestreams.Publisher;

/**
 * @author wburns
 * @since 10.0
 */
public interface LocalPublisherManager<K, V> {
   <R> CompletionStage<PublisherResult<R>> keyPublisherOperation(boolean parallelStream, IntSet segments,
         Set<K> keysToInclude, Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<K>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer);

   <R> CompletionStage<PublisherResult<R>> entryPublisherOperation(boolean parallelStream, IntSet segments,
         Set<K> keysToInclude, Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer);

   void segmentsLost(IntSet lostSegments);
}
