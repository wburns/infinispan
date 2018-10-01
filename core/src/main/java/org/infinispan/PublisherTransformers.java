package org.infinispan;

import java.util.function.BinaryOperator;

import org.reactivestreams.Publisher;

import io.reactivex.Flowable;

/**
 * @author wburns
 * @since 9.0
 */
public class PublisherTransformers {

   static Publisher<Long> count(CachePublisher<?> cachePublisher) {
      CachePublisher.CachePublisherTransformer<Object, Long> transformer =
            cp -> Flowable.fromPublisher(cp.asPublisher()).count().toFlowable();
      BinaryOperator<Long> binaryOperator = Long::sum;
      return cachePublisher.composeReduce(transformer, binaryOperator);
   }
}
