package org.infinispan.stream.impl.intops.object;

import java.util.stream.Stream;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import io.reactivex.Flowable;

/**
 * Performs limit operation on a regular {@link Stream}
 */
public class LimitOperation<S> implements IntermediateOperation<S, Stream<S>, S, Stream<S>> {
   private final long limit;

   public LimitOperation(long limit) {
      if (limit <= 0) {
         throw new IllegalArgumentException("Limit must be greater than 0");
      }
      this.limit = limit;
   }

   @Override
   public Stream<S> perform(Stream<S> stream) {
      return stream.limit(limit);
   }

   @Override
   public Flowable<S> performPublisher(Flowable<S> publisher) {
      return publisher.limit(limit);
   }

   public long getLimit() {
      return limit;
   }
}
