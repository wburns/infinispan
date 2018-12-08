package org.infinispan.stream.impl.intops.primitive.l;

import java.util.function.LongPredicate;
import java.util.stream.LongStream;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import io.reactivex.Flowable;

/**
 * Performs filter operation on a {@link LongStream}
 */
public class FilterLongOperation<S> implements IntermediateOperation<Long, LongStream, Long, LongStream> {
   private final LongPredicate predicate;

   public FilterLongOperation(LongPredicate predicate) {
      this.predicate = predicate;
   }

   @Override
   public LongStream perform(LongStream stream) {
      return stream.filter(predicate);
   }

   @Override
   public Flowable<Long> performPublisher(Flowable<Long> publisher) {
      return publisher.filter(predicate::test);
   }

   public LongPredicate getPredicate() {
      return predicate;
   }
}
