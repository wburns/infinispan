package org.infinispan.stream.impl.intops.primitive.l;

import java.util.stream.LongStream;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import io.reactivex.Flowable;

/**
 * Performs sorted operation on a {@link LongStream}
 */
public class SortedLongOperation implements IntermediateOperation<Long, LongStream, Long, LongStream> {
   private static final SortedLongOperation OPERATION = new SortedLongOperation();
   private SortedLongOperation() { }

   public static SortedLongOperation getInstance() {
      return OPERATION;
   }

   @Override
   public LongStream perform(LongStream stream) {
      return stream.sorted();
   }

   @Override
   public Flowable<Long> performPublisher(Flowable<Long> publisher) {
      return publisher.sorted();
   }
}
