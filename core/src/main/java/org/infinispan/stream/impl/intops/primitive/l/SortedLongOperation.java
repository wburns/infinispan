package org.infinispan.stream.impl.intops.primitive.l;

import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.intops.LimitableOperation;

import java.util.stream.LongStream;

/**
 * Performs sorted operation on a {@link LongStream}
 */
public class SortedLongOperation implements IntermediateOperation<Long, LongStream, Long, LongStream>, LimitableOperation {
   private static final SortedLongOperation OPERATION = new SortedLongOperation();
   private SortedLongOperation() { }

   public static SortedLongOperation getInstance() {
      return OPERATION;
   }

   @Override
   public LongStream perform(LongStream stream) {
      return stream.sorted();
   }
}
