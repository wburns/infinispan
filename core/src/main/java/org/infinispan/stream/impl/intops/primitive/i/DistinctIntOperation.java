package org.infinispan.stream.impl.intops.primitive.i;

import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.intops.LimitableOperation;

import java.util.stream.IntStream;

/**
 * Performs distinct operation on a {@link IntStream}
 */
public class DistinctIntOperation implements IntermediateOperation<Integer, IntStream, Integer, IntStream>, LimitableOperation {
   private static final DistinctIntOperation OPERATION = new DistinctIntOperation();
   private DistinctIntOperation() { }

   public static DistinctIntOperation getInstance() {
      return OPERATION;
   }

   @Override
   public IntStream perform(IntStream stream) {
      return stream.distinct();
   }
}
