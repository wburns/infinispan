package org.infinispan.stream.impl.intops.primitive.d;

import java.util.stream.DoubleStream;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import io.reactivex.Flowable;

/**
 * Performs limit operation on a {@link DoubleStream}
 */
public class LimitDoubleOperation implements IntermediateOperation<Double, DoubleStream, Double, DoubleStream> {
   private final long limit;

   public LimitDoubleOperation(long limit) {
      if (limit <= 0) {
         throw new IllegalArgumentException("Limit must be greater than 0");
      }
      this.limit = limit;
   }

   @Override
   public DoubleStream perform(DoubleStream stream) {
      return stream.limit(limit);
   }

   public long getLimit() {
      return limit;
   }

   @Override
   public Flowable<Double> mapFlowable(Flowable<Double> input) {
      return input.limit(limit);
   }
}
