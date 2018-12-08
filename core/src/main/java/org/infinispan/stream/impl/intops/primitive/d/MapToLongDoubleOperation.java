package org.infinispan.stream.impl.intops.primitive.d;

import java.util.function.DoubleToLongFunction;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import io.reactivex.Flowable;

/**
 * Performs map to long operation on a {@link DoubleStream}
 */
public class MapToLongDoubleOperation implements IntermediateOperation<Double, DoubleStream, Long, LongStream> {
   private final DoubleToLongFunction function;

   public MapToLongDoubleOperation(DoubleToLongFunction function) {
      this.function = function;
   }

   @Override
   public LongStream perform(DoubleStream stream) {
      return stream.mapToLong(function);
   }

   @Override
   public Flowable<Long> performPublisher(Flowable<Double> publisher) {
      return publisher.map(function::applyAsLong);
   }

   public DoubleToLongFunction getFunction() {
      return function;
   }
}
