package org.infinispan.stream.impl.intops.primitive.l;

import java.util.function.LongFunction;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import io.reactivex.Flowable;

/**
 * Performs map to object operation on a {@link LongStream}
 */
public class MapToObjLongOperation<R> implements IntermediateOperation<Long, LongStream, R, Stream<R>> {
   private final LongFunction<? extends R> function;

   public MapToObjLongOperation(LongFunction<? extends R> function) {
      this.function = function;
   }

   @Override
   public Stream<R> perform(LongStream stream) {
      return stream.mapToObj(function);
   }

   @Override
   public Flowable<R> performPublisher(Flowable<Long> publisher) {
      return publisher.map(function::apply);
   }

   public LongFunction<? extends R> getFunction() {
      return function;
   }
}
