package org.infinispan.stream.impl.intops.primitive.d;

import java.util.function.DoublePredicate;
import java.util.stream.DoubleStream;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import io.reactivex.Flowable;

/**
 * Performs filter operation on a {@link DoubleStream}
 */
public class FilterDoubleOperation implements IntermediateOperation<Double, DoubleStream, Double, DoubleStream> {
   private final DoublePredicate predicate;

   public FilterDoubleOperation(DoublePredicate predicate) {
      this.predicate = predicate;
   }

   @Override
   public DoubleStream perform(DoubleStream stream) {
      return stream.filter(predicate);
   }

   @Override
   public Flowable<Double> performPublisher(Flowable<Double> publisher) {
      return publisher.filter(predicate::test);
   }

   public DoublePredicate getPredicate() {
      return predicate;
   }
}
