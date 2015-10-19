package org.infinispan.stream.impl.termop;

import org.infinispan.stream.impl.SegmentRetryingCoordinator;
import org.infinispan.stream.impl.TerminalOperation;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

/**
 * A terminal based operation that runs the provided function to evaluate the operation.  If a segment is lost during
 * the evaluation of the function the function results will be ignored and subsequently retried with the new stable
 * segments.  This is repeated until either a full stable run is completed of the function or if the lost segment
 * states that there are no more segments left.
 * @param <E> output type of the function
 * @param <T> type of the stream entries
 * @param <S> type of the stream itself
 */
public class SegmentRetryingOperation<E, T, S extends BaseStream<T, S>> extends BaseTerminalOperation
        implements TerminalOperation<E> {
   private final Function<S, ? extends E> function;
   private final SegmentRetryingCoordinator<E> coordinator;

   public SegmentRetryingOperation(Iterable<IntermediateOperation> intermediateOperations,
           Supplier<? extends Stream<?>> supplier, Function<S, ? extends E> function) {
      super(intermediateOperations, supplier);
      this.function = function;
      this.coordinator = new SegmentRetryingCoordinator<>(this::innerPerformOperation,
              // Note we can't pass the supplier as is, since when this object is deserialized the supplier is set
              // later, so we have to reference that instead
              () -> this.supplier.get());
   }

   @Override
   public boolean lostSegment(boolean stopIfLost) {
      return coordinator.lostSegment(stopIfLost);
   }

   private E innerPerformOperation(BaseStream<?, ?> stream) {
      for (IntermediateOperation intOp : intermediateOperations) {
         stream = intOp.perform(stream);
      }
      return function.apply((S) stream);
   }

   @Override
   public E performOperation() {
      return coordinator.runOperation();
   }

   public Function<S, ? extends E> getFunction() {
      return function;
   }
}
