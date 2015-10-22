package org.infinispan.stream.impl.termop.object;

import org.infinispan.stream.impl.SegmentRetryingCoordinator;
import org.infinispan.stream.impl.SortedIterableTerminalOperation;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.termop.BaseTerminalOperation;

import java.util.Comparator;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

/**
 * This is a sorted operation where no post operation includes a map or flat map
 */
public class SortedIterableOperation<E, R> extends BaseTerminalOperation implements SortedIterableTerminalOperation<E, R> {
   protected int batchSize;
   protected long limit;

   protected final Iterable<IntermediateOperation> afterOperations;
   protected final Comparator<? super E> comparator;
   protected final SegmentRetryingCoordinator<Iterable<R>> coordinator;
   protected E lastSeen;

   protected SortedIterableOperation(Iterable<IntermediateOperation> beforeOperations,
           Iterable<IntermediateOperation> afterOperations, Supplier<? extends BaseStream<?, ?>> supplier,
           int batchSize, Comparator<? super E> comparator, Long limit, E lastSeen) {
      super(beforeOperations, supplier);
      this.limit = limit == null ? -1 : limit;
      this.batchSize = batchSize;
      this.afterOperations = afterOperations;
      this.comparator = comparator == null ? (Comparator<E>) Comparator.naturalOrder() : comparator;
      this.coordinator = new SegmentRetryingCoordinator<>(this::innerPerformOperation, () -> supplier.get());
      this.lastSeen = lastSeen;
   }

   public Iterable<R> innerPerformOperation(BaseStream<?, ?> stream) {
      int batchSize;
      boolean skipOverlap;
      if (limit == -1) {
         batchSize = this.batchSize;
         skipOverlap = false;
      } else if (limit > 0) {
         // If the limist currently less than batch size, use that instead and we can skip the overlap check
         // since we can just truncate the top end without error
         if (limit < this.batchSize) {
            batchSize = (int) limit;
            skipOverlap = true;
         } else {
            batchSize = this.batchSize;
            skipOverlap = false;
         }
      } else {
         // If limit is 0 then that means we already returned the limit number
         return null;
      }
      for (IntermediateOperation op : intermediateOperations) {
         stream = op.perform(stream);
      }

      // now we should have a Stream of E
      Stream<E> sortableStream = (Stream<E>) stream.sequential();
      StreamedConsumer<E> consumer = new ArraySortedStreamedConsumer<>(batchSize, comparator, skipOverlap);
      if (lastSeen != null) {
         sortableStream = sortableStream.filter(e -> comparator.compare(lastSeen, e) < 0);
      }
      try {
         sortableStream.sequential().forEach(consumer);

         // We do this before sort just in case
         if (consumer.estimatedSize() == 0) {
            return null;
         }

         lastSeen = consumer.compact();
      } catch (BatchOverlapException e) {
         // We retry the operation with the batchSize doubled to try to see if we can fit the values without
         // overlapping into the buffer space of the batch size
         this.batchSize = batchSize << 1;
         return innerPerformOperation(supplier.get());
      }

      Stream<E> afterStream = consumer.stream();

      long actualCount = consumer.estimatedSize();
      if (limit > 0) {
         if (limit < actualCount) {
            afterStream = afterStream.limit(limit);
            limit = 0;
         } else {
            limit -= actualCount;
         }
      }

      Stream<R> finalStream = (Stream<R>) afterStream;
      for (IntermediateOperation op : afterOperations) {
         finalStream = (Stream<R>) op.perform(finalStream);
      }
      return finalStream::iterator;
   }

   @Override
   public boolean lostSegment(boolean allSegmentsLost) {
      return coordinator.lostSegment(allSegmentsLost);
   }

   @Override
   public Iterable<R> performOperation(Consumer<Iterable<R>> response) {
      Iterable<R> lastIterable = null;
      Iterable<R> freshIterable;
      while ((freshIterable = innerPerformOperation(supplier.get())) != null) {
         if (lastIterable != null) {
            response.accept(lastIterable);
         }
         lastIterable = freshIterable;
      }
      return lastIterable;
   }

   @Override
   public void performOperationRehashAware(SortedConsumer<E, R> response) {
      Iterable<R> lastIterable = null;
      Iterable<R> freshIterable;
      E lastSeen = null;
      while ((freshIterable = coordinator.runOperation()) != null) {
         if (lastIterable != null) {
            response.accept(lastIterable, lastSeen);
         }
         lastSeen = this.lastSeen;
         lastIterable = freshIterable;
      }
      response.completed(lastIterable, lastSeen);
   }
}
