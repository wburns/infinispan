package org.infinispan.stream.impl.termop.object;

import org.infinispan.stream.impl.SegmentRetryingCoordinator;
import org.infinispan.stream.impl.SortedNoMapTerminalOperation;
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
public class SortedNoMapIteratorOperation<E> extends BaseTerminalOperation implements SortedNoMapTerminalOperation<E> {
   protected int batchSize;
   protected long limit;

   protected final Iterable<IntermediateOperation> afterOperations;
   protected final Comparator<? super E> comparator;
   protected final SegmentRetryingCoordinator<Iterable<E>> coordinator;
   protected E lastSeen;

   protected SortedNoMapIteratorOperation(Iterable<IntermediateOperation> beforeOperations,
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

   private int getLocalBatchSize() {
      if (limit == -1) {
         return batchSize;
      } else if (limit > 0) {
         return batchSize > limit ? (int) limit : batchSize;
      } else {
         return Integer.MIN_VALUE;
      }
   }

   public Iterable<E> innerPerformOperation(BaseStream<?, ?> stream) {
      int batchSize = getLocalBatchSize();
      if (batchSize == Integer.MIN_VALUE) {
         return null;
      }
      for (IntermediateOperation op : intermediateOperations) {
         stream = op.perform(stream);
      }

      // now we should have a Stream of E
      Stream<E> sortableStream = (Stream<E>) stream.sequential();
      StreamedConsumer<E> consumer = new ArraySortedStreamedConsumer<>(batchSize, comparator);
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
            limit = Long.MIN_VALUE;
         } else {
            limit -= actualCount;
         }
      }

      for (IntermediateOperation op : afterOperations) {
         afterStream = (Stream<E>) op.perform(afterStream);
      }
      return afterStream::iterator;
   }

   @Override
   public boolean lostSegment(boolean allSegmentsLost) {
      return coordinator.lostSegment(allSegmentsLost);
   }

   @Override
   public Iterable<E> performOperation(Consumer<Iterable<E>> response) {
      Iterable<E> lastIterable = null;
      Iterable<E> freshIterable;
      while ((freshIterable = innerPerformOperation(supplier.get())) != null) {
         if (lastIterable != null) {
            response.accept(lastIterable);
         }
         lastIterable = freshIterable;
      }
      return lastIterable;
   }

   @Override
   public Iterable<E> performOperationRehashAware(Consumer<Iterable<E>> response) {
      Iterable<E> lastIterable = null;
      Iterable<E> freshIterable;
      while ((freshIterable = coordinator.runOperation()) != null) {
         if (lastIterable != null) {
            response.accept(lastIterable);
         }
         lastIterable = freshIterable;
      }
      return lastIterable;
   }
}
