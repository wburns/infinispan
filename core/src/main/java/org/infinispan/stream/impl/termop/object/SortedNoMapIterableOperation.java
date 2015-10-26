package org.infinispan.stream.impl.termop.object;

import org.infinispan.stream.impl.SegmentRetryingCoordinator;
import org.infinispan.stream.impl.SortedIterableTerminalOperation;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.termop.BaseTerminalOperation;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

/**
 * This is a sorted operation where no post operation includes a map or flat map.  There can be a map or flat map
 * before the sort though and it will operate fine.
 */
public class SortedNoMapIterableOperation<E> extends BaseTerminalOperation implements SortedIterableTerminalOperation<E, E> {
   private static final Log log = LogFactory.getLog(SortedNoMapIterableOperation.class);

   protected int batchSize;
   protected long limit;

   protected final Iterable<IntermediateOperation> afterOperations;

   protected final Comparator<? super E> comparator;
   protected final SegmentRetryingCoordinator<Iterable<E>> coordinator;
   protected E lastSeen;

   public SortedNoMapIterableOperation(Iterable<IntermediateOperation> beforeOperations,
           Iterable<IntermediateOperation> afterOperations, Supplier<? extends BaseStream<?, ?>> supplier,
           int batchSize, Comparator<? super E> comparator, Long limit, E lastSeen) {
      super(beforeOperations, supplier);
      this.limit = limit == null ? -1 : limit;
      this.batchSize = batchSize;
      this.afterOperations = afterOperations;
      Objects.nonNull(comparator);
      this.comparator = comparator;
      this.coordinator = new SegmentRetryingCoordinator<>(this::innerPerformOperation, () -> supplier.get());
      this.lastSeen = lastSeen;
   }

   public Iterable<E> innerPerformOperation(BaseStream<?, ?> stream) {
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
         sortableStream.forEach(consumer);

         // We do this before sort just in case
         if (consumer.estimatedSize() == 0) {
            log.tracef("No estimated results found: lastSeen was %s!", lastSeen);
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
      log.tracef("Found %s result(s)", actualCount);
      if (limit > 0) {
         if (limit < actualCount) {
            afterStream = afterStream.limit(limit);
            log.tracef("Limited results to %s", limit);
            limit = 0;
         } else {
            limit -= actualCount;
         }
      }

      for (IntermediateOperation op : afterOperations) {
         afterStream = (Stream<E>) op.perform(afterStream);
      }
      return new MarshallableIterable<>(afterStream::iterator);
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
      if (lastIterable != null) {
         return lastIterable;
      } else {
         return Collections.emptyList();
      }
   }

   @Override
   public void performOperationRehashAware(SortedConsumer<E, E> response) {
      Iterable<E> lastIterable = null;
      Iterable<E> freshIterable;
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

   public Iterable<IntermediateOperation> getAfterOperations() {
      return afterOperations;
   }

   public Comparator<? super E> getComparator() {
      return comparator;
   }

   public E getLastSeen() {
      return lastSeen;
   }

   public int getBatchSize() {
      return batchSize;
   }

   public long getLimit() {
      return limit;
   }
}
