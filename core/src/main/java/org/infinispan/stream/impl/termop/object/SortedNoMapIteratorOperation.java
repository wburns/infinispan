package org.infinispan.stream.impl.termop.object;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.stream.impl.KeyTrackingTerminalOperation;
import org.infinispan.stream.impl.SegmentRetryingCoordinator;
import org.infinispan.stream.impl.SortedNoMapTerminalOperation;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.termop.BaseTerminalOperation;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

/**
 * This is a sorted operation where no post operation includes a map or flat map
 */
public class SortedNoMapIteratorOperation<E> extends BaseTerminalOperation implements SortedNoMapTerminalOperation<E> {
   protected final int batchSize;
   protected final Long limit;
   protected final Iterable<IntermediateOperation> afterOperations;
   protected final Comparator<? super E> comparator;
   protected final SegmentRetryingCoordinator<Iterable<E>> coordinator;
   protected E lastSeen;
   protected boolean completed = false;

   protected SortedNoMapIteratorOperation(Iterable<IntermediateOperation> beforeOperations,
           Iterable<IntermediateOperation> afterOperations, Supplier<? extends BaseStream<?, ?>> supplier,
           int batchSize, Comparator<? super E> comparator, Long limit, E lastSeen) {
      super(beforeOperations, supplier);
      this.batchSize = batchSize;
      this.afterOperations = afterOperations;
      this.comparator = comparator == null ? (Comparator<E>) Comparator.naturalOrder() : comparator;
      this.limit = limit;
      this.coordinator = new SegmentRetryingCoordinator<>(this::innerPerformOperation, () -> supplier.get());
      this.lastSeen = lastSeen;
   }

   public Iterable<E> innerPerformOperation(BaseStream<?, ?> stream) {
      for (IntermediateOperation op : intermediateOperations) {
         stream = op.perform(stream);
      }
      // now we should have a Stream of E
      Stream<E> sortableStream = (Stream<E>) stream;
      NavigableSet<E> sortedSet = new TreeSet<>(comparator);
      sortableStream.sequential().forEach(e -> {
         if (lastSeen == null || comparator.compare(lastSeen, e) < 0) {
            sortedSet.add(e);
            if (sortedSet.size() == batchSize + 1) {
               sortedSet.pollLast();
            }
         }
      });
      if (sortedSet.size() > batchSize) {
         Iterator<E> descIterator = sortedSet.descendingIterator();
         E top = descIterator.next();
         descIterator.remove();
         // We have to remove any entries that have same compared value at the tail
         while (descIterator.hasNext()) {
            if (comparator.compare(top, descIterator.next()) == 0) {
               descIterator.remove();
            } else {
               break;
            }
         }
         // This is the last seen value, so next iteration it won't add these
         if (descIterator.hasNext()) {
            lastSeen = descIterator.next();
         }
      } else {
         completed = true;
      }
      Stream<E> afterStream = sortedSet.stream();
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
      // TODO: need to do looping
      return innerPerformOperation(supplier.get());
   }

   @Override
   public Iterable<E> performOperationRehashAware(Consumer<Iterable<E>> response) {
      // TODO: need to do looping
      return coordinator.runOperation();
   }
}
