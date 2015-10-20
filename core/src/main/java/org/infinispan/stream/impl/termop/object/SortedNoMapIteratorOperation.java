package org.infinispan.stream.impl.termop.object;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.stream.impl.KeyTrackingTerminalOperation;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.termop.BaseTerminalOperation;

import java.util.Collection;
import java.util.function.Supplier;
import java.util.stream.BaseStream;

/**
 * This is a sorted operation where no post operation includes a map or flat map
 */
public class SortedNoMapIteratorOperation<K, E> extends BaseTerminalOperation implements KeyTrackingTerminalOperation<K, E,
        E> {
   protected SortedNoMapIteratorOperation(Iterable<IntermediateOperation> intermediateOperations,
           Supplier<? extends BaseStream<?, ?>> supplier) {
      super(intermediateOperations, supplier);
   }

   @Override
   public Collection<E> performOperation(IntermediateCollector<Iterable<E>> response) {
      return null;
   }

   @Override
   public Collection<CacheEntry<K, E>> performOperationRehashAware(IntermediateCollector<Iterable<CacheEntry<K, E>>> response) {
      return null;
   }

   @Override
   public boolean lostSegment(boolean allSegmentsLost) {
      return false;
   }
}
