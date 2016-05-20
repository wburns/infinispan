package org.infinispan.stream.impl.intops.object;

import org.infinispan.stream.impl.intops.ArraySortLimiterStreamBuilder;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.intops.SingleSortLimiterStream;
import org.infinispan.stream.impl.intops.SortLimiterStreamBuilder;

import java.util.Comparator;
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by wburns on 5/19/16.
 */
public class SortedLimitOperation<I> implements IntermediateOperation<I, Stream<I>, I, Stream<I>> {
   private final int limit;
   private final Comparator<? super I> comparator;

   public SortedLimitOperation(int limit, Comparator<? super I> comparator) {
      this.limit = limit;
      this.comparator = comparator;
   }

   @Override
   public Stream<I> perform(Stream<I> stream) {
      boolean isParallel = stream.isParallel();
      // We rely on fact that spliterator doesn't actually cause an invocation - we need it for characteristics
      Spliterator<I> spliterator = stream.spliterator();
      return StreamSupport.stream(() -> {
         SortLimiterStreamBuilder<I> builder;
         if (limit == 1) {
            builder = new SingleSortLimiterStream<I>(comparator);
         } else {
            builder = new ArraySortLimiterStreamBuilder<I>(limit, spliterator.getExactSizeIfKnown(), comparator);
         }
         spliterator.forEachRemaining(builder);
         return builder.stream().spliterator();
      }, spliterator.characteristics() | Spliterator.SORTED | Spliterator.ORDERED, isParallel);
   }
}
