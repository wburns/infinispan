package org.infinispan.stream.impl.intops;

import java.util.Comparator;
import java.util.stream.Stream;

/**
 * Created by wburns on 5/19/16.
 */
public class SingleSortLimiterStream<E> implements SortLimiterStreamBuilder<E> {
   private final Comparator<? super E> comparator;

   private E element;
   private boolean initialized = false;

   public SingleSortLimiterStream(Comparator<? super E> comparator) {
      this.comparator = comparator;
   }

   @Override
   public void accept(E e) {
      if (!initialized) {
         element = e;
      } else if (comparator.compare(e, element) < 0) {
         element = e;
      }
   }

   @Override
   public Stream<E> stream() {
      if (initialized) {
         return Stream.of(element);
      } else {
         return Stream.empty();
      }
   }

   @Override
   public Stream<E> parallelStream() {
      // No reason to make this parallel
      return stream();
   }
}
