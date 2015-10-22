package org.infinispan.stream.impl.termop.object;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Created by wburns on 10/22/15.
 */
public class ArrayListSortedConsumer<E> implements SortedConsumer<E> {
   private final int size;
   private final Comparator<? super E> comparator;

   private final List<E> list;

   private int virtualSize;

   public ArrayListSortedConsumer(int size, Comparator<? super E> comparator) {
      this.size = size;
      this.comparator = comparator;

      this.list = new ArrayList<>(size >> 1);

      this.virtualSize = 0;
   }

   @Override
   public void addElement(E element) {
      list.add(virtualSize++, element);
      // If the virtual size is now greater than our array size we have to sort and do compaction
      if (virtualSize == size * 2) {
         list.sort(comparator);
         virtualSize = size;
      }
   }

   @Override
   public Stream<E> toStream() {
      Stream<E> stream = list.stream().limit(virtualSize);
      return comparator == null ? stream.sorted() : stream.sorted(comparator);
   }
}
