package org.infinispan.stream.impl.termop.object;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Created by wburns on 10/22/15.
 */
public class ArraySortedStreamedConsumer<E> implements StreamedConsumer<E> {
   private final int size;
   private final Comparator<? super E> comparator;

   private final E[] array;

   private int virtualSize;
   private boolean sizeSorted = false;

   public ArraySortedStreamedConsumer(int size, Comparator<? super E> comparator) {
      if (size < 1) {
         throw new IllegalArgumentException("Size should be greater than 0, was " + size);
      }
      this.size = size;
      Objects.nonNull(comparator);
      this.comparator = comparator;

      this.array = (E[]) new Object[size << 1];

      this.virtualSize = 0;
   }

   @Override
   public void accept(E element) {
      // If we have a sorted size already only allow the element if it is smaller than the largest of size.
      // We have to keep duplicates in case if we have a boundary overlap
      if (!sizeSorted || comparator.compare(element, array[size - 1]) <= 0) {
         array[virtualSize++] = element;
         // If the virtual size is now greater than our array size we have to sort and do compaction
         if (virtualSize == size * 2) {
            compact();
         }
      }
   }

   @Override
   public Stream<E> stream() {
      if (virtualSize == 0) {
         return Stream.empty();
      }
      Stream<E> stream = Arrays.stream(array, 0, virtualSize);
      return comparator == null ? stream.sorted() : stream.sorted(comparator);
   }

   @Override
   public E compact() {
      if (virtualSize == 0) {
         return null;
      }
      boolean bigger = virtualSize > size;
      if (!sizeSorted || bigger) {
         Arrays.sort(array, 0, virtualSize, comparator);
      }
      if (bigger) {
         virtualSize = size;
         // This means we had a duplicate value spanning size boundaries.  We can't allow that so we have to throw
         // exception
         if (virtualSize > 0 && comparator.compare(array[virtualSize - 1], array[virtualSize]) == 0) {
            throw new BatchOverlapException("Number of duplicates via compareTo outnumbers size " + size);
         }
      }
      sizeSorted = true;
      return array[virtualSize - 1];
   }

   @Override
   public long estimatedSize() {
      return virtualSize;
   }
}
