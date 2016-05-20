package org.infinispan.stream.impl.intops;

import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 *
 */
public class ArraySortLimiterStreamBuilder<T> implements SortLimiterStreamBuilder<T> {
   protected final T[] data;
   protected final int limit;
   protected final Comparator<? super T> comparator;

   protected int virtualSize;
   protected boolean initialSort = false;

   /**
    * @param limit how many least elements to keep
    * @param length length of the input if known (not an estimate), -1 otherwise
    * @param comparator comparator for sorting
    */
   public ArraySortLimiterStreamBuilder(int limit, long length, Comparator<? super T> comparator) {
      this.limit = limit;
      this.comparator = comparator;
      int dataSize = limit * 2;
      if (length >= 0 && length < dataSize)
         dataSize = (int) length;
      this.data = (T[]) new Object[dataSize];
   }

   public void accept(T element) {
      if (!initialSort || comparator.compare(element, data[limit - 1]) < 0) {
         data[virtualSize++] = element;
         if (virtualSize == data.length) {
            compact();
         }
      }
   }

   @Override
   public Stream<T> stream() {
      if (virtualSize == 0) {
         return Stream.empty();
      }
      compact();
      return Arrays.stream(data, 0, virtualSize);
   }

   @Override
   public Stream<T> parallelStream() {
      return StreamSupport.stream(Arrays.spliterator(data, 0, virtualSize), true);
   }

   /**
    * Sorts the elements so that the first limit elements are ordered
    */
   protected void compact() {
      // Just sort them all the first time
      if (!initialSort) {
         Arrays.sort(data, 0, virtualSize, comparator);
         initialSort = true;
      }
      else if (virtualSize > limit) {
         // The initial half is already sorted so just sort the remaining ones
         sortTail(data, limit, virtualSize, comparator);
      } else {
         // All compacted already
         return;
      }
      virtualSize = limit;
   }

   /*
    * Sort the latter half of the array into the earlier half of the array
    */
   protected static <T> void sortTail(T[] data, int limit, int size, Comparator<? super T> comparator) {
      Arrays.sort(data, limit, size, comparator);
      // If the highest is lower than the already lowest, we can move the entire array by just shifting
      if (comparator.compare(data[size - 1], data[0]) < 0) {
         // Shift the old head contents over by how many new elements we have
         System.arraycopy(data, 0, data, size - limit, 2 * limit - size);
         // Copy the new elements into the now open spots
         System.arraycopy(data, limit, data, 0, size - limit);
      } else {
         @SuppressWarnings("unchecked")
         T[] buf = (T[]) new Object[limit];
         int i = 0, j = limit, k = 0;
         // data[limit-1] is guaranteed to be the worst element, thus no need
         // to check it
         while (i < limit - 1 && k < limit && j < size) {
            buf[k++] = comparator.compare(data[i], data[j]) <= 0 ? data[i++] : data[j++];
         }
         if (k < limit) {
            System.arraycopy(data, i < limit - 1 ? i : j, data, k, limit - k);
         }
         System.arraycopy(buf, 0, data, 0, k);
      }
   }
}
