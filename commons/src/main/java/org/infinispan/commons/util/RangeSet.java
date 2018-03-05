package org.infinispan.commons.util;

import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.stream.IntStream;

/**
 * Read-only set representing all the integers from {@code 0} to {@code size - 1} (inclusive).
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class RangeSet implements IntSet {
   final int size;

   public RangeSet(int size) {
      this.size = size;
   }

   @Override
   public int size() {
      return size;
   }

   @Override
   public boolean isEmpty() {
      return size <= 0;
   }

   @Override
   public boolean contains(Object o) {
      if (!(o instanceof Integer))
         return false;
      int i = (int) o;
      return contains(i);
   }

   @Override
   public boolean contains(int i) {
      return 0 <= i && i < size;
   }

   @Override
   public PrimitiveIterator.OfInt iterator() {
      return new RangeSetIterator(size);
   }

   @Override
   public int[] toIntArray() {
      int[] array = new int[size];
      for (int i = 0; i < size; i++) {
         array[i] = i;
      }
      return array;
   }

   @Override
   public Object[] toArray() {
      Object[] array = new Object[size];
      for (int i = 0; i < size; i++) {
         array[i] = i;
      }
      return array;
   }

   @Override
   public <T> T[] toArray(T[] a) {
      T[] array = a.length >= size ? a :
                  (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);
      for (int i = 0; i < size; i++) {
         array[i] = (T) Integer.valueOf(i);
      }
      return array;
   }

   @Override
   public boolean add(Integer integer) {
      throw new UnsupportedOperationException("RangeSet is immutable");
   }

   @Override
   public boolean remove(Object o) {
      throw new UnsupportedOperationException("RangeSet is immutable");
   }

   @Override
   public boolean remove(int i) {
      throw new UnsupportedOperationException("RangeSet is immutable");
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      if (c instanceof IntSet) {
         return containsAll((IntSet) c);
      }
      for (Object o : c) {
         if (!contains(o))
            return false;
      }
      return true;
   }

   @Override
   public boolean containsAll(IntSet set) {
      if (set instanceof RangeSet) {
         return size >= ((RangeSet) set).size;
      }
      PrimitiveIterator.OfInt iter = set.iterator();
      while (iter.hasNext()) {
         if (!contains(iter.nextInt())) {
            return false;
         }
      }
      return true;
   }

   @Override
   public boolean add(int i) {
      throw new UnsupportedOperationException("RangeSet is immutable");
   }

   public void set(int i) {
      throw new UnsupportedOperationException("RangeSet is immutable");
   }

   @Override
   public boolean addAll(IntSet set) {
      throw new UnsupportedOperationException("RangeSet is immutable");
   }

   @Override
   public boolean addAll(Collection<? extends Integer> c) {
      throw new UnsupportedOperationException("RangeSet is immutable");
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException("RangeSet is immutable");
   }

   @Override
   public boolean retainAll(IntSet c) {
      throw new UnsupportedOperationException("RangeSet is immutable");
   }

   @Override
   public boolean removeAll(IntSet set) {
      throw new UnsupportedOperationException("RangeSet is immutable");
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException("RangeSet is immutable");
   }

   @Override
   public void clear() {
      throw new UnsupportedOperationException("RangeSet is immutable");
   }

   @Override
   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (o == null || !(o instanceof Set))
         return false;

      if (o instanceof RangeSet) {
         RangeSet integers = (RangeSet) o;

         return size == integers.size;
      } else {
         Set set = (Set) o;
         return size == set.size() && containsAll(set);
      }
   }

   @Override
   public IntStream intStream() {
      return IntStream.range(0, size);
   }

   @Override
   public int hashCode() {
      return size;
   }

   @Override
   public String toString() {
      return "RangeSet(" + size + ")";
   }

   private static class RangeSetIterator implements PrimitiveIterator.OfInt {
      private int size;
      private int next;

      public RangeSetIterator(int size) {
         this.size = size;
         this.next = 0;
      }

      @Override
      public boolean hasNext() {
         return next < size;
      }

      @Override
      public int nextInt() {
         if (next >= size) {
            throw new NoSuchElementException();
         }
         return next++;
      }

      @Override
      public Integer next() {
         return nextInt();
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException("RangeSet is read-only");
      }
   }
}
