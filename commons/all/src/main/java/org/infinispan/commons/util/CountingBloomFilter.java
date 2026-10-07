package org.infinispan.commons.util;

import java.util.Arrays;
import java.util.function.ToIntFunction;

/**
 * A Counting Bloom Filter implementation that allows for addition, removal, and point-in-time
 * bit set generation in O(1) time without requiring key re-serialization or cache traversals.
 *
 * @param <E> type of element added to the filter
 * @since 16.3
 */
public class CountingBloomFilter<E> {
   private final int bitsToUse;
   private final byte[] counts;
   private final ToIntFunction<? super E>[] hashFunctions;

   CountingBloomFilter(int bitsToUse, ToIntFunction<? super E>[] hashFunctions) {
      if (bitsToUse <= 0) {
         throw new IllegalArgumentException("bitsToUse must be positive, received " + bitsToUse);
      }
      this.bitsToUse = bitsToUse;
      this.counts = new byte[bitsToUse];
      this.hashFunctions = hashFunctions;
   }

   public synchronized void add(E value) {
      for (ToIntFunction<? super E> function : hashFunctions) {
         int hash = function.applyAsInt(value);
         int bit = (hash == Integer.MIN_VALUE ? 0 : Math.abs(hash)) % bitsToUse;
         byte count = counts[bit];
         if (count < Byte.MAX_VALUE) {
            counts[bit] = (byte) (count + 1);
         }
      }
   }

   public synchronized void remove(E value) {
      for (ToIntFunction<? super E> function : hashFunctions) {
         int hash = function.applyAsInt(value);
         int bit = (hash == Integer.MIN_VALUE ? 0 : Math.abs(hash)) % bitsToUse;
         byte count = counts[bit];
         if (count > 0) {
            counts[bit] = (byte) (count - 1);
         }
      }
   }

   public synchronized boolean possiblyPresent(E value) {
      for (ToIntFunction<? super E> function : hashFunctions) {
         int hash = function.applyAsInt(value);
         int bit = (hash == Integer.MIN_VALUE ? 0 : Math.abs(hash)) % bitsToUse;
         if (counts[bit] == 0) {
            return false;
         }
      }
      return true;
   }

   public synchronized byte[] toBitSet() {
      int byteLen = (bitsToUse + 7) >>> 3;
      byte[] bytes = new byte[byteLen];
      for (int i = 0; i < bitsToUse; i++) {
         if (counts[i] > 0) {
            bytes[i >>> 3] |= (1 << (i & 7));
         }
      }
      return bytes;
   }

   public synchronized void clear() {
      Arrays.fill(counts, (byte) 0);
   }

   public int bitsToUse() {
      return bitsToUse;
   }
}
