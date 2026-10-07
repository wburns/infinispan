package org.infinispan.commons.util;

import java.util.function.ToIntFunction;

import org.infinispan.commons.hash.MurmurHash3;

/**
 * Counting BloomFilter implementation that allows for up to 10 hash functions all using MurmurHash3
 * with different seeds. The same seeds are used as in {@link MurmurHash3BloomFilter}.
 *
 * @since 16.3
 */
public class MurmurHash3CountingBloomFilter extends CountingBloomFilter<byte[]> {

   MurmurHash3CountingBloomFilter(int bitsToUse, int hashFunctions) {
      super(bitsToUse, functions(hashFunctions));
   }

   private static int defaultHashFunctionCount() {
      return Integer.parseInt(System.getProperty("infinispan.bloom-filter.hash-functions", "3"));
   }

   public static CountingBloomFilter<byte[]> createFilter(int bitsToUse) {
      return createFilter(bitsToUse, defaultHashFunctionCount());
   }

   public static CountingBloomFilter<byte[]> createFilter(int bitsToUse, int hashFunctions) {
      return new MurmurHash3CountingBloomFilter(bitsToUse, hashFunctions);
   }

   @SuppressWarnings("unchecked")
   private static ToIntFunction<byte[]>[] functions(int hashFunctions) {
      if (hashFunctions <= 0) {
         throw new IllegalArgumentException("Number of hash functions must be positive, received " + hashFunctions);
      }
      ToIntFunction<byte[]>[] functions = new ToIntFunction[hashFunctions];
      for (int i = 0; i < hashFunctions; ++i) {
         int prime = getPrime(i);
         functions[i] = bytes -> MurmurHash3.MurmurHash3_x64_32(bytes, prime);
      }
      return functions;
   }

   private static int getPrime(int offset) {
      switch (offset) {
         case 0:
            return 239;
         case 1:
            return 1847;
         case 2:
            return 2719;
         case 3:
            return 3989;
         case 4:
            return 4481;
         case 5:
            return 5683;
         case 6:
            return 6427;
         case 7:
            return 7537;
         case 8:
            return 8467;
         case 9:
            return 9973;
         default:
            throw new IllegalArgumentException("Only support up to 10 hash functions");
      }
   }
}
