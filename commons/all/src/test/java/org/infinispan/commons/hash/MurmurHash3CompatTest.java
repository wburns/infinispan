package org.infinispan.commons.hash;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Random;

import org.junit.jupiter.api.Test;

public class MurmurHash3CompatTest {

   private static final int SEED = 42;

   @Test
   public void testByteArrayHashes() {
      Random random = new Random(SEED);
      for (int i = 0; i < 10_000; i++) {
         int len = random.nextInt(256);
         byte[] bytes = new byte[len];
         random.nextBytes(bytes);
         int hashSeed = random.nextInt();

         long[] h128Old = MurmurHash3Old.MurmurHash3_x64_128(bytes, hashSeed);
         long[] h128New = MurmurHash3.MurmurHash3_x64_128(bytes, hashSeed);
         assertArrayEquals(h128Old, h128New, "Mismatch for MurmurHash3_x64_128 with len=" + len);

         long h64Old = MurmurHash3Old.MurmurHash3_x64_64(bytes, hashSeed);
         long h64New = MurmurHash3.MurmurHash3_x64_64(bytes, hashSeed);
         assertEquals(h64Old, h64New, "Mismatch for MurmurHash3_x64_64 with len=" + len);

         int h32Old = MurmurHash3Old.MurmurHash3_x64_32(bytes, hashSeed);
         int h32New = MurmurHash3.MurmurHash3_x64_32(bytes, hashSeed);
         assertEquals(h32Old, h32New, "Mismatch for MurmurHash3_x64_32 with len=" + len);

         assertEquals(MurmurHash3Old.getInstance().hash(bytes), MurmurHash3.getInstance().hash(bytes));
      }
   }

   @Test
   public void testLongArrayHashes() {
      Random random = new Random(SEED);
      for (int i = 0; i < 2_000; i++) {
         int len = random.nextInt(63) + 1;
         long[] longs = new long[len];
         for (int j = 0; j < len; j++) {
            longs[j] = random.nextLong();
         }
         int hashSeed = random.nextInt();

         long[] h128Old = MurmurHash3Old.MurmurHash3_x64_128(longs, hashSeed);
         long[] h128New = MurmurHash3.MurmurHash3_x64_128(longs, hashSeed);
         assertArrayEquals(h128Old, h128New, "Mismatch for long[] MurmurHash3_x64_128 with len=" + len);

         long h64Old = MurmurHash3Old.MurmurHash3_x64_64(longs, hashSeed);
         long h64New = MurmurHash3.MurmurHash3_x64_64(longs, hashSeed);
         assertEquals(h64Old, h64New, "Mismatch for long[] MurmurHash3_x64_64 with len=" + len);

         int h32Old = MurmurHash3Old.MurmurHash3_x64_32(longs, hashSeed);
         int h32New = MurmurHash3.MurmurHash3_x64_32(longs, hashSeed);
         assertEquals(h32Old, h32New, "Mismatch for long[] MurmurHash3_x64_32 with len=" + len);

         assertEquals(MurmurHash3Old.hash(longs), MurmurHash3.hash(longs));
      }
   }

   @Test
   public void testIntHashes() {
      Random random = new Random(SEED);
      for (int i = 0; i < 10_000; i++) {
         int val = random.nextInt();
         assertEquals(MurmurHash3Old.getInstance().hash(val), MurmurHash3.getInstance().hash(val));
      }
   }
}
