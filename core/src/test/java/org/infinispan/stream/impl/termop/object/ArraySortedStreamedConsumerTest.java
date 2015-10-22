package org.infinispan.stream.impl.termop.object;

import org.testng.annotations.Test;

import java.util.Comparator;

/**
 * Created by wburns on 10/22/15.
 */
@Test(groups = "functional", testName = "stream.impl.termop.object.ArraySortedStreamedConsumerTest")
public class ArraySortedStreamedConsumerTest {
   @Test(expectedExceptions = BatchOverlapException.class)
   public void testDuplicatesLargerThanBatchDuringAccept() {
      int size = 5;
      ArraySortedStreamedConsumer<Integer> consumer = new ArraySortedStreamedConsumer<>(size, Comparator.naturalOrder(),
              false);
      for (int i = 0; i < size * 2 + 1; ++i) {
         consumer.accept(10);
      }
   }

   @Test(expectedExceptions = BatchOverlapException.class)
   public void testDuplicatesLargerThanBatchDuringCompaction() {
      int size = 5;
      ArraySortedStreamedConsumer<Integer> consumer = new ArraySortedStreamedConsumer<>(size, Comparator.naturalOrder(),
              false);
      for (int i = 0; i < size + 1; ++i) {
         consumer.accept(10);
      }

      consumer.compact();
   }
}
