package org.infinispan.stream.impl.termop.object;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Created by wburns on 10/21/15.
 */
@Test
public class SortedNoMapIterableOperationTest {
   public List<Integer> generateInts() {
      List<Integer> ints = new ArrayList<>(10);
      ints.add(12);
      ints.add(12341);

      ints.add(7);
      ints.add(999484684);

      ints.add(12340);
      ints.add(Integer.MIN_VALUE);

      ints.add(7);
      ints.add(Integer.MAX_VALUE);

      ints.add(-81731);
      ints.add(Integer.MAX_VALUE - 1);

      return ints;
   }

   private static Object[][] createNumbers(int amount, int offset) {
      Object[][] data = new Object[amount][];
      for (int i = 0; i < amount; i++) {
         // Note the value is offset by 1.  This is because 0 based batch makes no sense
         data[i] = new Object[] { Integer.valueOf(i + offset) };
      }
      return data;
   }

   @DataProvider(name="batchNumbers")
   public static Object[][] createBatchNumbers() {
      return createNumbers(11, 1);
   }

   @DataProvider(name="limitNumbers")
   public static Object[][] createLimitNumbers() {
      return createNumbers(11, 1);
   }

   @Test(dataProvider = "batchNumbers")
   public void testBatchNumbers(Integer batchCount) {
      List<Integer> input = generateInts();
      List<Integer> streamInput = new ArrayList<>(input);

      SortedNoMapIterableOperation<Integer> op = new SortedNoMapIterableOperation(Collections.emptyList(),
              Collections.emptyList(), () -> streamInput.stream(), batchCount, null, null, null);

      Consumer<Iterable<Integer>> consumer = Mockito.mock(Consumer.class);
      Iterable<Integer> lastIterable = op.performOperation(consumer);

      ArgumentCaptor<Iterable> iterableCaptor = ArgumentCaptor.forClass(Iterable.class);
      // If the batch count is greater than or equal to the input then we shouldn't have any intermediate
      // responses
      Mockito.verify(consumer, Mockito.atLeast(batchCount >= input.size() ? 0 : 1)).accept(iterableCaptor.capture());

      // Sort the input to verify our sorting worked
      Collections.sort(input);
      AtomicInteger count = new AtomicInteger();

      Consumer<Integer> consumerVerifier = i -> assertEquals(input.get(count.getAndIncrement()), i);

      List<Iterable> iterables = iterableCaptor.getAllValues();
      iterables.forEach(i -> i.forEach(consumerVerifier));

      lastIterable.forEach(consumerVerifier);
   }

   @Test(dataProvider = "limitNumbers")
   public void testLimitNumbers(Integer limit) {
      if (limit == 2) {
         System.currentTimeMillis();
      }
      int batchSize = 5;
      List<Integer> input = generateInts();
      List<Integer> streamInput = new ArrayList<>(input);

      SortedNoMapIterableOperation<Integer> op = new SortedNoMapIterableOperation(Collections.emptyList(),
              Collections.emptyList(), () -> streamInput.stream(), batchSize, null, (long) limit, null);

      Consumer<Iterable<Integer>> consumer = Mockito.mock(Consumer.class);
      Iterable<Integer> lastIterable = op.performOperation(consumer);

      ArgumentCaptor<Iterable> iterableCaptor = ArgumentCaptor.forClass(Iterable.class);
      // If the limit is greater than the batch size then we need at least 1 intermediate
      Mockito.verify(consumer, Mockito.atLeast(limit > batchSize ? 1 : 0)).accept(iterableCaptor.capture());

      // Sort the input to verify our sorting worked
      Collections.sort(input);
      AtomicInteger count = new AtomicInteger();

      Consumer<Integer> consumerVerifier = i -> assertEquals(input.get(count.getAndIncrement()), i);

      List<Iterable> iterables = iterableCaptor.getAllValues();
      iterables.forEach(i -> i.forEach(consumerVerifier));

      lastIterable.forEach(consumerVerifier);
      assertEquals(limit > input.size() ? input.size() : limit.intValue(), count.get());
   }
}
