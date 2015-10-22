package org.infinispan.stream.impl.termop.object;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
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
public class SortedNoMapIteratorOperationTest {
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

   public void simpleTest() {
      List<Integer> input = generateInts();
      List<Integer> streamInput = new ArrayList<>(input);
      SortedNoMapIteratorOperation<Integer> op = new SortedNoMapIteratorOperation(Collections.emptyList(),
              Collections.emptyList(), () -> streamInput.stream(), 4, null, null, null);

      Consumer<Iterable<Integer>> consumer = Mockito.mock(Consumer.class);
      Iterable<Integer> lastIterable = op.performOperation(consumer);

      ArgumentCaptor<Iterable> iterableCaptor = ArgumentCaptor.forClass(Iterable.class);
      Mockito.verify(consumer, Mockito.times(2)).accept(iterableCaptor.capture());

      // Sort the input to verify our sorting worked
      Collections.sort(input);
      AtomicInteger count = new AtomicInteger();

      Consumer<Integer> consumerVerifier = i -> {
         assertEquals(input.get(count.getAndIncrement()), i);
      };

      List<Iterable> iterables = iterableCaptor.getAllValues();
      iterables.forEach(i -> i.forEach(consumerVerifier));

      lastIterable.forEach(consumerVerifier);
   }
}
