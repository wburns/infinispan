package org.infinispan.stream.impl.termop.object;

import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.mockito.Mockito;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Test to verify that sorted results from different origins are properly returned
 */
@Test
public class SortedCoalescingResultsTest extends AbstractInfinispanTest {
   @DataProvider(name="addressCount")
   public static Object[][] createBatchNumbers() {
      return createNumbers(9, 2);
   }

   private static Object[][] createNumbers(int amount, int offset) {
      Object[][] data = new Object[amount][];
      for (int i = 0; i < amount; i++) {
         // Note the value is offset by 1.  This is because 0 based batch makes no sense
         data[i] = new Object[] { Integer.valueOf(i + offset) };
      }
      return data;
   }

   @Test(dataProvider = "addressCount")
   public void testVariousProducers(Integer addressCount) throws InterruptedException {
      int count = 53;
      int batchSize = 5;
      Set<Address> addresses = createUniqueAddresses(addressCount);

      int[] ints = IntStream.iterate(82, i -> i -= 3).limit(count).toArray();

      SortedCoalescingResults<Integer> res = new SortedCoalescingResults<>(addresses, batchSize, Comparator.reverseOrder());

      int offset = 0;
      for (Address address : addresses) {
         int localInt = offset++;
         fork(() -> {
            int amount = 0;
            Stream.Builder<Integer> builder = Stream.builder();
            for (int i = localInt; i < count; i+= addressCount) {
               builder.accept(ints[i]);
               if (++amount == batchSize) {
                  res.onIntermediateResult(address, builder.build()::iterator);
                  builder = Stream.builder();
                  amount = 0;
               }
            }

            // Signals that this iterable is done
            res.onCompletion(address, Collections.emptySet(), builder.build()::iterator);
         });
      }

      for (int value : ints) {
         if (value == -74) {
            System.currentTimeMillis();
         }
         assertEquals(value, res.getNextValue().intValue());
      }
   }

   private Set<Address> createUniqueAddresses(int count) {
      Set<Address> addressSet = new HashSet<>();
      for (int i = 0; i < count; ++i) {
         addressSet.add(Mockito.mock(Address.class));
      }
      return addressSet;
   }
}
