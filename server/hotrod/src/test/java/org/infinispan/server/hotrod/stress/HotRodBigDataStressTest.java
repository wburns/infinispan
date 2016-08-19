package org.infinispan.server.hotrod.stress;

import java.lang.reflect.Method;

import org.infinispan.server.hotrod.HotRodSingleNodeTest;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.testng.annotations.Test;

/**
 * A simple test that stresses Hot Rod by storing big data and waits to allow
 * the test runner to generate heap dumps for the test.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = "profiling", testName = "server.hotrod.stress.HotRodBigDataStressTest")
class HotRodBigDataStressTest extends HotRodSingleNodeTest {

   public void testPutBigSizeValue(Method m) {
      byte[] value = HotRodTestingUtil.generateRandomString(10 * 1024 * 1024).getBytes();
      assertStatus(client.put(k(m), 0, 0, value), Success);
      while (true)
         Thread.sleep(5000);
   }

}
