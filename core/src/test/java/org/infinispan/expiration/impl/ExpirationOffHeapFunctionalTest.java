package org.infinispan.expiration.impl;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "expiration.impl.ExpirationOffHeapFunctionalTest")
public class ExpirationOffHeapFunctionalTest extends ExpirationFunctionalTest {

   @Override
   protected void configure(ConfigurationBuilder config) {
      config
              // Prevent the reaper from running, reaperEnabled(false) doesn't work when a store is present
              .expiration().wakeUpInterval(Long.MAX_VALUE)
              .memory().storageType(StorageType.OFF_HEAP);
   }
}
