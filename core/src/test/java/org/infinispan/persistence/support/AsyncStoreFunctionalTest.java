package org.infinispan.persistence.support;

import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

/**
 * Functional tests of the async store when running associated with a cache instance.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(groups = "functional", testName = "persistence.decorators.AsyncStoreFunctionalTest")
public class AsyncStoreFunctionalTest extends AbstractInfinispanTest {

   // TODO: Async store needs to be repurposed
//   private static final Log log = LogFactory.getLog(AsyncStoreFunctionalTest.class);
//
//   public void testPutAfterPassivation() {
//      GlobalConfigurationBuilder globalBuilder = globalBuilderWithCustomPersistenceManager();
//      ConfigurationBuilder builder = asyncStoreWithEvictionBuilder();
//      builder.persistence().passivation(true);
//
//      withCacheManager(new CacheManagerCallable(
//            TestCacheManagerFactory.createCacheManager(globalBuilder, builder)) {
//         @Override
//         public void call() {
//            Cache<Integer, String> cache = cm.getCache();
//
//            MockAsyncCacheWriter cacheStore = TestingUtil.getFirstWriter(cache);
//            CountDownLatch modApplyLatch = cacheStore.modApplyLatch;
//            CountDownLatch lockedWaitLatch = cacheStore.lockedWaitLatch;
//
//            // Store an entry in the cache
//            cache.put(1, "v1");
//            // Store a second entry to force the previous entry
//            // to be evicted and passivated
//            cache.put(2, "v2");
//
//            try {
//               // Wait for async store to have this modification queued up,
//               // ready to apply it to the cache store...
//               log.trace("Wait for async store to lock keys");
//               lockedWaitLatch.await(60, TimeUnit.SECONDS);
//            } catch (InterruptedException e) {
//               Thread.currentThread().interrupt();
//            }
//
//            try {
//               // Even though it's in the process of being passivated,
//               // the entry should still be found in memory
//               assertEquals("v1", cache.get(1));
//            } finally {
//               modApplyLatch.countDown();
//            }
//         }
//      });
//   }
//
//   private GlobalConfigurationBuilder globalBuilderWithCustomPersistenceManager() {
//      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder();
//      globalBuilder.defaultCacheName("cache");
//      globalBuilder.addModule(TestGlobalConfigurationBuilder.class)
//                   .testCacheComponent("cache", PersistenceManager.class.getName(),
//                                       new PassivationPersistenceManager(new CustomPersistenceManager()));
//      return globalBuilder;
//   }
//
//   public void testPutAfterEviction() {
//      GlobalConfigurationBuilder globalBuilder = globalBuilderWithCustomPersistenceManager();
//      ConfigurationBuilder builder = asyncStoreWithEvictionBuilder();
//
//      withCacheManager(new CacheManagerCallable(
//            TestCacheManagerFactory.createCacheManager(globalBuilder, builder)) {
//         @Override
//         public void call() {
//            Cache<Integer, String> cache = cm.getCache();
//
//            MockAsyncCacheWriter cacheStore = TestingUtil.getFirstWriter(cache);
//            CountDownLatch modApplyLatch = cacheStore.modApplyLatch;
//            CountDownLatch lockedWaitLatch = cacheStore.lockedWaitLatch;
//
//            // Store an entry in the cache
//            cache.put(1, "v1");
//
//            try {
//               // Wait for async store to have this modification queued up,
//               // ready to apply it to the cache store...
//               log.trace("Wait for async store to lock keys");
//               lockedWaitLatch.await(60, TimeUnit.SECONDS);
//            } catch (InterruptedException e) {
//               Thread.currentThread().interrupt();
//            }
//
//            // This shouldn't result in k=1 being evicted
//            // because the k=1 put is queued in the async store
//            cache.put(2, "v2");
//
//            try {
//               assertEquals("v1", cache.get(1));
//               assertEquals("v2", cache.get(2));
//            } finally {
//               modApplyLatch.countDown();
//            }
//         }
//      });
//   }
//
//   public void testGetAfterRemove() throws Exception {
//      GlobalConfigurationBuilder globalBuilder = globalBuilderWithCustomPersistenceManager();
//      ConfigurationBuilder builder = new ConfigurationBuilder();
//      builder.persistence()
//               .addStore(DummyInMemoryStoreConfigurationBuilder.class)
//               .async().enabled(true);
//
//      withCacheManager(new CacheManagerCallable(
//            TestCacheManagerFactory.createCacheManager(globalBuilder, builder)) {
//         @Override
//         public void call() {
//            Cache<Integer, String> cache = cm.getCache();
//
//            MockAsyncCacheWriter cacheStore = TestingUtil.getFirstWriter(cache);
//            CountDownLatch modApplyLatch = cacheStore.modApplyLatch;
//            CountDownLatch lockedWaitLatch = cacheStore.lockedWaitLatch;
//
//            // Store a value first
//            cache.put(1, "skip");
//
//            // Wait until cache store contains the expected key/value pair
//            ((DummyInMemoryStore) cacheStore.undelegate())
//                  .blockUntilCacheStoreContains(1, "skip", 60000);
//
//            // Remove it from the cache container
//            cache.remove(1);
//
//            try {
//               // Wait for async store to have this modification queued up,
//               // ready to apply it to the cache store...
//               lockedWaitLatch.await(60, TimeUnit.SECONDS);
//            } catch (InterruptedException e) {
//               Thread.currentThread().interrupt();
//            }
//
//            try {
//               // Even though the remove it's pending,
//               // the entry should not be retrieved
//               assertEquals(null, cache.get(1));
//            } finally {
//               modApplyLatch.countDown();
//            }
//
//            ExpirationManager expirationManager = cache.getAdvancedCache().getExpirationManager();
//            expirationManager.processExpiration();
//
//            Set<Integer> keys = cache.keySet();
//            assertTrue("Keys not empty: " + keys, keys.isEmpty());
//            Set<Map.Entry<Integer, String>> entries = cache.entrySet();
//            assertTrue("Entry set not empty: " + entries, entries.isEmpty());
//            Collection<String> values = cache.values();
//            assertTrue("Values not empty: " + values, values.isEmpty());
//         }
//      });
//   }
//
//   public void testClear() {
//      ConfigurationBuilder builder = new ConfigurationBuilder();
//      builder.persistence()
//            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
//            .async().enabled(true);
//
//      withCacheManager(new CacheManagerCallable(
//            TestCacheManagerFactory.createCacheManager(builder)) {
//         @Override
//         public void call() {
//            Cache<Integer, String> cache = cm.getCache();
//            AdvancedAsyncCacheWriter asyncStore = TestingUtil.getFirstWriter(cache);
//            DummyInMemoryStore dummyStore = TestingUtil.extractField(asyncStore, "actual");
//            cache.put(1, "uno");
//            cache.put(2, "dos");
//            cache.put(3, "tres");
//            eventually(new Condition() {
//               @Override
//               public boolean isSatisfied() throws Exception {
//                  return dummyStore.size() == 3;
//               }
//            });
//            cache.clear();
//            eventually(new Condition() {
//               @Override
//               public boolean isSatisfied() throws Exception {
//                  return dummyStore.size() == 0;
//               }
//            });
//         }
//      });
//   }
//
//   /**
//    * A test for asynchronous write behind file cache store (passivation true, eviction on). Verifies
//    * https://bugzilla.redhat.com/show_bug.cgi?id=862594
//    */
//   public void testPutRemove() {
//      GlobalConfigurationBuilder globalBuilder = globalBuilderWithCustomPersistenceManager();
//      ConfigurationBuilder builder = asyncStoreWithEvictionBuilder();
//      builder.memory().size(1000L);
//
//      withCacheManager(new CacheManagerCallable(
//            TestCacheManagerFactory.createCacheManager(globalBuilder, builder)) {
//         @Override
//         public void call() {
//
//            Cache<String, String> cache = cm.getCache();
//            MockAsyncCacheWriter cacheStore = TestingUtil.getFirstWriter(cache);
//            DummyInMemoryStore dummyStore = (DummyInMemoryStore) cacheStore.undelegate();
//            CountDownLatch lockedWaitLatch = cacheStore.lockedWaitLatch;
//
//            int number = 200;
//            String key = "key";
//            String value = "value";
//
//            for (int i = 0; i < number; i++) {
//               cache.put(key + i, value + i);
//            }
//
//            for (int i = 0; i < number; i++) {
//               String entry = cache.get((key + i));
//               dummyStore.blockUntilCacheStoreContains(key + i, value + i, 60000);
//               assertEquals(value + i, entry);
//            }
//
//            for (int i = 0; i < 200; i++) {
//               cache.remove(key + i);
//            }
//            for (int i = 0; i < number; i++) {
//               MarshallableEntry entry = dummyStore.loadEntry(key + i);
//               while (entry != null) {
//
//                  try {
//                     log.trace("Wait for async store to lock keys");
//                     lockedWaitLatch.await(60, TimeUnit.SECONDS);
//                     entry = dummyStore.loadEntry(key + i);
//                  } catch (InterruptedException e) {
//                     Thread.currentThread().interrupt();
//                  }
//               }
//            }
//         }
//      });
//   }
//
//   private ConfigurationBuilder asyncStoreWithEvictionBuilder() {
//      ConfigurationBuilder builder = new ConfigurationBuilder();
//      // Emulate eviction with direct data container eviction
//      builder.memory().size(1L)
//            .persistence()
//            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
//            .async().enabled(true);
//      return builder;
//   }
//
//   public static class MockAsyncCacheWriter extends AsyncCacheWriter {
//
//      private static final Log log = LogFactory.getLog(MockAsyncCacheWriter.class);
//
//      private final CountDownLatch modApplyLatch;
//      private final CountDownLatch lockedWaitLatch;
//
//      public MockAsyncCacheWriter(CountDownLatch modApplyLatch, CountDownLatch lockedWaitLatch,
//                                  CacheWriter delegate) {
//         super(delegate);
//         this.modApplyLatch = modApplyLatch;
//         this.lockedWaitLatch = lockedWaitLatch;
//      }
//
//      @Override
//      protected void applyModificationsSync(List<Modification> mods)
//            throws PersistenceException {
//         try {
//            // Wait for signal to do the modification
//            if (containsModificationForKey(1, mods) && !isSkip(findModificationForKey(1, mods))) {
//               log.tracef("Wait to apply modifications: %s", mods);
//               lockedWaitLatch.countDown();
//               modApplyLatch.await(60, TimeUnit.SECONDS);
//               log.tracef("Apply modifications: %s", mods);
//            }
//            super.applyModificationsSync(mods);
//         } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//         }
//      }
//
//      private boolean containsModificationForKey(Object key, List<Modification> mods) {
//         return findModificationForKey(key, mods) != null;
//      }
//
//      private Modification findModificationForKey(Object key, List<Modification> mods) {
//         for (Modification modification : mods) {
//            switch (modification.getType()) {
//               case STORE:
//                  Store store = (Store) modification;
//                  if (store.getKey().equals(key))
//                     return store;
//                  break;
//               case REMOVE:
//                  Remove remove = (Remove) modification;
//                  if (remove.getKey().equals(key))
//                     return remove;
//                  break;
//               default:
//                  return null;
//            }
//         }
//         return null;
//      }
//
//      private boolean isSkip(Modification mod) {
//         if (mod instanceof Store) {
//            MarshallableEntry storedValue = ((Store) mod).getStoredValue();
//            return storedValue.getValue().equals("skip");
//         }
//         return false;
//      }
//
//   }
//
//   public static class CustomPersistenceManager extends PersistenceManagerImpl {
//
//      @Override
//      protected AsyncCacheWriter createAsyncWriter(CacheWriter tmpStore) {
//         CountDownLatch modApplyLatch = new CountDownLatch(1);
//         CountDownLatch lockedWaitLatch = new CountDownLatch(1);
//         return new MockAsyncCacheWriter(modApplyLatch, lockedWaitLatch, tmpStore);
//      }
//   }

}
