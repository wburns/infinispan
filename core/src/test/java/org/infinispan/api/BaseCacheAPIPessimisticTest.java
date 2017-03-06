package org.infinispan.api;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.LockedStream;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author wburns
 * @since 9.1
 */
public abstract class BaseCacheAPIPessimisticTest extends CacheAPITest {
   @Override
   protected void amend(ConfigurationBuilder cb) {
      cb.transaction().lockingMode(LockingMode.PESSIMISTIC);
   }

   /**
    * Tests to make sure that locked stream works properly when another user has the lock for a given key
    */
   public void testLockedStreamBlocked() throws InterruptedException, TimeoutException, BrokenBarrierException, ExecutionException {
      for (int i = 0; i < 10; i++) {
         cache.put(i, "value" + i);
      }

      CyclicBarrier barrier = new CyclicBarrier(2);

      int key = 4;

      Future<Object> putFuture = fork(() -> {
         TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
         try {
            tm.begin();
            try {
               Object prev = cache.put(key, "value" + key + "-new");
               // Wait for main thread to get to same point
               barrier.await(10, TimeUnit.SECONDS);
               // Main thread lets us complete
               barrier.await(10, TimeUnit.SECONDS);
               // Commit the change
               tm.commit();
               return prev;
            } finally {
               if (tm.getStatus() == Status.STATUS_ACTIVE) {
                  tm.rollback();
               }
            }
         } catch (NotSupportedException | SystemException e) {
            throw new RuntimeException(e);
         }
      });

      // Wait until fork thread has alredy locked the key
      barrier.await(10, TimeUnit.SECONDS);

      LockedStream<Object, Object> stream = cache.getAdvancedCache().lockedStream();
      Future<?> forEachFuture = fork(() -> stream.filter(e -> e.getKey().equals(key)).forEach((c, e) ->
            assertEquals("value" + key + "-new", c.put(e.getKey(), String.valueOf(e.getValue() + "-other")))));

      try {
         forEachFuture.get(50, TimeUnit.MILLISECONDS);
         fail("Test should have thrown TimeoutException");
      } catch (TimeoutException e) {

      }

      // Let the tx put complete
      barrier.await(10, TimeUnit.SECONDS);

      forEachFuture.get(10, TimeUnit.MINUTES);

      // The put should replace the value that forEach inserted
      assertEquals("value" + key, putFuture.get(10, TimeUnit.SECONDS));
      // The put should be last since it had to wait until lock was released on forEachWithLock
      assertEquals("value" + key + "-new-other", cache.get(key));

      // Make sure the locks were cleaned up properly
      LockManager lockManager = cache.getAdvancedCache().getComponentRegistry().getComponent(LockManager.class);
      assertEquals(0, lockManager.getNumberOfLocksHeld());
   }

   @DataProvider(name = "testLockedStreamInTx")
   public Object[][] testLockedStreamInTxProvider() {
      return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE} };
   }

   @Test(dataProvider = "testLockedStreamInTx")
   public void testLockedStreamInTxCommit(Boolean shouldCommit) throws SystemException, NotSupportedException,
         HeuristicRollbackException, HeuristicMixedException, RollbackException {
      for (int i = 0; i < 5; i++) {
         cache.put(i, "value" + i);
      }

      TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
      tm.begin();
      try {
         cache.getAdvancedCache().lockedStream().forEach((c, e) -> c.put(e.getKey(), e.getValue() + "-changed"));
         if (shouldCommit) {
            tm.commit();
         } else {
            tm.rollback();
         }
      } finally {
         if (tm.getStatus() == Status.STATUS_ACTIVE) {
            tm.rollback();
         }
      }

      for (int i = 0; i < 5; i++) {
         assertEquals("value" + i + "-changed", cache.get(i));
      }
   }

   public void testLockedStreamTxInsideConsumer() {
      for (int i = 0; i < 5; i++) {
         cache.put(i, "value" + i);
      }

      cache.getAdvancedCache().lockedStream().forEach((c, e) -> {
         TransactionManager tm = c.getAdvancedCache().getTransactionManager();
         try {
            tm.begin();
            c.put(e.getKey(), e.getValue() + "-changed");
            tm.commit();
         } catch (Exception e1) {
            throw new RuntimeException(e1);
         }
      });

      for (int i = 0; i < 5; i++) {
         assertEquals("value" + i + "-changed", cache.get(i));
      }
   }
}
