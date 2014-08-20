package org.infinispan.commons.util.concurrent;

import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;

import static org.testng.AssertJUnit.*;

@Test(groups = "functional", testName = "commons.util.NonReentrantWriterPreferredReadWriteLockTest")
public class NonReentrantWriterPreferredReadWriteLockTest {
   
   enum LockInvocation {
      LOCK {
         @Override
         boolean acquireLock(Lock lock) throws Exception {
            lock.lock();
            return true;
         }
      },
      LOCK_INTERRUPTIBLY {
         @Override
         boolean acquireLock(Lock lock) throws Exception {
            lock.lockInterruptibly();
            return true;
         }
      },
      TRY_LOCK {
         @Override
         boolean acquireLock(Lock lock) throws Exception {
            return lock.tryLock();
         }
      },
      TRY_LOCK_NEVER_TIMEOUT {
         @Override
         boolean acquireLock(Lock lock) throws Exception {
            return lock.tryLock(Long.MAX_VALUE, TimeUnit.DAYS);
         }
      },
      TRY_LOCK_TIMEOUT_AFTER_MISS {
         @Override
         boolean acquireLock(Lock lock) throws Exception {
            return lock.tryLock(0, TimeUnit.NANOSECONDS);
         }
      };

      abstract boolean acquireLock(Lock lock) throws Exception;
   }

   @Test
   public void testReadLock() throws Exception {
      testReadLock(LockInvocation.LOCK);
   }

   @Test
   public void testTryReadLock() throws Exception {
      testReadLock(LockInvocation.TRY_LOCK);
   }

   @Test
   public void testTryReadLockWithHighTimeout() throws Exception {
      testReadLock(LockInvocation.TRY_LOCK_NEVER_TIMEOUT);
   }

   @Test
   public void testTryReadLockWithLowTimeout() throws Exception {
      testReadLock(LockInvocation.TRY_LOCK_TIMEOUT_AFTER_MISS);
   }

   @Test
   public void testLockInterruptiblyReadLock() throws Exception {
      testReadLock(LockInvocation.LOCK_INTERRUPTIBLY);
   }
   
   private void testReadLock(LockInvocation invocation) throws Exception {
      NonReentrantWriterPreferredReadWriteLock lock = new NonReentrantWriterPreferredReadWriteLock();
      assertEquals(Integer.MAX_VALUE, lock.getAvailablePermits());
      Lock readLock = lock.readLock();
      invocation.acquireLock(readLock);
      assertEquals(Integer.MAX_VALUE - 1, lock.getAvailablePermits());
      Lock readLock2 = lock.readLock();
      invocation.acquireLock(readLock2);
      assertEquals(Integer.MAX_VALUE - 2, lock.getAvailablePermits());
      readLock2.unlock();
      assertEquals(Integer.MAX_VALUE - 1, lock.getAvailablePermits());
      readLock.unlock();
      assertEquals(Integer.MAX_VALUE, lock.getAvailablePermits());
   }

   @Test
   public void testWriteLock() throws Exception {
      testWriteLock(LockInvocation.LOCK);
   }

   @Test
   public void testTryWriteLock() throws Exception {
      testWriteLock(LockInvocation.TRY_LOCK);
   }

   @Test
   public void testTryWriteLockWithHighTimeout() throws Exception {
      testWriteLock(LockInvocation.TRY_LOCK_NEVER_TIMEOUT);
   }

   @Test
   public void testTryWriteLockWithLowTimeout() throws Exception {
      testWriteLock(LockInvocation.TRY_LOCK_TIMEOUT_AFTER_MISS);
   }

   @Test
   public void testLockInterruptiblyWriteLock() throws Exception {
      testWriteLock(LockInvocation.LOCK_INTERRUPTIBLY);
   }

   private void testWriteLock(LockInvocation invocation) throws Exception {
      NonReentrantWriterPreferredReadWriteLock lock = new NonReentrantWriterPreferredReadWriteLock();
      assertEquals(Integer.MAX_VALUE, lock.getAvailablePermits());
      Lock readLock = lock.writeLock();
      invocation.acquireLock(readLock);
      assertEquals(0, lock.getAvailablePermits());
      readLock.unlock();
      assertEquals(Integer.MAX_VALUE, lock.getAvailablePermits());
   }

   @Test
   public void testWriteLockBlockingAnotherWriteLock() throws Exception {
      testWriteLockBlockingAnotherWriteLock(LockInvocation.LOCK);
   }

   @Test
   public void testWriteLockBlockingAnotherTryWriteLock() throws Exception {
      testWriteLockBlockingAnotherWriteLock(LockInvocation.TRY_LOCK);
   }

   @Test
   public void testWriteLockBlockingAnotherWriteLockWithHighTimeout() throws Exception {
      testWriteLockBlockingAnotherWriteLock(LockInvocation.TRY_LOCK_NEVER_TIMEOUT);
   }

   @Test
   public void testWriteLockBlockingAnotherWriteLockWithLowTimeout() throws Exception {
      testWriteLockBlockingAnotherWriteLock(LockInvocation.TRY_LOCK_TIMEOUT_AFTER_MISS);
   }

   @Test
   public void testWriteLockBlockingAnotherWriteLockInterruptibly() throws Exception {
      testWriteLockBlockingAnotherWriteLock(LockInvocation.LOCK_INTERRUPTIBLY);
   }

   private void testWriteLockBlockingAnotherWriteLock(final LockInvocation invocation) throws Exception {
      NonReentrantWriterPreferredReadWriteLock lock = new NonReentrantWriterPreferredReadWriteLock();
      Lock writeLock = lock.writeLock();
      writeLock.lock();

      final Lock writeLock2 = lock.writeLock();

      Callable<Boolean> callable = new Callable<Boolean>() {
         @Override
         public Boolean call() throws Exception {
            return invocation.acquireLock(writeLock2);
         }
      };
      // Just run inline if it will return false immediately
      if (invocation == LockInvocation.TRY_LOCK || invocation == LockInvocation.TRY_LOCK_TIMEOUT_AFTER_MISS) {
         // Should not have been able to acquire lock
         assertFalse(callable.call());
         writeLock.unlock();
         assertTrue(callable.call());
      } else {
         ExecutorService es = Executors.newSingleThreadExecutor();
         try {
            Future<Boolean> future = es.submit(callable);
            try {
               future.get(100, TimeUnit.MILLISECONDS);
               fail("Expected a timeout exception!");
            } catch (TimeoutException e) {

            }
            // finally we unlock the original locker
            writeLock.unlock();
            assertTrue(future.get(2, TimeUnit.SECONDS));
         } finally {
            es.shutdownNow();
         }
      }

      assertEquals(0, lock.getAvailablePermits());
      writeLock2.unlock();
      assertEquals(Integer.MAX_VALUE, lock.getAvailablePermits());
   }
}