package org.infinispan.commons.util.concurrent;

import org.testng.annotations.Test;

import java.util.concurrent.locks.Lock;

import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "functional", testName = "commons.util.NonReentrantWriterPreferredReadWriteLockTest")
public class NonReentrantWriterPreferredReadWriteLockTest {
   @Test
   public void testReadLock() {
      NonReentrantWriterPreferredReadWriteLock lock = new NonReentrantWriterPreferredReadWriteLock();
      assertEquals(Integer.MAX_VALUE, lock.getAvailablePermits());
      Lock readLock = lock.readLock();
      readLock.lock();
      assertEquals(Integer.MAX_VALUE - 1, lock.getAvailablePermits());
      Lock readLock2 = lock.readLock();
      readLock.lock();
      assertEquals(Integer.MAX_VALUE - 2, lock.getAvailablePermits());
      readLock2.unlock();
      assertEquals(Integer.MAX_VALUE - 1, lock.getAvailablePermits());
      readLock.unlock();
      assertEquals(Integer.MAX_VALUE, lock.getAvailablePermits());
   }

   @Test
   public void testWriteLock() {
      NonReentrantWriterPreferredReadWriteLock lock = new NonReentrantWriterPreferredReadWriteLock();
      assertEquals(Integer.MAX_VALUE, lock.getAvailablePermits());
      Lock writeLock = lock.writeLock();
      writeLock.lock();
      assertEquals(0, lock.getAvailablePermits());
      writeLock.unlock();
      assertEquals(Integer.MAX_VALUE, lock.getAvailablePermits());
   }
}