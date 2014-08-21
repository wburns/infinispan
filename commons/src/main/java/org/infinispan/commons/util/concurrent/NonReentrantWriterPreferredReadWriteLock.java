package org.infinispan.commons.util.concurrent;

import java.util.concurrent.Phaser;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * // TODO: Document this
 *
 * @author wburns
 * @since 7.0
 */
public class NonReentrantWriterPreferredReadWriteLock implements ReadWriteLock {
   private final Semaphore semaphore = new Semaphore(Integer.MAX_VALUE);

   // Only 1 writer can own the phaser at a time, Note that only the write lock holder should keep a
   // registered value for any duration as read/write should register and then immediately unregister
   // and await the phase to complete
   // Note no one should reference this phaser variable other that writers, readers should
   // only retrieve from the AtomicReference
   private final Phaser internalPhaser;
   private final AtomicReference<Phaser> phaserRef = new AtomicReference<>();

   public NonReentrantWriterPreferredReadWriteLock() {
      internalPhaser = new Phaser(1);
   }

   @Override
   public ReadLock readLock() {
      return new ReadLock();
   }

   @Override
   public WriteLock writeLock() {
      return new WriteLock();
   }

   int getAvailablePermits() {
      return semaphore.availablePermits();
   }

   class WriteLock implements Lock {

      private void releaseWriteLock(Phaser phaser, int permitCount) {
         semaphore.release(permitCount);
         // This will allow readers and writers that are waiting to wake up
         phaser.arrive();
         phaserRef.set(null);
      }

      @Override
      public void lock() {
         boolean acquired = false;
         while (!acquired) {
            Phaser currentPhaser = phaserRef.get();
            if (currentPhaser != null) {
               // This means another write has the lock!  So we have to wait for them to release it
               currentPhaser.awaitAdvance(currentPhaser.arriveAndDeregister());
            } else {
               // If we were able to set the value it means we can retrieve the lock, otherwise another
               // write lock has been acquired before us so we have to try again and wait on them if
               // possible before trying to acquire again
               if (phaserRef.compareAndSet(null, internalPhaser)) {
                  // We can finally get the lock now!
                  int totalPermits = semaphore.drainPermits();
                  while (totalPermits < Integer.MAX_VALUE) {
                     semaphore.acquireUninterruptibly();
                     totalPermits++;
                     // We do a drain just in case if a lot of reads unlocked at the same time
                     totalPermits += semaphore.drainPermits();
                  }
                  acquired = true;
               }
            }
         }
      }

      @Override
      public void lockInterruptibly() throws InterruptedException {
         boolean acquired = false;
         while (!acquired) {
            Phaser currentPhaser = phaserRef.get();
            if (currentPhaser != null) {
               currentPhaser.register();
               // This means another write has the lock!  So we have to wait for them to release it
               currentPhaser.awaitAdvance(currentPhaser.arriveAndDeregister());
            } else {
               // If we were able to set the value it means we can retrieve the lock, otherwise another
               // write lock has been acquired before us so we have to try again and wait on them if
               // possible before trying to acquire again
               if (phaserRef.compareAndSet(null, internalPhaser)) {
                  // We can finally get the lock now!
                  int totalPermits = semaphore.drainPermits();
                  try {
                     while (totalPermits < Integer.MAX_VALUE) {
                        semaphore.acquire();
                        totalPermits++;
                        // We do a drain just in case if a lot of reads unlocked at the same time
                        totalPermits += semaphore.drainPermits();
                     }
                  } finally {
                     // If we weren't able to acquire all of the permits it means we timed out or were
                     // interrupted so release the lock
                     if (totalPermits == Integer.MAX_VALUE) {
                        acquired = true;
                     } else {
                        releaseWriteLock(internalPhaser, totalPermits);
                     }
                  }
               }
            }
         }
      }

      @Override
      public boolean tryLock() {
         boolean acquired = false;
         Phaser currentPhaser = phaserRef.get();
         if (currentPhaser == null && phaserRef.compareAndSet(null, internalPhaser)) {
            int permits = semaphore.drainPermits();
            if (permits == Integer.MAX_VALUE) {
               acquired = true;
            } else {
               releaseWriteLock(currentPhaser, permits);
            }
         }
         return acquired;
      }

      @Override
      public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
         long targetTime = System.nanoTime() + unit.toNanos(time);
         boolean acquired = false;
         try {
            while (!acquired) {
               Phaser currentPhaser = phaserRef.get();
               if (currentPhaser != null) {
                  currentPhaser.register();
                  // This means another write has the lock!  So we have to wait for them to release it
                  currentPhaser.awaitAdvanceInterruptibly(currentPhaser.arriveAndDeregister(),
                                                          targetTime - System.nanoTime(), TimeUnit.NANOSECONDS);

               } else {
                  // If we were able to set the value it means we can retrieve the lock, otherwise another
                  // write lock has been acquired before us so we have to try again and wait on them if
                  // possible before trying to acquire again
                  if (phaserRef.compareAndSet(null, internalPhaser)) {
                     // We can finally get the lock now!
                     int totalPermits = semaphore.drainPermits();
                     try {
                        while (totalPermits < Integer.MAX_VALUE) {
                           semaphore.tryAcquire(targetTime - System.nanoTime(), TimeUnit.NANOSECONDS);
                           totalPermits++;
                           // We do a drain just in case if a lot of reads unlocked at the same time
                           totalPermits += semaphore.drainPermits();
                        }
                     } finally {
                        // If we weren't able to acquire all of the permits it means we timed out or were
                        // interrupted so release the lock
                        if (totalPermits == Integer.MAX_VALUE) {
                           acquired = true;
                        } else {
                           releaseWriteLock(internalPhaser, totalPermits);
                        }
                     }
                  }
               }
            }
         } catch (TimeoutException e) {
         }
         return acquired;
      }

      @Override
      public void unlock() {
         Phaser currentPhaser = phaserRef.get();
         // Simple check, this still doesn't verify this lock has it!
         if (currentPhaser == null) {
            throw new IllegalMonitorStateException();
         }
         releaseWriteLock(currentPhaser, Integer.MAX_VALUE);
      }

      @Override
      public Condition newCondition() {
         throw new UnsupportedOperationException("Conditions are not supported for NonReentrantWritePreferredReadWriteLock");
      }
   }

   class ReadLock implements Lock {

      @Override
      public void lock() {
         // Keep trying until we get a lock
         while (!semaphore.tryAcquire()) {
            // If we couldn't get a lock that means there is a writer happening
            Phaser phaser = phaserRef.get();
            if (phaser != null) {
               phaser.register();
               // We need to remove ourself as well to not block others
               phaser.awaitAdvance(phaser.arriveAndDeregister());
            }
         }
      }

      @Override
      public void lockInterruptibly() throws InterruptedException {
         // Keep trying until we get a lock
         while (!semaphore.tryAcquire()) {
            // If we couldn't get a lock that means there is a writer happening
            Phaser phaser = phaserRef.get();
            if (phaser != null) {
               phaser.register();
               // We need to remove ourself as well to not block others
               phaser.awaitAdvanceInterruptibly(phaser.arriveAndDeregister());
            }
         }
      }

      @Override
      public boolean tryLock() {
         return semaphore.tryAcquire();
      }

      @Override
      public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
         long targetTime = System.nanoTime() + unit.toNanos(time);
         // Keep trying until we get a lock
         while (!semaphore.tryAcquire()) {
            // If we couldn't get a lock that means there is a writer happening
            Phaser phaser = phaserRef.get();
            if (phaser != null) {
               phaser.register();
               // We need to remove ourself as well to not block others
               try {
                  phaser.awaitAdvanceInterruptibly(phaser.arriveAndDeregister(), targetTime - System.nanoTime(),
                                                   TimeUnit.NANOSECONDS);
               } catch (TimeoutException e) {
                  return false;
               }
            }
         }
         return true;
      }

      @Override
      public void unlock() {
         // We don't currently verify that we have the lock
         semaphore.release();
      }

      @Override
      public Condition newCondition() {
         throw new UnsupportedOperationException("Conditions are not supported for NonReentrantWritePreferredReadWriteLock");
      }
   }
}
