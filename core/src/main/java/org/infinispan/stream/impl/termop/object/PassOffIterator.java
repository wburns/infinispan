package org.infinispan.stream.impl.termop.object;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * // TODO: Document this
 *
 * @author wburns
 * @since 8.2
 */
public class PassOffIterator<C> implements CloseableIterator<C> {
   private static final Log log = LogFactory.getLog(PassOffIterator.class);
   private static final boolean trace = log.isTraceEnabled();

   private final Queue<C> queue;
   private final Lock nextLock;
   private final Condition nextCondition;
   private final TimeService timeService;
   private final long timeout;
   private final TimeUnit unit;

   private boolean completed;
   private CacheException exception;

   public PassOffIterator(Queue<C> queue, Lock lock, Condition condition, TimeService timeService, long timeout,
                          TimeUnit unit) {
      this.queue = queue;
      this.nextLock = lock;
      this.nextCondition = condition;
      this.timeService = timeService;
      this.timeout = timeout;
      this.unit = unit;
   }

   @Override
   public boolean hasNext() {
      boolean hasNext = !queue.isEmpty();
      if (!hasNext) {
         boolean interrupted = false;
         long targetTime = timeService.expectedEndTime(timeout, unit);
         nextLock.lock();
         try {
            while (!(hasNext = !queue.isEmpty()) && !completed) {
               try {
                  if (!nextCondition.await(timeService.remainingTime(targetTime, TimeUnit.NANOSECONDS),
                          TimeUnit.NANOSECONDS)) {
                     if (trace) {
                        log.tracef("Did not retrieve entries in allotted timeout: %s units: unit", timeout, unit);
                     }
                     throw new TimeoutException("Did not retrieve entries in allotted timeout: " + timeout +
                             " units: " + unit);
                  }
               } catch (InterruptedException e) {
                  // If interrupted, we just loop back around
                  interrupted = true;
               }
            }
            if (!hasNext && exception != null) {
               throw exception;
            }
         } finally {
            nextLock.unlock();
         }

         if (interrupted) {
            Thread.currentThread().interrupt();
         }
      }
      return hasNext;
   }

   @Override
   public C next() {
      C entry = queue.poll();
      if (entry == null) {
         if (completed) {
            if (exception != null) {
               throw exception;
            }
            throw new NoSuchElementException();
         }
         nextLock.lock();
         try {
            while ((entry = queue.poll()) == null && !completed) {
               try {
                  nextCondition.await();
               } catch (InterruptedException e) {
                  // If interrupted, we just loop back around
               }
            }
            if (entry == null) {
               if (exception != null) {
                  throw exception;
               }
               throw new NoSuchElementException();
            }
         } finally {
            nextLock.unlock();
         }
      }
      return entry;
   }

   @Override
   public void close() {
      close(null);
   }

   protected void close(CacheException e) {
      nextLock.lock();
      try {
         // We only set the exception if we weren't completed prior - which will allow for this iterator to complete
         // since it retrieved all the other values
         if (!completed) {
            exception = e;
            completed = true;
         }
         nextCondition.signalAll();
      } finally {
         nextLock.unlock();
      }
   }
}
