package org.infinispan.query.impl.massindex;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;

import org.infinispan.util.concurrent.CompletableFutures;

/**
 * A lock to prevent multiple {@link org.infinispan.query.MassIndexer} in non-clustered environments.
 * @since 10.1
 */
final class LocalMassIndexerLock implements MassIndexLock {

   private final Semaphore lock = new Semaphore(1);

   @Override
   public CompletionStage<Boolean> lock() {
      return CompletableFutures.booleanStage(lock.tryAcquire());
   }

   @Override
   public CompletionStage<Void> unlock() {
      lock.release();
      return CompletableFutures.completedNull();
   }
}
