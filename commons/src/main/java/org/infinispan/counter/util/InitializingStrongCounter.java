package org.infinispan.counter.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.Handle;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.SyncStrongCounter;

public class InitializingStrongCounter implements StrongCounter {
   private final String name;
   private final CompletionStage<CounterConfiguration> configuration;
   private final CompletionStage<StrongCounter> initializingCounter;

   public InitializingStrongCounter(String name, CompletionStage<CounterConfiguration> configuration,
         CompletionStage<StrongCounter> initializingCounter) {
      this.name = name;
      this.configuration = configuration;
      this.initializingCounter = initializingCounter;
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public CompletableFuture<Long> getValue() {
      return initializingCounter.thenCompose(StrongCounter::getValue).toCompletableFuture();
   }

   @Override
   public CompletableFuture<Long> incrementAndGet() {
      return initializingCounter.thenCompose(StrongCounter::incrementAndGet).toCompletableFuture();
   }

   @Override
   public CompletableFuture<Long> decrementAndGet() {
      return initializingCounter.thenCompose(StrongCounter::decrementAndGet).toCompletableFuture();
   }

   @Override
   public CompletableFuture<Long> addAndGet(long delta) {
      return initializingCounter.thenCompose(sc -> sc.addAndGet(delta)).toCompletableFuture();
   }

   @Override
   public CompletableFuture<Void> reset() {
      return initializingCounter.thenCompose(StrongCounter::reset).toCompletableFuture();
   }

   @Override
   public <T extends CounterListener> Handle<T> addListener(T listener) {
      return initializingCounter.toCompletableFuture().join().addListener(listener);
   }

   @Override
   public CompletableFuture<Boolean> compareAndSet(long expect, long update) {
      return initializingCounter.thenCompose(sc -> sc.compareAndSet(expect, update)).toCompletableFuture();
   }

   @Override
   public CompletableFuture<Long> compareAndSwap(long expect, long update) {
      return initializingCounter.thenCompose(sc -> sc.compareAndSwap(expect, update)).toCompletableFuture();
   }

   @Override
   public CounterConfiguration getConfiguration() {
      return configuration.toCompletableFuture().join();
   }

   @Override
   public CompletableFuture<Void> remove() {
      return initializingCounter.thenCompose(StrongCounter::remove).toCompletableFuture();
   }

   @Override
   public SyncStrongCounter sync() {
      return initializingCounter.toCompletableFuture().join().sync();
   }
}
