package org.infinispan.counter.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.Handle;
import org.infinispan.counter.api.SyncWeakCounter;
import org.infinispan.counter.api.WeakCounter;

public class InitializingWeakCounter implements WeakCounter {
   private final String name;
   private final CompletionStage<CounterConfiguration> configuration;
   private final CompletionStage<WeakCounter> initializingCounter;

   public InitializingWeakCounter(String name, CompletionStage<CounterConfiguration> configuration,
         CompletionStage<WeakCounter> initializingCounter) {
      this.name = name;
      this.configuration = configuration;
      this.initializingCounter = initializingCounter;
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public long getValue() {
      return initializingCounter.toCompletableFuture().join().getValue();
   }

   @Override
   public CompletionStage<Long> getValueAsync() {
      return initializingCounter.thenCompose(WeakCounter::getValueAsync);
   }

   @Override
   public CompletableFuture<Void> increment() {
      return initializingCounter.thenCompose(WeakCounter::increment).toCompletableFuture();
   }

   @Override
   public CompletableFuture<Void> decrement() {
      return initializingCounter.thenCompose(WeakCounter::decrement).toCompletableFuture();
   }

   @Override
   public CompletableFuture<Void> add(long delta) {
      return initializingCounter.thenCompose(wc -> wc.add(delta)).toCompletableFuture();
   }

   @Override
   public CompletableFuture<Void> reset() {
      return initializingCounter.thenCompose(WeakCounter::reset).toCompletableFuture();
   }

   @Override
   public <T extends CounterListener> Handle<T> addListener(T listener) {
      return initializingCounter.toCompletableFuture().join().addListener(listener);
   }

   @Override
   public CounterConfiguration getConfiguration() {
      return configuration.toCompletableFuture().join();
   }

   @Override
   public CompletableFuture<Void> remove() {
      return initializingCounter.thenCompose(WeakCounter::remove).toCompletableFuture();
   }

   @Override
   public SyncWeakCounter sync() {
      return initializingCounter.toCompletableFuture().join().sync();
   }
}
