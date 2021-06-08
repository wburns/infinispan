package org.infinispan.persistence.sql;

import java.util.concurrent.CompletionStage;

import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.NonBlockingStore;

public class SQLStore<K, V> implements NonBlockingStore<K, V> {
   @Override
   public CompletionStage<Void> start(InitializationContext ctx) {
      return null;
   }

   @Override
   public CompletionStage<Void> stop() {
      return null;
   }

   @Override
   public CompletionStage<MarshallableEntry<K, V>> load(int segment, Object key) {
      return null;
   }

   @Override
   public CompletionStage<Void> write(int segment, MarshallableEntry<? extends K, ? extends V> entry) {
      return null;
   }

   @Override
   public CompletionStage<Boolean> delete(int segment, Object key) {
      return null;
   }

   @Override
   public CompletionStage<Void> clear() {
      return null;
   }
}
