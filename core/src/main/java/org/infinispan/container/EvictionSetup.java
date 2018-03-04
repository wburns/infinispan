package org.infinispan.container;

import java.util.Collections;

import org.infinispan.commons.util.EvictionListener;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.util.concurrent.WithinThreadExecutor;

import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

/**
 * @author wburns
 * @since 9.3
 */
public class EvictionSetup {
   private EvictionSetup() { }

   public static <K, V> Caffeine<K, InternalCacheEntry<K, V>> applyListener(Caffeine<K, InternalCacheEntry<K, V>> caffeine,
         EvictionListener<K, InternalCacheEntry<K, V>> listener) {
      return caffeine.executor(new WithinThreadExecutor()).removalListener((k, v, c) -> {
         switch (c) {
            case SIZE:
               listener.onEntryEviction(Collections.singletonMap(k, v));
               break;
            case EXPLICIT:
               listener.onEntryRemoved(new ImmortalCacheEntry(k, v));
               break;
            case REPLACED:
               listener.onEntryActivated(k);
               break;
         }
      }).writer(new CacheWriter<K, InternalCacheEntry<K, V>>() {
         @Override
         public void write(K key, InternalCacheEntry<K, V> value) {

         }

         @Override
         public void delete(K key, InternalCacheEntry<K, V> value, RemovalCause cause) {
            if (cause == RemovalCause.SIZE) {
               listener.onEntryChosenForEviction(new ImmortalCacheEntry(key, value));
            }
         }
      });
   }

   public static <K, V> Caffeine<K, V> caffeineBuilder() {
      return (Caffeine<K, V>) Caffeine.newBuilder();
   }
}
