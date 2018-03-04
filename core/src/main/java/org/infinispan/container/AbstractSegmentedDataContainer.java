package org.infinispan.container;

import java.util.Iterator;
import java.util.function.ToIntFunction;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.metadata.Metadata;

/**
 * @author wburns
 * @since 9.0
 */
public abstract class AbstractSegmentedDataContainer<K, V> implements SegmentedDataContainer<K, V> {

   protected abstract ToIntFunction<Object> segmentMapper();

   @Override
   public InternalCacheEntry<K, V> get(Object k) {
      return get(segmentMapper().applyAsInt(k), k);
   }

   @Override
   public InternalCacheEntry<K, V> peek(Object k) {
      return get(segmentMapper().applyAsInt(k), k);
   }

   @Override
   public void put(K k, V v, Metadata metadata) {
      put(segmentMapper().applyAsInt(k), k, v, metadata);
   }

   @Override
   public boolean containsKey(Object k) {
      return containsKey(segmentMapper().applyAsInt(k), k);
   }

   @Override
   public InternalCacheEntry<K, V> remove(Object k) {
      return remove(segmentMapper().applyAsInt(k), k);
   }

   @Override
   public void evict(K key) {
      evict(segmentMapper().applyAsInt(key), key);
   }

   @Override
   public InternalCacheEntry<K, V> compute(K key, ComputeAction<K, V> action) {
      return compute(segmentMapper().applyAsInt(key), key, action);
   }
}
