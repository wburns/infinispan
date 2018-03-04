package org.infinispan.container;

import java.util.Iterator;

import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.metadata.Metadata;
import org.reactivestreams.Publisher;

/**
 * @author wburns
 * @since 9.0
 */
public interface SegmentedDataContainer<K, V> extends DataContainer<K, V> {
   InternalCacheEntry<K, V> get(int segment, Object k);

   InternalCacheEntry<K, V> peek(int segment, Object k);

   void put(int segment, K k, V v, Metadata metadata);

   boolean containsKey(int segment, Object k);

   InternalCacheEntry<K, V> remove(int segment, Object k);

   int size(int segment);

   int sizeIncludingExpired(int segment);

   void clear(int segment);

   void evict(int segment, K key);

   InternalCacheEntry<K, V> compute(int segment, K key, ComputeAction<K, V> action);

   Iterator<InternalCacheEntry<K, V>> iterator(int segment);

   Iterator<InternalCacheEntry<K, V>> iteratorIncludingExpired(int segment);
}
