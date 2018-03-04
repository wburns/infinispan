package org.infinispan.container;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;

import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.metadata.Metadata;

/**
 * @author wburns
 * @since 9.3
 */
public interface SegmentedDataContainer<K, V> extends DataContainer<K, V> {
   InternalCacheEntry<K, V> get(int segment, Object k);

   InternalCacheEntry<K, V> peek(int segment, Object k);

   void put(int segment, K k, V v, Metadata metadata);

   boolean containsKey(int segment, Object k);

   InternalCacheEntry<K, V> remove(int segment, Object k);

   void evict(int segment, K key);

   InternalCacheEntry<K, V> compute(int segment, K key, ComputeAction<K, V> action);

   default int size(IntSet segments) {
      int size = 0;
      // We have to loop through to make sure to remove expired entries
      for (Iterator<InternalCacheEntry<K, V>> iter = iterator(segments); iter.hasNext(); ) {
         iter.next();
         if (++size == Integer.MAX_VALUE) return Integer.MAX_VALUE;
      }
      return size;
   }

   int sizeIncludingExpired(IntSet segments);

   void clear(IntSet segments);

   Spliterator<InternalCacheEntry<K, V>> spliterator(IntSet segments);

   Spliterator<InternalCacheEntry<K, V>> spliteratorIncludingExpired(IntSet segments);

   Iterator<InternalCacheEntry<K, V>> iterator(IntSet segments);

   Iterator<InternalCacheEntry<K, V>> iteratorIncludingExpired(IntSet segments);

   /**
    * Removes and un-associates the given segments. This will notify any listeners registered via
    * {@link #addRemovalListener(Consumer)}. There is no guarantee if the consumer is invoked once or multiple times
    * for a given group of segments and could be in any order
    * @param segments segments of the container to remove
    */
   void removeSegments(IntSet segments);

   void addRemovalListener(Consumer<Iterable<InternalCacheEntry<K, V>>> listener);

   void removeRemovalListener(Object listener);
}
