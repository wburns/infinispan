package org.infinispan.persistence.spi;

import java.util.concurrent.Executor;
import java.util.function.Predicate;

import org.infinispan.commons.util.IntSet;
import org.infinispan.marshall.core.MarshalledEntry;
import org.reactivestreams.Publisher;

import net.jcip.annotations.ThreadSafe;

/**
 * @author wburns
 * @since 9.4
 */
@ThreadSafe
public interface SegmentedAdvancedLoadWriteStore<K, V> extends AdvancedLoadWriteStore<K, V> {
   // CacheLoader methods
   MarshalledEntry<K, V> load(int segment, Object key);

   boolean contains(int segment, Object key);

   // CacheWriter methods
   void write(int segment, MarshalledEntry<? extends K, ? extends V> entry);

   boolean delete(int segment, Object key);

   // AdvancedCacheLoader methods
   int size(int segment);

   Publisher<K> publishKeys(IntSet segments, Predicate<? super K> filter);

   Publisher<MarshalledEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean fetchValue,
         boolean fetchMetadata);

   // AdvancedCacheWriter methods
   void clear(IntSet segments);

   void purge(IntSet segments, Executor threadPool, PurgeListener<? super K> listener);

   /**
    * @param segments segments to associate with this store
    */
   default void addSegments(IntSet segments) { }

   /**
    * @param segments segments that should no longer be associated with this store
    */
   default void removeSegments(IntSet segments) { }
}
