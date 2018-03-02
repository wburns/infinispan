package org.infinispan.persistence.spi;

import java.util.concurrent.Executor;
import java.util.function.Predicate;

import org.infinispan.marshall.core.MarshalledEntry;
import org.reactivestreams.Publisher;

import net.jcip.annotations.ThreadSafe;

/**
 * @author wburns
 * @since 9.3
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

   Publisher<K> publishKeys(int segment, Predicate<? super K> filter);

   Publisher<MarshalledEntry<K, V>> publishEntries(int segment, Predicate<? super K> filter, boolean fetchValue,
         boolean fetchMetadata);

   // AdvancedCacheWriter methods
   void clear(int segment);

   void purge(int segment, Executor threadPool, PurgeListener<? super K> listener);
}
