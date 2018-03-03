package org.infinispan.persistence.support;

import java.util.concurrent.Executor;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.spi.SegmentedAdvancedLoadWriteStore;
import org.reactivestreams.Publisher;

/**
 * Abstract segment loader writer that implements all the non segmented methods and throws an exception if they are
 * invoked - which means it is a bug! These methods are also all declared final as to make sure the end user does not
 * implement the incorrect method.
 * @author wburns
 * @since 9.3
 */
public abstract class AbstractSegmentedAdvancedLoadWriteStore<K, V> implements SegmentedAdvancedLoadWriteStore<K, V> {
   public ToIntFunction<Object> getKeyMapper() {
      throw new UnsupportedOperationException();
   }

   @Override
   public final MarshalledEntry<K, V> load(Object key) {
      return load(getKeyMapper().applyAsInt(key), key);
   }

   @Override
   public final boolean contains(Object key) {
      throw new UnsupportedOperationException();
   }

   @Override
   public final void write(MarshalledEntry<? extends K, ? extends V> entry) {
      write(getKeyMapper().applyAsInt(entry.getKey()), entry);
   }

   @Override
   public final boolean delete(Object key) {
      throw new UnsupportedOperationException();
   }

   @Override
   public int size() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Publisher<K> publishKeys(Predicate<? super K> filter) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Publisher<MarshalledEntry<K, V>> publishEntries(Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void purge(Executor threadPool, PurgeListener<? super K> listener) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void clear() {
      throw new UnsupportedOperationException();
   }

   @Override
   public abstract void deleteBatch(Iterable<Object> keys);

   @Override
   public abstract void writeBatch(Iterable<MarshalledEntry<? extends K, ? extends V>> marshalledEntries);
}
