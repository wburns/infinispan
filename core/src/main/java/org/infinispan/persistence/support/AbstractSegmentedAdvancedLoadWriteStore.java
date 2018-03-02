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
   @Override
   public final MarshalledEntry<K, V> load(Object key) {
      throw new UnsupportedOperationException();
   }

   @Override
   public final boolean contains(Object key) {
      throw new UnsupportedOperationException();
   }

   @Override
   public final void write(MarshalledEntry<? extends K, ? extends V> entry) {
      throw new UnsupportedOperationException();
   }

   @Override
   public final boolean delete(Object key) {
      throw new UnsupportedOperationException();
   }

   @Override
   public final int size() {
      throw new UnsupportedOperationException();
   }

   @Override
   public final Publisher<K> publishKeys(Predicate<? super K> filter) {
      throw new UnsupportedOperationException();
   }

   @Override
   public final Publisher<MarshalledEntry<K, V>> publishEntries(Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata) {
      throw new UnsupportedOperationException();
   }

   @Override
   public final void purge(Executor threadPool, PurgeListener<? super K> listener) {
      throw new UnsupportedOperationException();
   }

   @Override
   public final void clear() {
      throw new UnsupportedOperationException();
   }

   @Override
   public abstract void deleteBatch(Iterable<Object> keys);

   @Override
   public abstract void writeBatch(Iterable<MarshalledEntry<? extends K, ? extends V>> marshalledEntries);
}
