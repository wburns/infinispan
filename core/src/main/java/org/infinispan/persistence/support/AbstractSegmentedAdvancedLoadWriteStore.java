package org.infinispan.persistence.support;

import java.util.function.ToIntFunction;

import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.spi.SegmentedAdvancedLoadWriteStore;

/**
 * Abstract segment loader writer that implements all the non segmented methods and throws an exception if they are
 * invoked - which means it is a bug! These methods are also all declared final as to make sure the end user does not
 * implement the incorrect method.
 * @author wburns
 * @since 9.4
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
      return contains(getKeyMapper().applyAsInt(key), key);
   }

   @Override
   public final void write(MarshalledEntry<? extends K, ? extends V> entry) {
      write(getKeyMapper().applyAsInt(entry.getKey()), entry);
   }

   @Override
   public final boolean delete(Object key) {
      return delete(getKeyMapper().applyAsInt(key), key);
   }
}
