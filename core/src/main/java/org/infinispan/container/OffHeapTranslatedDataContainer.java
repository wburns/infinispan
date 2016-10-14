package org.infinispan.container;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.function.BiConsumer;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.KeyFilter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.metadata.Metadata;
import org.infinispan.stream.impl.spliterators.IteratorAsSpliterator;
import org.infinispan.util.CollectionMapper;
import org.infinispan.util.SetMapper;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.IllegalReferenceCountException;

/**
 * @author wburns
 * @since 9.0
 */
public class OffHeapTranslatedDataContainer implements DataContainer<WrappedByteArray, WrappedByteArray> {
   private final ByteBufAllocator allocator;
   private final DataContainer<Object, ByteBufWrapperValue> realDataContainer;

   protected InternalEntryFactory entryFactory;

   public OffHeapTranslatedDataContainer(ByteBufAllocator allocator, DataContainer<Object, ByteBufWrapperValue> realDataContainer) {
      this.allocator = allocator;
      this.realDataContainer = realDataContainer;
   }

   @Inject
   public void initialize(ComponentRegistry registry, InternalEntryFactory entryFactory) {
      registry.wireDependencies(realDataContainer);
      this.entryFactory = entryFactory;
   }

   InternalCacheEntry<Object, ByteBufWrapperValue> toOffHeap(WrappedBytes key, WrappedBytes value,
                                                                        Metadata metadata) {
      int keySize = key.getLength();
      int valueSize = value.getLength();
      ByteBuf buf = allocator.directBuffer(keySize + valueSize, keySize + valueSize);
      buf.writeBytes(key.getBytes(), key.backArrayOffset(), keySize);
      buf.writeBytes(value.getBytes(), value.backArrayOffset(), valueSize);
      // Set the reader index to the size of the key to tell the wrapper where the key ends
      buf.setIndex(keySize, keySize);

      return entryFactory.create(ByteBufWrapperKey.newInstance(buf, key.hashCode()), ByteBufWrapperValue.newInstance(buf), metadata);
   }

   static WrappedByteArray toWrapper(Object obj) {
      if (obj instanceof WrappedByteArray) {
         return (WrappedByteArray) obj;
      }
      throw new IllegalArgumentException("Require WrappedByteArray: got " + obj.getClass());
   }

   InternalCacheEntry<WrappedByteArray, WrappedByteArray> fromWrappedEntry(
         InternalCacheEntry<Object, ByteBufWrapperValue> entry) {
      if (entry == null) {
         return null;
      }
      // This cast should never fail
      ByteBufWrapperKey key = (ByteBufWrapperKey) entry.getKey();
      ByteBufWrapper value = entry.getValue();
      WrappedByteArray newKey = fromWrapper(key);
      WrappedByteArray newValue = fromWrapper(value);
      return entryFactory.create(newKey, newValue, entry);
   }

   /**
    * Same as {@link OffHeapTranslatedDataContainer#fromWrappedEntry(InternalCacheEntry)} except that the key will
    * not be unwrapped since the caller already has it
    * @param key
    * @param entry
    * @return
    */
   InternalCacheEntry<WrappedByteArray, WrappedByteArray> fromWrappedEntry(
         Object key, InternalCacheEntry<Object, ByteBufWrapperValue> entry) {
      if (entry == null) {
         return null;
      }

      // This cast should never fail
      ByteBufWrapper value = entry.getValue();
      WrappedByteArray newValue = fromWrapper(value);
      // Release the key so it can be freed
      ((ByteBufWrapperKey) entry.getKey()).release();
      return entryFactory.create((WrappedByteArray) key, newValue, entry);
   }

   static WrappedByteArray fromWrapper(Object obj) {
      if (obj instanceof ByteBufWrapper) {
         ByteBufWrapper bbw = (ByteBufWrapper) obj;
         byte[] bytes = new byte[bbw.getLength()];
         bbw.getBuffer().getBytes(bbw.getOffset(), bytes);
         return new WrappedByteArray(bytes);
      }
      if (obj instanceof WrappedByteArray) {
         return (WrappedByteArray) obj;
      }
      throw new IllegalArgumentException("Unsupported instance:" + obj.getClass());
   }

   @Override
   public InternalCacheEntry<WrappedByteArray, WrappedByteArray> get(Object k) {
      WrappedByteArray wba = toWrapper(k);
      while (true) {
         InternalCacheEntry<Object, ByteBufWrapperValue> ice = realDataContainer.get(wba);
         if (ice == null) return null;
         // We retain it temporarily to make sure we can read the value, if we cannot then have to loop back around
         if (((ByteBufWrapperKey) ice.getKey()).retain() != null) {
            return fromWrappedEntry(k, ice);
         }
      }
   }

   @Override
   public InternalCacheEntry<WrappedByteArray, WrappedByteArray> peek(Object k) {
      return get(k);
   }

   @Override
   public void put(WrappedByteArray key, WrappedByteArray value, Metadata metadata) {
      ByRef<InternalCacheEntry<Object, ByteBufWrapperValue>> ref = new ByRef<>(null);
      InternalCacheEntry<Object, ByteBufWrapperValue> ice = toOffHeap(key, value, metadata);
      realDataContainer.compute(ice.getKey(), (k, oldEntry, factory) -> {
         if (oldEntry != null) {
            // Need to release after returning from compute.  This is to prevent a read from reading a released
            ref.set(oldEntry);
            // TODO: we could just rely on the fact that the reader will spin trying over and over
//            ((ByteBufWrapperKey) oldEntry.getKey()).release();
         }
         return ice;
      });
      InternalCacheEntry<Object, ByteBufWrapperValue> prev = ref.get();
      if (prev != null) {
         ((ByteBufWrapperKey) prev.getKey()).release();
         prev.getValue().deallocate();
      }
   }

   @Override
   public boolean containsKey(Object k) {
      return realDataContainer.containsKey(toWrapper(k));
   }

   @Override
   public InternalCacheEntry<WrappedByteArray, WrappedByteArray> remove(Object k) {
      InternalCacheEntry<Object, ByteBufWrapperValue> entry = realDataContainer.remove(toWrapper(k));
      InternalCacheEntry<WrappedByteArray, WrappedByteArray> returned = fromWrappedEntry(entry);
      ((ByteBufWrapperKey) entry.getKey()).release();
      entry.getValue().deallocate();
      return returned;
   }

   @Override
   public int size() {
      return realDataContainer.size();
   }
   @Override
   public int sizeIncludingExpired() {
      return realDataContainer.sizeIncludingExpired();
   }

   @Override
   public void clear() {
      Iterator<InternalCacheEntry<Object, ByteBufWrapperValue>> iterator = realDataContainer.iterator();
      // Have to release all entries first - TODO this has concurrency issue if another operation done at same time
      while (iterator.hasNext()) {
         InternalCacheEntry<Object, ByteBufWrapperValue> entry = iterator.next();
         ((ByteBufWrapperKey) entry.getKey()).release();
         entry.getValue().deallocate();
      }
      realDataContainer.clear();
   }

   @Override
   public Set<WrappedByteArray> keySet() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Collection<WrappedByteArray> values() {
      return new CollectionMapper<>(realDataContainer.values(), OffHeapTranslatedDataContainer::fromWrapper);
   }

   @Override
   public Set<InternalCacheEntry<WrappedByteArray, WrappedByteArray>> entrySet() {
      return new SetMapper<>(realDataContainer.entrySet(), this::fromWrappedEntry);
   }

   @Override
   public void purgeExpired() {
      realDataContainer.purgeExpired();
   }

   @Override
   public void evict(WrappedByteArray key) {
      throw new UnsupportedOperationException();
   }

   @Override
   public InternalCacheEntry<WrappedByteArray, WrappedByteArray> compute(WrappedByteArray key, ComputeAction<WrappedByteArray, WrappedByteArray> action) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void executeTask(KeyFilter<? super WrappedByteArray> filter, BiConsumer<? super WrappedByteArray,
         InternalCacheEntry<WrappedByteArray, WrappedByteArray>> action) throws InterruptedException {
      realDataContainer.executeTask(KeyFilter.ACCEPT_ALL_FILTER, (k, e) -> {
         // TODO: handle concurrent changes to entry
         InternalCacheEntry<WrappedByteArray, WrappedByteArray> inner = fromWrappedEntry(e);
         if (filter.accept(inner.getKey())) {
            action.accept(inner.getKey(), inner);
         }
      });
   }

   @Override
   public void executeTask(KeyValueFilter<? super WrappedByteArray, ? super WrappedByteArray> filter,
                           BiConsumer<? super WrappedByteArray, InternalCacheEntry<WrappedByteArray, WrappedByteArray>> action) throws InterruptedException {
      throw new UnsupportedOperationException();
   }

   @Override
   public Iterator<InternalCacheEntry<WrappedByteArray, WrappedByteArray>> iterator() {
      return new IteratorMapper<>(realDataContainer.iterator(), this::fromWrappedEntry);
   }

   @Override
   public Iterator<InternalCacheEntry<WrappedByteArray, WrappedByteArray>> iteratorIncludingExpired() {
      return new IteratorMapper<>(realDataContainer.iteratorIncludingExpired(), this::fromWrappedEntry);
   }

   @Override
   public void resize(long newSize) {
      realDataContainer.resize(newSize);
   }

   @Override
   public long capacity() {
      return realDataContainer.capacity();
   }
}
