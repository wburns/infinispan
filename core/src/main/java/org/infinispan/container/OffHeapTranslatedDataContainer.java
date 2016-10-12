package org.infinispan.container;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.function.BiConsumer;

import org.infinispan.commons.marshall.WrappedByteArray;
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
   private final DataContainer<Object, ByteBufWrapper> realDataContainer;

   protected InternalEntryFactory entryFactory;

   public OffHeapTranslatedDataContainer(ByteBufAllocator allocator, DataContainer<Object, ByteBufWrapper> realDataContainer) {
      this.allocator = allocator;
      this.realDataContainer = realDataContainer;
   }

   @Inject
   public void initialize(ComponentRegistry registry, InternalEntryFactory entryFactory) {
      registry.wireDependencies(realDataContainer);
      this.entryFactory = entryFactory;
   }

   <T> T toOffHeap(WrappedByteArray array) {
      byte[] bytes = array.getBytes();
      ByteBuf buf = allocator.directBuffer(bytes.length, bytes.length);
      buf.writeBytes(bytes);

      return (T) ByteBufWrapper.newInstance(buf, array.getHashCode());
   }

   static WrappedByteArray toWrapper(Object obj) {
      if (obj instanceof WrappedByteArray) {
         return (WrappedByteArray) obj;
      }
      throw new IllegalArgumentException("Require WrappedByteArray: got " + obj.getClass());
   }

   InternalCacheEntry<WrappedByteArray, WrappedByteArray> fromWrappedEntry(
         InternalCacheEntry<Object, ByteBufWrapper> entry) {
      if (entry == null) {
         return null;
      }
      // This cast should never fail
      ByteBufWrapper key = (ByteBufWrapper) entry.getKey();
      ByteBufWrapper value = entry.getValue();
      WrappedByteArray newKey = fromWrapper(key);
//      // Release it so it can be freed
//      key.getBuffer().release();
      WrappedByteArray newValue = fromWrapper(value);
//      // Release it so it can be freed
//      value.getBuffer().release();
      return entryFactory.create(newKey, newValue, entry);
   }

   InternalCacheEntry<WrappedByteArray, WrappedByteArray> fromWrappedEntry(
         Object key, InternalCacheEntry<Object, ByteBufWrapper> entry) {
      if (entry == null) {
         return null;
      }

      // This cast should never fail
      ByteBufWrapper value = entry.getValue();
      WrappedByteArray newValue = fromWrapper(value);
      // Release it so it can be freed
      value.release();
      return entryFactory.create((WrappedByteArray) key, newValue, entry);
   }

   static WrappedByteArray fromWrapper(Object obj) {
      if (obj instanceof ByteBufWrapper) {
         ByteBufWrapper bbw = (ByteBufWrapper) obj;
         byte[] bytes = new byte[bbw.getBuffer().readableBytes()];
         bbw.getBuffer().getBytes(0, bytes);
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
         InternalCacheEntry<Object, ByteBufWrapper> ice = realDataContainer.get(wba);
         if (ice == null) return null;
         // We retain it temporarily to make sure we can read the value, if we cannot then have to loop back around
         if (ice.getValue().retain() != null) {
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
      ByRef<InternalCacheEntry<Object, ByteBufWrapper>> ref = new ByRef<>(null);
      realDataContainer.compute(toOffHeap(key), (k, oldEntry, factory) -> {
         if (oldEntry != null) {
            // If the entry was replaced the node value is just updated, so the lookup key needs to
            // be relinquished
            // TODO: is this an issue with Caffeine?  Seems unlikely JRE would ever change this behavior
            ((ByteBufWrapper) k).release();
            // Need to release after compute - note the key in the container is the same instance
            // as the key in entry value
            ref.set(oldEntry);
         }
         return factory.create(k, toOffHeap(value), metadata);
      });
      InternalCacheEntry<Object, ByteBufWrapper> prev = ref.get();
      if (prev != null) {
         prev.getValue().release();
      }
   }

   @Override
   public boolean containsKey(Object k) {
      return realDataContainer.containsKey(toWrapper(k));
   }

   @Override
   public InternalCacheEntry<WrappedByteArray, WrappedByteArray> remove(Object k) {
      InternalCacheEntry<Object, ByteBufWrapper> entry = realDataContainer.remove(toWrapper(k));
      InternalCacheEntry<WrappedByteArray, WrappedByteArray> returned = fromWrappedEntry(entry);
      ((ByteBufWrapper) entry.getKey()).release();
      entry.getValue().release();
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
      Iterator<InternalCacheEntry<Object, ByteBufWrapper>> iterator = realDataContainer.iterator();
      // Have to release all entries first - TODO this has concurrency issue if another operation done at same time
      while (iterator.hasNext()) {
         InternalCacheEntry<Object, ByteBufWrapper> entry = iterator.next();
         ((ByteBufWrapper) entry.getKey()).release();
         entry.getValue().release();
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
