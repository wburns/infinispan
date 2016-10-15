package org.infinispan.container.offheap;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.container.DataContainer;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.KeyFilter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.CollectionMapper;
import org.infinispan.util.SetMapper;
import org.infinispan.util.TimeService;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * @author wburns
 * @since 9.0
 */
public class OffHeapTranslatedDataContainer implements DataContainer<WrappedByteArray, WrappedByteArray> {
   private final ByteBufAllocator allocator;
   // Bad hack to have the ConcurrentMap be typed properly
   private final ConcurrentMap<OffHeapInternalEntry, OffHeapInternalEntry> entries;
   private OffHeapEntryFactory offHeapEntryFactory;

   public OffHeapTranslatedDataContainer(ByteBufAllocator allocator) {
      this.allocator = allocator;
      entries = new ConcurrentHashMap<>();
   }

   @Inject
   public void inject(StreamingMarshaller marshaller, TimeService timeService, InternalEntryFactory internalEntryFactory) {
      offHeapEntryFactory = new OffHeapEntryFactory(marshaller, allocator, timeService, internalEntryFactory);
   }

//   protected OffHeapTranslatedDataContainer(long thresholdSize, EvictionStrategy strategy, EvictionThreadPolicy policy,
//         EntrySizeCalculator<? super WrappedByteArray, ? super WrappedByteArray> sizeCalculator,
//         ByteBufAllocator allocator) {
//      this.allocator = allocator;
//      this.wrappedEntries = castMap(entries);
//   }
//
//   protected OffHeapTranslatedDataContainer(long thresholdSize, EvictionStrategy strategy, EvictionThreadPolicy policy,
//         EvictionType thresholdPolicy, ByteBufAllocator allocator) {
//      this.allocator = allocator;
//      this.wrappedEntries = castMap(entries);
//   }

   static WrappedByteArray toWrapper(Object obj) {
      if (obj instanceof WrappedByteArray) {
         return (WrappedByteArray) obj;
      }
      throw new IllegalArgumentException("Require WrappedByteArray: got " + obj.getClass());
   }

   InternalCacheEntry<WrappedByteArray, WrappedByteArray> fromWrappedEntry(OffHeapInternalEntry entry) {
      if (entry == null) {
         return null;
      }
      ByteBufWrapper wrapper = entry.getBuffer();
      InternalCacheEntry<WrappedByteArray, WrappedByteArray> ice = offHeapEntryFactory.fromBuffer(wrapper.buffer);
      return ice;
   }

   @Override
   public InternalCacheEntry<WrappedByteArray, WrappedByteArray> get(Object k) {
      WrappedByteArray wba = toWrapper(k);
      while (true) {
         OffHeapInternalEntry entry = entries.get(wba);
         if (entry == null) return null;
         ByteBufWrapper wrapper = entry.getBuffer();
         // We retain it temporarily to make sure we can read the value, if we cannot then have to loop back around
         if (wrapper.retain() != null) {
            try {
               return fromWrappedEntry(entry);
            } finally {
               wrapper.release();
            }
         }
      }
   }

   @Override
   public InternalCacheEntry<WrappedByteArray, WrappedByteArray> peek(Object k) {
      return get(k);
   }

   @Override
   public void put(WrappedByteArray key, WrappedByteArray value, Metadata metadata) {
      ByteBuf buf = offHeapEntryFactory.create(key, value, metadata);
      OffHeapInternalEntry entry = new OffHeapInternalEntry(key.hashCode(), new ByteBufWrapper(buf));
      entries.compute(entry, (k, v) -> {
         if (v != null) {
            ByteBufWrapper prev = v.changeByteBufWrapper(entry.getBuffer());
            prev.release();
            return v;
         } else {
            return k;
         }
      });
   }

   @Override
   public boolean containsKey(Object k) {
      return entries.containsKey(toWrapper(k));
   }

   @Override
   public InternalCacheEntry<WrappedByteArray, WrappedByteArray> remove(Object k) {
      OffHeapInternalEntry entry = entries.remove(toWrapper(k));
      InternalCacheEntry<WrappedByteArray, WrappedByteArray> returned = fromWrappedEntry(entry);
      entry.getBuffer().release();
      return returned;
   }

   @Override
   public int size() {
      return entries.size();
   }

   @Override
   public int sizeIncludingExpired() {
      return 0;
   }


   @Override
   public void clear() {
      Iterator<OffHeapInternalEntry> iterator = entries.keySet().iterator();
      while (iterator.hasNext()) {
         OffHeapInternalEntry entry = iterator.next();
         iterator.remove();
         entry.getBuffer().release();
      }
   }

   @Override
   public Set<WrappedByteArray> keySet() {
      // TODO: handle expired + concurrent update/removal
      return new SetMapper<>(entries.keySet(), e -> fromWrappedEntry(e).getKey());
   }

   @Override
   public Collection<WrappedByteArray> values() {
      // TODO: handle expired + concurrent update/removal
      return new CollectionMapper<>(entries.values(), e -> fromWrappedEntry(e).getValue());
   }

   @Override
   public Set<InternalCacheEntry<WrappedByteArray, WrappedByteArray>> entrySet() {
      // TODO: handle expired + concurrent update/removal
      return new SetMapper<>(entries.entrySet(), e -> fromWrappedEntry(e.getValue()));
   }

   @Override
   public void purgeExpired() {
      // TODO: pass to expiration manager
   }

   @Override
   public void evict(WrappedByteArray key) {
      // TODO: handle passivation
      throw new UnsupportedOperationException();
   }

   @Override
   public InternalCacheEntry<WrappedByteArray, WrappedByteArray> compute(WrappedByteArray key, ComputeAction<WrappedByteArray, WrappedByteArray> action) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void executeTask(KeyFilter<? super WrappedByteArray> filter, BiConsumer<? super WrappedByteArray,
         InternalCacheEntry<WrappedByteArray, WrappedByteArray>> action) throws InterruptedException {
      // TODO: need to do this
      entries.forEach((k, v) -> {
         InternalCacheEntry<WrappedByteArray, WrappedByteArray> entry = fromWrappedEntry(k);
         if (filter.accept(entry.getKey())) {
            action.accept(entry.getKey(), entry);
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
      // TODO: handle expired + concurrent update/removal
      return new IteratorMapper<>(entries.values().iterator(), this::fromWrappedEntry);
   }

   @Override
   public Iterator<InternalCacheEntry<WrappedByteArray, WrappedByteArray>> iteratorIncludingExpired() {
      // TODO: handle concurrent update/removal
      return new IteratorMapper<>(entries.values().iterator(), this::fromWrappedEntry);
   }
}
