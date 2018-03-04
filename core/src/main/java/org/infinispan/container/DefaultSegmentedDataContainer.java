package org.infinispan.container;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiConsumer;
import java.util.function.IntConsumer;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.filter.KeyFilter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.remoting.transport.Address;

/**
 * @author wburns
 * @since 9.0
 */
public class DefaultSegmentedDataContainer<K, V> extends AbstractSegmentedDataContainer<K, V> implements SegmentedDataContainer<K, V> {
   private final AtomicReferenceArray<DataContainer<K, V>> containers;
   private final Supplier<DataContainer<K, V>> supplier;
   private final Address localNode;
   private final ToIntFunction<Object> segmentMapper;

   public DefaultSegmentedDataContainer(int maxSegments, Supplier<DataContainer<K, V>> supplier, Address localNode,
         ToIntFunction<Object> segmentMapper) {
      this.containers = new AtomicReferenceArray<>(maxSegments);
      this.supplier = supplier;
      this.localNode = localNode;
      this.segmentMapper = segmentMapper;
   }

   @Override
   protected ToIntFunction<Object> segmentMapper() {
      return segmentMapper;
   }

   @Override
   public InternalCacheEntry<K, V> get(int segment, Object k) {
      DataContainer<K, V> container = containers.get(segment);
      return container == null ? null : container.get(k);
   }

   @Override
   public InternalCacheEntry<K, V> peek(int segment, Object k) {
      DataContainer<K, V> container = containers.get(segment);
      return container == null ? null : container.peek(k);
   }

   @Override
   public void put(int segment, K k, V v, Metadata metadata) {
      DataContainer<K, V> container = containers.get(segment);
      if (container != null) {
         container.put(k, v, metadata);
      }
   }

   @Override
   public boolean containsKey(int segment, Object k) {
      DataContainer<K, V> container = containers.get(segment);
      return container != null && container.containsKey(k);
   }

   @Override
   public InternalCacheEntry<K, V> remove(int segment, Object k) {
      DataContainer<K, V> container = containers.get(segment);
      return container == null ? null : container.remove(k);
   }

   @Override
   public int size(int segment) {
      DataContainer<K, V> container = containers.get(segment);
      return container == null ? 0 : container.size();
   }

   @Override
   public int sizeIncludingExpired(int segment) {
      DataContainer<K, V> container = containers.get(segment);
      return container == null ? 0 : container.size();
   }

   @Override
   public void clear(int segment) {
      DataContainer<K, V> container = containers.get(segment);
      if (container != null) {
         container.clear();
      }
   }

   @Override
   public void evict(int segment, K key) {
      DataContainer<K, V> container = containers.get(segment);
      if (container != null) {
         container.evict(key);
      }
   }

   @Override
   public InternalCacheEntry<K, V> compute(int segment, K key, ComputeAction<K, V> action) {
      DataContainer<K, V> container = containers.get(segment);
      return container == null ? null : container.compute(key, action);
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iterator(int segment) {
      DataContainer<K, V> container = containers.get(segment);
      return container == null ? Collections.emptyIterator() : container.iterator();
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iteratorIncludingExpired(int segment) {
      DataContainer<K, V> container = containers.get(segment);
      return container == null ? Collections.emptyIterator() : container.iteratorIncludingExpired();
   }

   @Override
   public int size() {
      int size = 0;
      for (int i = 0; i < containers.length(); ++i) {
         size += size(i);
         // Overflow
         if (size < 0) {
            return Integer.MAX_VALUE;
         }
      }
      return size;
   }

   @Override
   public int sizeIncludingExpired() {
      int size = 0;
      for (int i = 0; i < containers.length(); ++i) {
         size += sizeIncludingExpired(i);
         // Overflow
         if (size < 0) {
            return Integer.MAX_VALUE;
         }
      }
      return size;
   }

   @Override
   public void clear() {
      for (int i = 0; i < containers.length(); ++i) {
         clear(i);
      }
   }

   @Override
   public Set<K> keySet() {
      return null;
   }

   @Override
   public Collection<V> values() {
      return null;
   }

   @Override
   public Set<InternalCacheEntry<K, V>> entrySet() {
      return null;
   }

   @Override
   public void executeTask(KeyFilter<? super K> filter, BiConsumer<? super K, InternalCacheEntry<K, V>> action) throws InterruptedException {

   }

   @Override
   public void executeTask(KeyValueFilter<? super K, ? super V> filter, BiConsumer<? super K, InternalCacheEntry<K, V>> action) throws InterruptedException {

   }

   private void startNewDataContainerForSegment(int segment) {
      if (containers.get(segment) == null) {
         containers.set(segment, supplier.get());
      }
   }

   @TopologyChanged
   public void onTopologyChange(TopologyChangedEvent<K, V> topologyChangedEvent) {
      if (topologyChangedEvent.isPre()) {
         ConsistentHash ch = topologyChangedEvent.getWriteConsistentHashAtEnd();
         Set<Integer> segments = ch.getSegmentsForOwner(localNode);
         if (segments instanceof IntSet) {
            ((IntSet) segments).forEach((IntConsumer) this::startNewDataContainerForSegment);
         } else {
            segments.forEach(this::startNewDataContainerForSegment);
         }
      } else {
         // TODO: need to remove containers here
      }
   }
}
