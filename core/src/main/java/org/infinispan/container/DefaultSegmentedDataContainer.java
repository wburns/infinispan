package org.infinispan.container;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import org.infinispan.Cache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.RangeSet;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.HashConfiguration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.filter.KeyFilter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.rxjava.FlowableFromIntSetFunction;

import io.reactivex.Flowable;

/**
 * @author wburns
 * @since 9.0
 */
public class DefaultSegmentedDataContainer<K, V> extends AbstractSegmentedDataContainer<K, V> {
   private final Supplier<DataContainer<K, V>> supplier;

   private ComponentRegistry componentRegistry;
   private ToIntFunction<Object> segmentMapper;
   private AtomicReferenceArray<DataContainer<K, V>> containers;
   private Address localNode;

   @Inject
   private Cache<K, V> cache;

   public DefaultSegmentedDataContainer(Supplier<DataContainer<K, V>> supplier) {
      this.supplier = supplier;
   }

   @Start
   @Override
   public void start() {
      localNode = cache.getCacheManager().getAddress();
      componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      cache.addListener(this);

      HashConfiguration hashConfiguration = cache.getCacheConfiguration().clustering().hash();
      segmentMapper = hashConfiguration.keyPartitioner()::getSegment;
      containers = new AtomicReferenceArray<>(hashConfiguration.numSegments());

      CacheMode mode = cache.getCacheConfiguration().clustering().cacheMode();
      // Local or remote cache we just instantiate all the stores immediately
      if (!mode.isClustered() || mode.isReplicated()) {
         for (int i = 0; i < containers.length(); ++i) {
            startNewDataContainerForSegment(i);
         }
      }
   }

   @Stop
   @Override
   public void stop() {
      cache.removeListener(this);

      for (int i = 0; i < containers.length(); ++i) {
         DataContainer<K, V> dataContainer = containers.getAndSet(i, null);
         dataContainer.stop();
      }
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
   public Iterator<InternalCacheEntry<K, V>> iterator() {
      IntSet intSet = new RangeSet(containers.length());
      Flowable<Iterator<InternalCacheEntry<K, V>>> flowable = new FlowableFromIntSetFunction<>(intSet,
            i -> {
               Iterator<InternalCacheEntry<K, V>> iter = iterator(i);
               if (iter == null) {
                  return Collections.emptyIterator();
               }
               return iter;
            });
      Iterable<InternalCacheEntry<K, V>> iterable = flowable.flatMapIterable(it -> () -> it).blockingIterable();
      return iterable.iterator();
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iteratorIncludingExpired() {
      IntSet intSet = new RangeSet(containers.length());
      Flowable<Iterator<InternalCacheEntry<K, V>>> flowable = new FlowableFromIntSetFunction<>(intSet,
            i -> {
               Iterator<InternalCacheEntry<K, V>> iter = iteratorIncludingExpired(i);
               if (iter == null) {
                  return Collections.emptyIterator();
               }
               return iter;
            });
      Iterable<InternalCacheEntry<K, V>> iterable = flowable.flatMapIterable(it -> () -> it).blockingIterable();
      return iterable.iterator();
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

   @Override
   public void removeDataContainer(int segment, Consumer<DataContainer<K, V>> preDestroy) {
      DataContainer<K, V> container = containers.getAndSet(segment, null);
      preDestroy.accept(container);
      container.stop();
   }

   private void startNewDataContainerForSegment(int segment) {
      if (containers.get(segment) == null) {
         DataContainer<K, V> dataContainer = supplier.get();
         componentRegistry.wireDependencies(dataContainer);
         dataContainer.start();
         containers.set(segment, dataContainer);
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
         ConsistentHash beginCH = topologyChangedEvent.getWriteConsistentHashAtEnd();
         ConsistentHash endCH = topologyChangedEvent.getWriteConsistentHashAtEnd();

         Set<Integer> beginSegments = beginCH.getSegmentsForOwner(localNode);
         Set<Integer> endSegments = endCH.getSegmentsForOwner(localNode);

         if (beginSegments instanceof IntSet && endSegments instanceof IntSet) {
            IntSet endIntSet = (IntSet) endSegments;
            ((IntSet) beginSegments).forEach((int i) -> {
               if (!endIntSet.contains(i)) {
                  DataContainer<K, V> dataContainer = containers.getAndSet(i, null);
                  if (dataContainer != null) {
                     dataContainer.stop();
                  }
               }
            });
         } else {
            beginSegments.forEach(i -> {
               if (!endSegments.contains(i)) {
                  DataContainer<K, V> dataContainer = containers.getAndSet(i, null);
                  if (dataContainer != null) {
                     dataContainer.stop();
                  }
               }
            });
         }
      }
   }
}
