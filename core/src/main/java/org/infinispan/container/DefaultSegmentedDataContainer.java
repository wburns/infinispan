package org.infinispan.container;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.IntConsumer;

import org.infinispan.Cache;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.ConcatIterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.SmallIntSet;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.HashConfiguration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.remoting.transport.Address;

/**
 * @author wburns
 * @since 9.3
 */
@Listener
public class DefaultSegmentedDataContainer<K, V> extends AbstractSegmentedDataContainer<K, V>
      implements SegmentedDataContainer<K, V> {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final boolean trace = log.isTraceEnabled();

   private AtomicReferenceArray<ConcurrentMap<K, InternalCacheEntry<K, V>>> maps;
   private Address localNode;

   @Inject private KeyPartitioner keyPartitioner;
   @Inject private Cache<K, V> cache;

   @Start
   @Override
   public void start() {
      localNode = cache.getCacheManager().getAddress();
      cache.addListener(this);

      HashConfiguration hashConfiguration = cache.getCacheConfiguration().clustering().hash();
      maps = new AtomicReferenceArray<>(hashConfiguration.numSegments());

      CacheMode mode = cache.getCacheConfiguration().clustering().cacheMode();
      // Local or remote cache we just instantiate all the stores immediately
      if (!mode.isClustered() || mode.isReplicated()) {
         for (int i = 0; i < maps.length(); ++i) {
            maps.set(i, new ConcurrentHashMap<>());
         }
      }
   }

   @Stop
   @Override
   public void stop() {
      cache.removeListener(this);

      for (int i = 0; i < maps.length(); ++i) {
         maps.set(0, null);
      }
   }

   @Override
   protected Log log() {
      return log;
   }

   @Override
   protected boolean trace() {
      return trace;
   }

   @Override
   protected ConcurrentMap<K, InternalCacheEntry<K, V>> getMapForSegment(int segment) {
      return maps.get(segment);
   }

   @Override
   protected int getSegmentForKey(Object key) {
      return keyPartitioner.getSegment(key);
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iterator(IntSet segments) {
      return new EntryIterator(iteratorIncludingExpired(segments));
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iterator() {
      return new EntryIterator(iteratorIncludingExpired());
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iteratorIncludingExpired(IntSet segments) {
      List<Collection<InternalCacheEntry<K, V>>> valueIterables = new ArrayList<>(segments.size());
      segments.forEach((int s) -> {
         ConcurrentMap<K, InternalCacheEntry<K, V>> map = maps.get(s);
         if (map != null) {
            valueIterables.add(map.values());
         }
      });
      return new ConcatIterator<>(valueIterables);
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iteratorIncludingExpired() {
      List<Collection<InternalCacheEntry<K, V>>> valueIterables = new ArrayList<>(maps.length());
      for (int i = 0; i < maps.length(); ++i) {
         ConcurrentMap<K, InternalCacheEntry<K, V>> map = maps.get(i);
         if (map != null) {
            valueIterables.add(map.values());
         }
      }
      return new ConcatIterator<>(valueIterables);
   }

   @Override
   public int sizeIncludingExpired(IntSet segment) {
      PrimitiveIterator.OfInt iter = segment.iterator();
      int size = 0;
      while (iter.hasNext()) {
         ConcurrentMap<K, InternalCacheEntry<K, V>> map = maps.get(iter.nextInt());
         size += map != null ? map.size() : 0;
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
      for (int i = 0; i < maps.length(); ++i) {
         ConcurrentMap<K, InternalCacheEntry<K, V>> map = maps.get(i);
         if (map != null) {
            size += map.size();
            // Overflow
            if (size < 0) {
               return Integer.MAX_VALUE;
            }
         }
      }
      return size;
   }

   @Override
   public void clear(IntSet segments) {
      segments.forEach((int s) -> {
         ConcurrentMap<K, InternalCacheEntry<K, V>> map = maps.get(s);
         if (map != null) {
            map.clear();
         }
      });
   }

   @Override
   public void clear() {
      for (int i = 0; i < maps.length(); ++i) {
         ConcurrentMap<K, InternalCacheEntry<K, V>> map = maps.get(i);
         if (map != null) {
            map.clear();
         }
      }
   }

   @Override
   public Set<K> keySet() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Collection<V> values() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Set<InternalCacheEntry<K, V>> entrySet() {
      throw new UnsupportedOperationException();
   }

   @Override
   public void removeSegments(IntSet segments) {
      segments.forEach((int s) -> {
         ConcurrentMap<K, InternalCacheEntry<K, V>> map = maps.getAndSet(s, null);
         listeners.forEach(c -> c.accept(map.values()));
      });
   }

   private void startNewMap(int segment) {
      if (maps.get(segment) == null) {
         // Just in case of concurrent starts - this shouldn't be possible
         maps.compareAndSet(segment, null, new ConcurrentHashMap<>());
      }
   }

   @TopologyChanged
   public void onTopologyChange(TopologyChangedEvent<K, V> topologyChangedEvent) {
      if (topologyChangedEvent.isPre()) {
         ConsistentHash ch = topologyChangedEvent.getWriteConsistentHashAtEnd();
         Set<Integer> segments = ch.getSegmentsForOwner(localNode);
         if (segments instanceof IntSet) {
            ((IntSet) segments).forEach((IntConsumer) this::startNewMap);
         } else {
            segments.forEach(this::startNewMap);
         }
      } else {
         ConsistentHash beginCH = topologyChangedEvent.getWriteConsistentHashAtStart();
         ConsistentHash endCH = topologyChangedEvent.getWriteConsistentHashAtEnd();

         // When node first joins start CH is null - means it was all add so we don't need to remove any
         if (beginCH != null) {
            Set<Integer> beginSegments = beginCH.getSegmentsForOwner(localNode);
            Set<Integer> endSegments = endCH.getSegmentsForOwner(localNode);

            if (beginSegments.size() != 256) {
               System.currentTimeMillis();
            }

            IntSet copy = new SmallIntSet(beginSegments);
            copy.retainAll(endSegments);
            removeSegments(copy);
         }
      }
   }
}
