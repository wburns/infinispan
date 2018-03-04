package org.infinispan.stream.impl.local;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.RemovableCloseableIterator;
import org.infinispan.container.SegmentedDataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.rxjava.FlowableFromIntSetFunction;

import io.reactivex.Flowable;

/**
 * @author wburns
 * @since 9.0
 */
public class SegmentedEntryStreamSupplier<K, V> implements AbstractLocalCacheStream.StreamSupplier<CacheEntry<K, V>, Stream<CacheEntry<K, V>>> {
   // TODO: make this a component!
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final boolean trace = log.isTraceEnabled();

   private final Cache<K, V> cache;
   private final boolean remoteIterator;
   private final ToIntFunction<Object> toIntFunction;
   private final SegmentedDataContainer<K, V> segmentedDataContainer;

   public SegmentedEntryStreamSupplier(Cache<K, V> cache, boolean remoteIterator, ToIntFunction<Object> toIntFunction,
         SegmentedDataContainer<K, V> segmentedDataContainer) {
      this.cache = cache;
      this.remoteIterator = remoteIterator;
      this.toIntFunction = toIntFunction;
      this.segmentedDataContainer = segmentedDataContainer;
   }

   @Override
   public Stream<CacheEntry<K, V>> buildStream(IntSet segmentsToFilter, Set<?> keysToFilter) {
      Stream<CacheEntry<K, V>> stream;
      if (keysToFilter != null) {
         if (trace) {
            log.tracef("Applying key filtering %s", keysToFilter);
         }
         // Make sure we aren't going remote to retrieve these
         AdvancedCache<K, V> advancedCache = AbstractDelegatingCache.unwrapCache(cache).getAdvancedCache()
               .withFlags(Flag.CACHE_MODE_LOCAL);
         stream = keysToFilter.stream()
               .map(advancedCache::getCacheEntry)
               .filter(Objects::nonNull);
         if (segmentsToFilter != null && toIntFunction != null) {
            if (trace) {
               log.tracef("Applying segment filter %s", segmentsToFilter);
            }
            stream = stream.filter(k -> {
               K key = k.getKey();
               int segment = toIntFunction.applyAsInt(key);
               boolean isPresent = segmentsToFilter.contains(segment);
               if (trace)
                  log.tracef("Is key %s present in segment %d? %b", key, segment, isPresent);
               return isPresent;
            });
         }
      } else {
         if (segmentsToFilter != null) {
            Flowable<Iterator<InternalCacheEntry<K, V>>> flowable = new FlowableFromIntSetFunction<>(segmentsToFilter,
                  i -> {
                     Iterator<InternalCacheEntry<K, V>> iter = segmentedDataContainer.iterator(i);
                     if (iter == null) {
                        return Collections.emptyIterator();
                     }
                     return iter;
                  });
            Iterable<InternalCacheEntry<K, V>> iterable = flowable.flatMapIterable(it -> () -> it).blockingIterable();
            stream = StreamSupport.stream(cast(iterable.spliterator()), false);
         } else {
            stream = StreamSupport.stream(cast(segmentedDataContainer.spliterator()), false);
         }
      }
      return stream;
   }

   private Spliterator<CacheEntry<K, V>> cast(Spliterator spliterator) {
      return (Spliterator<CacheEntry<K, V>>) spliterator;
   }

   @Override
   public CloseableIterator<CacheEntry<K, V>> removableIterator(CloseableIterator<CacheEntry<K, V>> realIterator) {
      if (remoteIterator) {
         return realIterator;
      }
      return new RemovableCloseableIterator<>(realIterator, e -> cache.remove(e.getKey(), e.getValue()));
   }
}
