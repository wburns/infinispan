package org.infinispan.stream.impl.local;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.commons.util.RemovableCloseableIterator;
import org.infinispan.commons.util.SpliteratorMapper;
import org.infinispan.container.SegmentedDataContainer;
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
public class SegmentedKeyStreamSupplier<K, V> implements AbstractLocalCacheStream.StreamSupplier<K, Stream<K>> {
   // TODO: make this a component!
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final boolean trace = log.isTraceEnabled();

   private final Cache<K, V> cache;
   private final ToIntFunction<Object> toIntFunction;
   private final SegmentedDataContainer<K, V> segmentedDataContainer;

   public SegmentedKeyStreamSupplier(Cache<K, V> cache, ToIntFunction<Object> toIntFunction,
         SegmentedDataContainer<K, V> segmentedDataContainer) {
      this.cache = cache;
      this.toIntFunction = toIntFunction;
      this.segmentedDataContainer = segmentedDataContainer;
   }

   @Override
   public Stream<K> buildStream(IntSet segmentsToFilter, Set<?> keysToFilter, boolean parallel) {
      Stream<K> stream;
      if (keysToFilter != null) {
         if (trace) {
            log.tracef("Applying key filtering %s", keysToFilter);
         }
         // Make sure we aren't going remote to retrieve these
         AdvancedCache<K, V> advancedCache = AbstractDelegatingCache.unwrapCache(cache).getAdvancedCache()
               .withFlags(Flag.CACHE_MODE_LOCAL);
         stream = (Stream<K>) (parallel ? keysToFilter.parallelStream() : keysToFilter.stream())
               .filter(advancedCache::containsKey);
         if (segmentsToFilter != null && toIntFunction != null) {
            if (trace) {
               log.tracef("Applying segment filter %s", segmentsToFilter);
            }
            stream = stream.filter(k -> {
               int segment = toIntFunction.applyAsInt(k);
               boolean isPresent = segmentsToFilter.contains(segment);
               if (trace)
                  log.tracef("Is key %s present in segment %d? %b", k, segment, isPresent);
               return isPresent;
            });
         }
      } else {
         if (segmentsToFilter != null) {
            stream = StreamSupport.stream(new SpliteratorMapper<>(segmentedDataContainer.spliterator(segmentsToFilter), Map.Entry::getKey), parallel);
         } else {
            stream = StreamSupport.stream(new SpliteratorMapper<>(segmentedDataContainer.spliterator(), Map.Entry::getKey), parallel);
         }
      }
      return stream;
   }

   @Override
   public CloseableIterator<K> removableIterator(CloseableIterator<K> realIterator) {
      return new RemovableCloseableIterator<>(realIterator, cache::remove);
   }
}
