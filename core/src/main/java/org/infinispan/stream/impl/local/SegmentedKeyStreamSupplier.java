package org.infinispan.stream.impl.local;

import java.lang.invoke.MethodHandles;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.RemovableCloseableIterator;
import org.infinispan.context.Flag;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author wburns
 * @since 9.0
 */
public class SegmentedKeyStreamSupplier<K, V> implements AbstractLocalCacheStream.StreamSupplier<K, Stream<K>> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final boolean trace = log.isTraceEnabled();

   private final Cache<K, V> cache;
   private final ToIntFunction<Object> toIntFunction;
   private final Function<IntSet, Stream<K>> function;

   public SegmentedKeyStreamSupplier(Cache<K, V> cache, ToIntFunction<Object> toIntFunction,
         Function<IntSet, Stream<K>> function) {
      this.cache = cache;
      this.toIntFunction = toIntFunction;
      this.function = function;
   }

   @Override
   public Stream<K> buildStream(IntSet segmentsToFilter, Set<?> keysToFilter) {
      Stream<K> stream;
      if (keysToFilter != null) {
         if (trace) {
            log.tracef("Applying key filtering %s", keysToFilter);
         }
         // Make sure we aren't going remote to retrieve these
         AdvancedCache<K, V> advancedCache = AbstractDelegatingCache.unwrapCache(cache).getAdvancedCache()
               .withFlags(Flag.CACHE_MODE_LOCAL);
         stream = (Stream<K>) keysToFilter.stream().filter(advancedCache::containsKey);
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
         stream = function.apply(segmentsToFilter);
      }
      return stream;
   }

   @Override
   public CloseableIterator<K> removableIterator(CloseableIterator<K> realIterator) {
      return new RemovableCloseableIterator<>(realIterator, cache::remove);
   }
}
