package org.infinispan.stream.impl.local;

import java.lang.invoke.MethodHandles;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.RemovableCloseableIterator;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author wburns
 * @since 9.0
 */
public class SegmentedEntryStreamSupplier<K, V> implements AbstractLocalCacheStream.StreamSupplier<CacheEntry<K, V>, Stream<CacheEntry<K, V>>> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final boolean trace = log.isTraceEnabled();

   private final Cache<K, V> cache;
   private final boolean remoteIterator;
   private final ToIntFunction<Object> toIntFunction;
   private final Function<IntSet, Stream<CacheEntry<K, V>>> function;

   public SegmentedEntryStreamSupplier(Cache<K, V> cache, boolean remoteIterator, ToIntFunction<Object> toIntFunction,
         Function<IntSet, Stream<CacheEntry<K, V>>> function) {
      this.cache = cache;
      this.remoteIterator = remoteIterator;
      this.toIntFunction = toIntFunction;
      this.function = function;
   }

   @Override
   public Stream<CacheEntry<K, V>> buildStream(IntSet segmentsToFilter, Set<?> keysToFilter) {
      return null;
   }

   @Override
   public CloseableIterator<CacheEntry<K, V>> removableIterator(CloseableIterator<CacheEntry<K, V>> realIterator) {
      if (remoteIterator) {
         return realIterator;
      }
      return new RemovableCloseableIterator<>(realIterator, e -> cache.remove(e.getKey(), e.getValue()));
   }
}
