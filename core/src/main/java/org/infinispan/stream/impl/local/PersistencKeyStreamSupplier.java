package org.infinispan.stream.impl.local;

import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Set;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorMapper;
import org.infinispan.commons.util.Closeables;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.RemovableCloseableIterator;
import org.infinispan.context.Flag;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.DoubleIterator;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

/**
 * @author wburns
 * @since 9.3
 */
public class PersistencKeyStreamSupplier<K, V> implements AbstractLocalCacheStream.StreamSupplier<K, Stream<K>> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final boolean trace = log.isTraceEnabled();

   private final Cache<K, V> cache;
   private final boolean remoteIterator;
   private final ToIntFunction<Object> toIntFunction;
   private final CacheStream<K> inMemoryStream;
   private final PersistenceManager persistenceManager;

   public PersistencKeyStreamSupplier(Cache<K, V> cache, boolean remoteIterator, ToIntFunction<Object> toIntFunction,
         CacheStream<K> inMemoryStream, PersistenceManager persistenceManager) {
      this.cache = cache;
      this.remoteIterator = remoteIterator;
      this.toIntFunction = toIntFunction;
      this.inMemoryStream = inMemoryStream;
      this.persistenceManager = persistenceManager;
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
         Publisher<K> publisher;
         CacheStream<K> inMemoryStream = this.inMemoryStream;
         Set<K> seenKeys = new HashSet<>(2048);
         if (segmentsToFilter != null) {
            inMemoryStream = inMemoryStream.filterKeySegments(segmentsToFilter);
            publisher = persistenceManager.publishKeys(segmentsToFilter, k -> !seenKeys.contains(k), null,
                  PersistenceManager.AccessMode.BOTH);

         } else {
            publisher = persistenceManager.publishKeys(k -> !seenKeys.contains(k), null, PersistenceManager.AccessMode.BOTH);
         }
         CloseableIterator<K> localIterator = new CloseableIteratorMapper<>(Closeables.iterator(inMemoryStream), k -> {
            seenKeys.add(k);
            return k;
         });
         // TODO: need to handle close here
         Iterable<K> iterable = () -> new DoubleIterator<>(localIterator,
               () -> org.infinispan.util.Closeables.iterator(publisher, 128));

         // TODO: we should change how we access stores based on if parallel or not
         stream = StreamSupport.stream(iterable.spliterator(), parallel);
      }
      return stream;
   }

   @Override
   public CloseableIterator<K> removableIterator(CloseableIterator<K> realIterator) {
      if (remoteIterator) {
         return realIterator;
      }
      return new RemovableCloseableIterator<>(realIterator, cache::remove);
   }
}
