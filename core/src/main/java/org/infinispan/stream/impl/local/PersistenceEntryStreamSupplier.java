package org.infinispan.stream.impl.local;

import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
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
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.DoubleIterator;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

/**
 * @author wburns
 * @since 9.3
 */
public class PersistenceEntryStreamSupplier<K, V> implements AbstractLocalCacheStream.StreamSupplier<CacheEntry<K, V>, Stream<CacheEntry<K, V>>> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final boolean trace = log.isTraceEnabled();

   private final Cache<K, V> cache;
   private final boolean remoteIterator;
   private final InternalEntryFactory iceFactory;
   private final ToIntFunction<Object> toIntFunction;
   private final CacheStream<CacheEntry<K, V>> inMemoryStream;
   private final PersistenceManager persistenceManager;

   public PersistenceEntryStreamSupplier(Cache<K, V> cache, boolean remoteIterator, InternalEntryFactory iceFactory,
         ToIntFunction<Object> toIntFunction, CacheStream<CacheEntry<K, V>> inMemoryStream,
         PersistenceManager persistenceManager) {
      this.cache = cache;
      this.remoteIterator = remoteIterator;
      this.iceFactory = iceFactory;
      this.toIntFunction = toIntFunction;
      this.inMemoryStream = inMemoryStream;
      this.persistenceManager = persistenceManager;
   }

   @Override
   public Stream<CacheEntry<K, V>> buildStream(IntSet segmentsToFilter, Set<?> keysToFilter, boolean parallel) {
      Stream<CacheEntry<K, V>> stream;
      if (keysToFilter != null) {
         if (trace) {
            log.tracef("Applying key filtering %s", keysToFilter);
         }
         // Make sure we aren't going remote to retrieve these
         AdvancedCache<K, V> advancedCache = AbstractDelegatingCache.unwrapCache(cache).getAdvancedCache()
               .withFlags(Flag.CACHE_MODE_LOCAL);
         Stream<?> keyStream = parallel ? keysToFilter.parallelStream() : keysToFilter.stream();
         stream = keyStream
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
         Publisher<CacheEntry<K, V>> publisher;
         CacheStream<CacheEntry<K, V>> inMemoryStream = this.inMemoryStream;
         Set<K> seenKeys = new HashSet<>(2048);
         Function<MarshalledEntry<K, V>, CacheEntry<K, V>> function =
               me -> (CacheEntry<K, V>) PersistenceUtil.convert(me, iceFactory);
         if (segmentsToFilter != null) {
            inMemoryStream = inMemoryStream.filterKeySegments(segmentsToFilter);
            publisher = persistenceManager.publishEntries(segmentsToFilter, k -> !seenKeys.contains(k), function, true,
                  true, PersistenceManager.AccessMode.BOTH);

         } else {
            publisher = persistenceManager.publishEntries(k -> !seenKeys.contains(k), function, true, true,
                  PersistenceManager.AccessMode.BOTH);
         }
         CloseableIterator<CacheEntry<K, V>> localIterator = new CloseableIteratorMapper<>(Closeables.iterator(inMemoryStream), e -> {
            seenKeys.add(e.getKey());
            return e;
         });
         // TODO: need to handle close here
         Iterable<CacheEntry<K, V>> iterable = () -> new DoubleIterator<>(localIterator,
               () -> org.infinispan.util.Closeables.iterator(publisher, 128));

         // TODO: we should change how we access stores based on if parallel or not
         stream = StreamSupport.stream(iterable.spliterator(), parallel);
      }
      return stream;
   }

   @Override
   public CloseableIterator<CacheEntry<K, V>> removableIterator(CloseableIterator<CacheEntry<K, V>> realIterator) {
      if (remoteIterator) {
         return realIterator;
      }
      return new RemovableCloseableIterator<>(realIterator, e -> cache.remove(e.getKey(), e.getValue()));
   }
}
