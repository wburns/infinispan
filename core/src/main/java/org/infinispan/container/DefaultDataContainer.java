package org.infinispan.container;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.EntrySizeCalculator;
import org.infinispan.commons.util.EvictionListener;
import org.infinispan.commons.util.FilterIterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.HashConfiguration;
import org.infinispan.container.entries.CacheEntrySizeCalculator;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.PrimitiveEntrySizeCalculator;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.eviction.ActivationManager;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.eviction.EvictionType;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.filter.KeyFilter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.marshall.core.WrappedByteArraySizeCalculator;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.WithinThreadExecutor;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.RemovalCause;

import net.jcip.annotations.ThreadSafe;

/**
 * DefaultDataContainer is both eviction and non-eviction based data container.
 *
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @author Vladimir Blagojevic
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 *
 * @since 4.0
 */
@ThreadSafe
public class DefaultDataContainer<K, V> extends AbstractSegmentedDataContainer<K, V> {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final boolean trace = log.isTraceEnabled();

   private final ConcurrentMap<K, InternalCacheEntry<K, V>> entries;
   private final Cache<K, InternalCacheEntry<K, V>> evictionCache;

   @Inject protected InternalEntryFactory entryFactory;
   @Inject private EvictionManager evictionManager;
   @Inject private PassivationManager passivator;
   @Inject private ActivationManager activator;
   @Inject private PersistenceManager pm;
   @Inject private TimeService timeService;
   @Inject private CacheNotifier cacheNotifier;
   @Inject private ExpirationManager<K, V> expirationManager;
   @Inject private org.infinispan.Cache<K, V> cache;
   private KeyPartitioner keyPartitioner;

   public DefaultDataContainer(int concurrencyLevel) {
      // If no comparing implementations passed, could fallback on JDK CHM
      entries = CollectionFactory.makeConcurrentParallelMap(128, concurrencyLevel);
      evictionCache = null;
   }

   private static <K, V> Caffeine<K, V> caffeineBuilder() {
      return (Caffeine<K, V>) Caffeine.newBuilder();
   }

   protected DefaultDataContainer(int concurrencyLevel, long thresholdSize, EvictionType thresholdPolicy) {
      DefaultEvictionListener evictionListener = new DefaultEvictionListener();
      Caffeine<K, InternalCacheEntry<K, V>> caffeine = caffeineBuilder();

      switch (thresholdPolicy) {
         case MEMORY:
            CacheEntrySizeCalculator<K, V> calc = new CacheEntrySizeCalculator<>(new WrappedByteArraySizeCalculator<>(
                  new PrimitiveEntrySizeCalculator()));
            caffeine.weigher((k, v) -> (int) calc.calculateSize(k, v)).maximumWeight(thresholdSize);
            break;
         case COUNT:
            caffeine.maximumSize(thresholdSize);
            break;
         default:
            throw new UnsupportedOperationException("Policy not supported: " + thresholdPolicy);
      }
      evictionCache = applyListener(caffeine, evictionListener).build();
      entries = evictionCache.asMap();
   }

   private Caffeine<K, InternalCacheEntry<K, V>> applyListener(Caffeine<K, InternalCacheEntry<K, V>> caffeine, DefaultEvictionListener listener) {
      return caffeine.executor(new WithinThreadExecutor()).removalListener((k, v, c) -> {
         switch (c) {
            case SIZE:
               listener.onEntryEviction(Collections.singletonMap(k, v));
               break;
            case EXPLICIT:
               listener.onEntryRemoved(new ImmortalCacheEntry(k, v));
               break;
            case REPLACED:
               listener.onEntryActivated(k);
               break;
         }
      }).writer(new CacheWriter<K, InternalCacheEntry<K, V>>() {
         @Override
         public void write(K key, InternalCacheEntry<K, V> value) {

         }

         @Override
         public void delete(K key, InternalCacheEntry<K, V> value, RemovalCause cause) {
            if (cause == RemovalCause.SIZE) {
               listener.onEntryChosenForEviction(new ImmortalCacheEntry(key, value));
            }
         }
      });
   }

   /**
    * Method invoked when memory policy is used. This calculator only calculates the given key and value.
    * @param concurrencyLevel
    * @param thresholdSize
    * @param sizeCalculator
    */
   protected DefaultDataContainer(int concurrencyLevel, long thresholdSize,
                                  EntrySizeCalculator<? super K, ? super V> sizeCalculator) {
      this(thresholdSize, new CacheEntrySizeCalculator<>(sizeCalculator));
   }

   /**
    * Constructor that allows user to provide a size calculator that also handles the cache entry and metadata.
    * @param thresholdSize
    * @param sizeCalculator
    */
   protected DefaultDataContainer(long thresholdSize,
         EntrySizeCalculator<? super K, ? super InternalCacheEntry<K, V>> sizeCalculator) {
      DefaultEvictionListener evictionListener = new DefaultEvictionListener();

      evictionCache = applyListener(Caffeine.newBuilder()
            .weigher((K k, InternalCacheEntry<K, V> v) -> (int) sizeCalculator.calculateSize(k, v))
            .maximumWeight(thresholdSize), evictionListener)
            .build();

      entries = evictionCache.asMap();
   }

   public static <K, V> DefaultDataContainer<K, V> boundedDataContainer(int concurrencyLevel, long maxEntries,
            EvictionType thresholdPolicy) {
      return new DefaultDataContainer<>(concurrencyLevel, maxEntries, thresholdPolicy);
   }

   public static <K, V> DefaultDataContainer<K, V> boundedDataContainer(int concurrencyLevel, long maxEntries,
                                                                        EntrySizeCalculator<? super K, ? super V> sizeCalculator) {
      return new DefaultDataContainer<>(concurrencyLevel, maxEntries, sizeCalculator);
   }

   public static <K, V> DefaultDataContainer<K, V> unBoundedDataContainer(int concurrencyLevel) {
      return new DefaultDataContainer<>(concurrencyLevel);
   }

   @Start
   public void start() {
      HashConfiguration hashConfiguration = cache.getCacheConfiguration().clustering().hash();
      keyPartitioner = hashConfiguration.keyPartitioner();
   }

   @Override
   protected ConcurrentMap<K, InternalCacheEntry<K, V>> getMapForSegment(int segment) {
      return entries;
   }

   @Override
   protected int getSegmentForKey(Object key) {
      // We always map to same map, so no reason to waste finding out segment
      return -1;
   }

   @Override
   protected Log log() {
      return log;
   }

   @Override
   protected boolean trace() {
      return trace;
   }

   private Policy.Eviction<K, InternalCacheEntry<K, V>> eviction() {
      if (evictionCache != null) {
         Optional<Policy.Eviction<K, InternalCacheEntry<K, V>>> eviction = evictionCache.policy().eviction();
         if (eviction.isPresent()) {
            return eviction.get();
         }
      }
      throw new UnsupportedOperationException();
   }

   @Override
   public long capacity() {
      Policy.Eviction<K, InternalCacheEntry<K, V>> evict = eviction();
      return evict.getMaximum();
   }

   @Override
   public void resize(long newSize) {
      Policy.Eviction<K, InternalCacheEntry<K, V>> evict = eviction();
      evict.setMaximum(newSize);
   }

   @Override
   public int sizeIncludingExpired() {
      return entries.size();
   }

   @Override
   public void clear() {
      log.tracef("Clearing data container");
      entries.clear();
   }

   @Override
   public Set<K> keySet() {
      return Collections.unmodifiableSet(entries.keySet());
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iterator() {
      return new EntryIterator(entries.values().iterator());
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iterator(IntSet segments) {
      return new FilterIterator<>(iterator(),
            ice -> segments.contains(keyPartitioner.getSegment(ice.getKey())));
   }

   @Override
   public Spliterator<InternalCacheEntry<K, V>> spliterator() {
      return new EntrySpliterator(spliteratorIncludingExpired());
   }

   @Override
   public Spliterator<InternalCacheEntry<K, V>> spliteratorIncludingExpired() {
      // Technically this spliterator is distinct, but it won't be set - we assume that is okay for now
      return entries.values().spliterator();
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iteratorIncludingExpired() {
      return entries.values().iterator();
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iteratorIncludingExpired(IntSet segments) {
      return new FilterIterator<>(iteratorIncludingExpired(),
            ice -> segments.contains(keyPartitioner.getSegment(ice.getKey())));
   }

   @Override
   public long evictionSize() {
      Policy.Eviction<K, InternalCacheEntry<K, V>> evict = eviction();
      return evict.weightedSize().orElse(entries.size());
   }

   @Override
   public void removeSegments(IntSet segments) {
      if (!segments.isEmpty()) {
         List<InternalCacheEntry<K, V>> removedEntries;
         if (!listeners.isEmpty()) {
            removedEntries = new ArrayList<>(entries.size());
         } else {
            removedEntries = null;
         }
         Iterator<InternalCacheEntry<K, V>> iter = iteratorIncludingExpired(segments);
         while (iter.hasNext()) {
            InternalCacheEntry<K, V> ice = iter.next();
            if (removedEntries != null) {
               removedEntries.add(ice);
            }
            iter.remove();
         }
         if (removedEntries != null) {
            List<InternalCacheEntry<K, V>> unmod = Collections.unmodifiableList(removedEntries);
            listeners.forEach(c -> c.accept(unmod));
         }
      }
   }

   private final class DefaultEvictionListener implements EvictionListener<K, InternalCacheEntry<K, V>> {

      @Override
      public void onEntryEviction(Map<K, InternalCacheEntry<K, V>> evicted) {
         evictionManager.onEntryEviction(evicted);
      }

      @Override
      public void onEntryChosenForEviction(Entry<K, InternalCacheEntry<K, V>> entry) {
         passivator.passivate(entry.getValue());
      }

      @Override
      public void onEntryActivated(Object key) {
         activator.onUpdate(key, true);
      }

      @Override
      public void onEntryRemoved(Entry<K, InternalCacheEntry<K, V>> entry) {
      }
   }

   @Override
   public void executeTask(final KeyFilter<? super K> filter, final BiConsumer<? super K, InternalCacheEntry<K, V>> action)
         throws InterruptedException {
      if (filter == null)
         throw new IllegalArgumentException("No filter specified");
      if (action == null)
         throw new IllegalArgumentException("No action specified");

      long now = timeService.wallClockTime();
      entries.forEach((K key, InternalCacheEntry<K, V> value) -> {
         if (filter.accept(key) && !value.isExpired(now)) {
            action.accept(key, value);
         }
      });
      //TODO figure out the way how to do interruption better (during iteration)
      if(Thread.currentThread().isInterrupted()){
         throw new InterruptedException();
      }
   }

   @Override
   public void executeTask(final KeyValueFilter<? super K, ? super V> filter, final BiConsumer<? super K, InternalCacheEntry<K, V>> action)
         throws InterruptedException {
      if (filter == null)
         throw new IllegalArgumentException("No filter specified");
      if (action == null)
         throw new IllegalArgumentException("No action specified");

      long now = timeService.wallClockTime();
      entries.forEach((K key, InternalCacheEntry<K, V> value) -> {
         if (filter.accept(key, value.getValue(), value.getMetadata()) && !value.isExpired(now)) {
            action.accept(key, value);
         }
      });
      //TODO figure out the way how to do interruption better (during iteration)
      if(Thread.currentThread().isInterrupted()){
         throw new InterruptedException();
      }
   }
}
