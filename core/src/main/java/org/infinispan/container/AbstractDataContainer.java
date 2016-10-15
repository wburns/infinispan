package org.infinispan.container;

import static org.infinispan.commons.util.Util.toStr;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.CaffeineConcurrentMap;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.PeekableMap;
import org.infinispan.commons.util.concurrent.ParallelIterableMap;
import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8.EvictionListener;
import org.infinispan.commons.util.concurrent.jdk8backported.EntrySizeCalculator;
import org.infinispan.container.entries.CacheEntrySizeCalculator;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MarshalledValueEntrySizeCalculator;
import org.infinispan.eviction.ActivationManager;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.eviction.EvictionType;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.KeyFilter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.marshall.core.WrappedByteArraySizeCalculator;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.L1Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.CoreImmutables;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.WithinThreadExecutor;

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
public abstract class AbstractDataContainer<K, V> implements DataContainer<K, V> {

   private static final Log log = LogFactory.getLog(AbstractDataContainer.class);
   private static final boolean trace = log.isTraceEnabled();

   protected final ConcurrentMap<K, InternalCacheEntry<K, V>> entries;
   protected InternalEntryFactory entryFactory;
   protected EvictionManager evictionManager;
   protected PassivationManager passivator;
   protected ActivationManager activator;
   protected PersistenceManager pm;
   protected TimeService timeService;
   protected CacheNotifier cacheNotifier;
   protected ExpirationManager<K, V> expirationManager;

   public AbstractDataContainer(int concurrencyLevel) {
      // If no comparing implementations passed, could fallback on JDK CHM
      entries = CollectionFactory.makeConcurrentParallelMap(128, concurrencyLevel);
   }

   public AbstractDataContainer(int concurrencyLevel,
                                Equivalence<? super K> keyEq) {
      // If at least one comparing implementation give, use ComparingCHMv8
      entries = CollectionFactory.makeConcurrentParallelMap(128, concurrencyLevel, keyEq, AnyEquivalence.getInstance());
   }

   private static <K, V> Caffeine<K, V> caffeineBuilder() {
      return (Caffeine<K, V>) Caffeine.newBuilder();
   }

   protected AbstractDataContainer(int concurrencyLevel, long thresholdSize,
                                   EvictionStrategy strategy, EvictionThreadPolicy policy,
                                   Equivalence<? super K> keyEquivalence, EvictionType thresholdPolicy) {
      DefaultEvictionListener evictionListener;
      // translate eviction policy and strategy
      switch (policy) {
         case PIGGYBACK:
         case DEFAULT:
            evictionListener = new DefaultEvictionListener();
            break;
         default:
            throw new IllegalArgumentException("No such eviction thread policy " + strategy);
      }

      Caffeine<K, InternalCacheEntry<K, V>> caffeine = caffeineBuilder();

      switch (thresholdPolicy) {
         case MEMORY:
            CacheEntrySizeCalculator<K, V> calc = new CacheEntrySizeCalculator<>(new WrappedByteArraySizeCalculator<>(
                  new MarshalledValueEntrySizeCalculator()));
            caffeine.weigher((k, v) -> (int) calc.calculateSize(k, v)).maximumWeight(thresholdSize);
            break;
         case COUNT:
            caffeine.maximumSize(thresholdSize);
            break;
         default:
            throw new UnsupportedOperationException("Policy not supported: " + thresholdPolicy);
      }
      entries = new CaffeineConcurrentMap<>(applyListener(caffeine, evictionListener).build());
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
    * Method invoked when memory policy is used
    * @param concurrencyLevel
    * @param thresholdSize
    * @param strategy
    * @param policy
    * @param keyEquivalence
    * @param sizeCalculator
    */
   protected AbstractDataContainer(int concurrencyLevel, long thresholdSize,
                                   EvictionStrategy strategy, EvictionThreadPolicy policy,
                                   Equivalence<? super K> keyEquivalence,
                                   EntrySizeCalculator<? super K, ? super V> sizeCalculator) {
      DefaultEvictionListener evictionListener;
      // translate eviction policy and strategy
      switch (policy) {
         case PIGGYBACK:
         case DEFAULT:
            evictionListener = new DefaultEvictionListener();
            break;
         default:
            throw new IllegalArgumentException("No such eviction thread policy " + policy);
      }

      EntrySizeCalculator<K, InternalCacheEntry<K, V>> calc = new CacheEntrySizeCalculator<>(sizeCalculator);

      com.github.benmanes.caffeine.cache.Cache<K, InternalCacheEntry<K, V>> cache = applyListener(Caffeine.newBuilder()
            .weigher((K k, InternalCacheEntry<K, V> v) -> (int) calc.calculateSize(k, v))
            .maximumWeight(thresholdSize), evictionListener)
            .build();

      entries = new CaffeineConcurrentMap<>(cache);
   }

   @Inject
   public void initialize(EvictionManager evictionManager, PassivationManager passivator,
                          InternalEntryFactory entryFactory, ActivationManager activator, PersistenceManager clm,
                          TimeService timeService, CacheNotifier cacheNotifier, ExpirationManager<K, V> expirationManager) {
      this.evictionManager = evictionManager;
      this.passivator = passivator;
      this.entryFactory = entryFactory;
      this.activator = activator;
      this.pm = clm;
      this.timeService = timeService;
      this.cacheNotifier = cacheNotifier;
      this.expirationManager = expirationManager;
   }

   @Override
   public InternalCacheEntry<K, V> peek(Object key) {
      if (entries instanceof PeekableMap) {
         return ((PeekableMap<K, InternalCacheEntry<K, V>>)entries).peek(key);
      }
      return entries.get(key);
   }

   @Override
   public InternalCacheEntry<K, V> get(Object k) {
      InternalCacheEntry<K, V> e = entries.get(k);
      if (e != null && e.canExpire()) {
         long currentTimeMillis = timeService.wallClockTime();
         if (e.isExpired(currentTimeMillis)) {
            expirationManager.handleInMemoryExpiration(e, currentTimeMillis);
            e = null;
         } else {
            e.touch(currentTimeMillis);
         }
      }
      return e;
   }

   @Override
   public boolean containsKey(Object k) {
      InternalCacheEntry<K, V> ice = peek(k);
      if (ice != null && ice.canExpire() && ice.isExpired(timeService.wallClockTime())) {
         entries.remove(k);
         ice = null;
      }
      return ice != null;
   }

   private Policy.Eviction<K, V> eviction() {
      if (entries instanceof CaffeineConcurrentMap) {
         com.github.benmanes.caffeine.cache.Cache<K, V> cache = ((CaffeineConcurrentMap) entries).getCache();
         Optional<Policy.Eviction<K, V>> eviction = cache.policy().eviction();
         if (eviction.isPresent()) {
            return eviction.get();
         }
      }
      throw new UnsupportedOperationException();
   }

   @Override
   public long capacity() {
      Policy.Eviction<K, V> evict = eviction();
      return evict.getMaximum();
   }

   @Override
   public void resize(long newSize) {
      Policy.Eviction<K, V> evict = eviction();
      evict.setMaximum(newSize);
   }

   @Override
   public int size() {
      int size = 0;
      // We have to loop through to make sure to remove expired entries
      for (Iterator<InternalCacheEntry<K, V>> iter = iterator(); iter.hasNext(); ) {
         iter.next();
         if (++size == Integer.MAX_VALUE) return Integer.MAX_VALUE;
      }
      return size;
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
   public void purgeExpired() {
      // Just calls to expiration manager to handle this
      expirationManager.processExpiration();
   }

   @Override
   public InternalCacheEntry<K, V> compute(K key, ComputeAction<K, V> action) {
      // TODO: guessing this should be only in DefaultDataContainer
      return entries.compute(key, (k, oldEntry) -> {
         InternalCacheEntry<K, V> newEntry = action.compute(k, oldEntry, entryFactory);
         if (newEntry == oldEntry) {
            return oldEntry;
         } else if (newEntry == null) {
            activator.onRemove(k, false);
            return null;
         }
         activator.onUpdate(k, oldEntry == null);
         if (trace)
            log.tracef("Store %s in container", newEntry);
         return newEntry;
      });
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
   public void executeTask(final KeyValueFilter<? super K, ? super V> filter, final BiConsumer<? super K, InternalCacheEntry<K, V>> action)
         throws InterruptedException {
      if (filter == null)
         throw new IllegalArgumentException("No filter specified");
      if (action == null)
         throw new IllegalArgumentException("No action specified");

      ParallelIterableMap<K, InternalCacheEntry<K, V>> map = (ParallelIterableMap<K, InternalCacheEntry<K, V>>) entries;
      map.forEach(32, (K key, InternalCacheEntry<K, V> value) -> {
         if (filter.accept(key, value.getValue(), value.getMetadata())) {
            action.accept(key, value);
         }
      });
      //TODO figure out the way how to do interruption better (during iteration)
      if(Thread.currentThread().isInterrupted()){
         throw new InterruptedException();
      }
   }
}
