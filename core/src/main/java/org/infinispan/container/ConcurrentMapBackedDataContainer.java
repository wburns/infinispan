package org.infinispan.container;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.PeekableMap;
import org.infinispan.commons.util.concurrent.ParallelIterableMap;
import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8;
import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8.EvictionListener;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.eviction.ActivationManager;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.KeyFilter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.L1Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.CoreImmutables;
import org.infinispan.util.TimeService;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.BOTH;

/**
 * DefaultDataContainer is both eviction and non-eviction based data container.
 *
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @author Vladimir Blagojevic
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @author William Burns
 *
 * @since 9.0
 */
@ThreadSafe
public abstract class ConcurrentMapBackedDataContainer<K, V> implements DataContainer<K, V> {

   private static final Log log = LogFactory.getLog(ConcurrentMapBackedDataContainer.class);
   private static final boolean trace = log.isTraceEnabled();

   protected final boolean passivationEnabled;

   protected InternalEntryFactory entryFactory;
   protected EvictionManager evictionManager;
   protected PassivationManager passivator;
   protected ActivationManager activator;
   protected PersistenceManager pm;
   protected TimeService timeService;
   protected CacheNotifier cacheNotifier;
   protected ExpirationManager<K, V> expirationManager;

   protected ConcurrentMapBackedDataContainer(boolean passivationEnabled) {
      this.passivationEnabled = passivationEnabled;
   }

   protected abstract ConcurrentMap<K, InternalCacheEntry<K, V>> getMap();

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
      ConcurrentMap<K, InternalCacheEntry<K, V>> map = getMap();
      if (map instanceof PeekableMap) {
         return ((PeekableMap<K, InternalCacheEntry<K, V>>) map).peek(key);
      }
      return map.get(key);
   }

   @Override
   public InternalCacheEntry<K, V> get(Object k) {
      InternalCacheEntry<K, V> e = getMap().get(k);
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
   public void put(K k, V v, Metadata metadata) {
      boolean l1Entry = false;
      if (metadata instanceof L1Metadata) {
         metadata = ((L1Metadata) metadata).metadata();
         l1Entry = true;
      }
      InternalCacheEntry<K, V> e = getMap().get(k);

      if (trace) {
         log.tracef("Creating new ICE for writing. Existing=%s, metadata=%s, new value=%s", e, metadata, v);
      }
      final InternalCacheEntry<K, V> copy;
      if (l1Entry) {
         copy = entryFactory.createL1(k, v, metadata);
      } else if (e != null) {
         copy = entryFactory.update(e, v, metadata);
      } else {
         // this is a brand-new entry
         copy = entryFactory.create(k, v, metadata);
      }

      if (trace)
         log.tracef("Store %s in container", copy);

      if (passivationEnabled) {
         getMap().compute(copy.getKey(), (key, entry) -> {
            activator.onUpdate(key, entry == null);
            return copy;
         });
      } else {
         getMap().put(copy.getKey(), copy);
      }
   }

   @Override
   public boolean containsKey(Object k) {
      InternalCacheEntry<K, V> ice = peek(k);
      if (ice != null && ice.canExpire() && ice.isExpired(timeService.wallClockTime())) {
         getMap().remove(k, ice);
         ice = null;
      }
      return ice != null;
   }

   @Override
   public InternalCacheEntry<K, V> remove(Object k) {
      final InternalCacheEntry<K, V> e;
      if (passivationEnabled) {
         final InternalCacheEntry<K, V>[] reference = new InternalCacheEntry[1];
         getMap().compute((K) k, (key, entry) -> {
            activator.onRemove(key, entry == null);
            reference[0] = entry;
            return null;
         });
         e = reference[0];
      } else {
         e = getMap().remove(k);
      }
      return e == null || (e.canExpire() && e.isExpired(timeService.wallClockTime())) ? null : e;
   }

   @Override
   public long capacity() {
      if (getMap() instanceof BoundedEquivalentConcurrentHashMapV8) {
         BoundedEquivalentConcurrentHashMapV8<K, V> resizable = (BoundedEquivalentConcurrentHashMapV8<K, V>)getMap();
         return resizable.capacity();
      } else throw new UnsupportedOperationException();
   }

   @Override
   public void resize(long newSize) {
      if (getMap() instanceof BoundedEquivalentConcurrentHashMapV8) {
         BoundedEquivalentConcurrentHashMapV8<K, V> resizable = (BoundedEquivalentConcurrentHashMapV8<K, V>)getMap();
         resizable.resize(newSize);
      } else throw log.cannotResizeUnboundedContainer();
   }

   @Override
   public int size() {
      int size = 0;
      // We have to loop through to make sure to remove expired getMap()
      for (Iterator<InternalCacheEntry<K, V>> iter = iterator(); iter.hasNext(); ) {
         iter.next();
         if (++size == Integer.MAX_VALUE) return Integer.MAX_VALUE;
      }
      return size;
   }

   @Override
   public int sizeIncludingExpired() {
      return getMap().size();
   }

   @Override
   public void clear() {
      log.tracef("Clearing data container");
      getMap().clear();
   }

   @Override
   public Set<K> keySet() {
      return Collections.unmodifiableSet(getMap().keySet());
   }

   @Override
   public Collection<V> values() {
      return new Values();
   }

   @Override
   public Set<InternalCacheEntry<K, V>> entrySet() {
      return new EntrySet();
   }

   @Override
   public void purgeExpired() {
      // Just calls to expiration manager to handle this
      expirationManager.processExpiration();
   }

   @Override
   public void evict(K key) {
      if (passivationEnabled) {
         getMap().computeIfPresent(key, (o, entry) -> {
            passivator.passivate(entry);
            return null;
         });
      } else {
         getMap().remove(key);
      }
   }

   @Override
   public InternalCacheEntry<K, V> compute(K key, ComputeAction<K, V> action) {
      return getMap().compute(key, (k, oldEntry) -> {
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

   @Override
   public Iterator<InternalCacheEntry<K, V>> iterator() {
      return new EntryIterator(getMap().values().iterator(), false);
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iteratorIncludingExpired() {
      return new EntryIterator(getMap().values().iterator(), true);
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
         if (entry.getValue().isEvicted()) {
            onEntryChosenForEviction(entry);
         } else if (pm != null) {
            pm.deleteFromAllStores(entry.getKey(), BOTH);
         }
      }
   }

   private class ImmutableEntryIterator extends EntryIterator {
      ImmutableEntryIterator(Iterator<InternalCacheEntry<K, V>> it){
         super(it, false);
      }

      @Override
      public InternalCacheEntry<K, V> next() {
         return CoreImmutables.immutableInternalCacheEntry(super.next());
      }
   }

   public class EntryIterator implements Iterator<InternalCacheEntry<K, V>> {

      private final Iterator<InternalCacheEntry<K, V>> it;
      private final boolean includeExpired;

      private InternalCacheEntry<K, V> next;

      EntryIterator(Iterator<InternalCacheEntry<K, V>> it, boolean includeExpired){
         this.it=it;
         this.includeExpired = includeExpired;
      }

      private InternalCacheEntry<K, V> getNext() {
         boolean initializedTime = false;
         long now = 0;
         while (it.hasNext()) {
            InternalCacheEntry<K, V> entry = it.next();
            if (includeExpired || !entry.canExpire()) {
               return entry;
            } else {
               if (!initializedTime) {
                  now = timeService.wallClockTime();
                  initializedTime = true;
               }
               if (!entry.isExpired(now)) {
                  return entry;
               }
            }
         }
         return null;
      }

      @Override
      public InternalCacheEntry<K, V> next() {
         if (next == null) {
            next = getNext();
         }
         if (next == null) {
            throw new NoSuchElementException();
         }
         InternalCacheEntry<K, V> toReturn = next;
         next = null;
         return toReturn;
      }

      @Override
      public boolean hasNext() {
         if (next == null) {
            next = getNext();
         }
         return next != null;
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException();
      }
   }

   /**
    * Minimal implementation needed for unmodifiable Set
    *
    */
   private class EntrySet extends AbstractSet<InternalCacheEntry<K, V>> {

      @Override
      public boolean contains(Object o) {
         if (!(o instanceof Entry)) {
            return false;
         }

         @SuppressWarnings("rawtypes")
         Entry e = (Entry) o;
         InternalCacheEntry ice = getMap().get(e.getKey());
         if (ice == null) {
            return false;
         }
         return ice.getValue().equals(e.getValue());
      }

      @Override
      public Iterator<InternalCacheEntry<K, V>> iterator() {
         return new ImmutableEntryIterator(getMap().values().iterator());
      }

      @Override
      public int size() {
         return getMap().size();
      }

      @Override
      public String toString() {
         return getMap().toString();
      }
   }

   /**
    * Minimal implementation needed for unmodifiable Collection
    *
    */
   private class Values extends AbstractCollection<V> {
      @Override
      public Iterator<V> iterator() {
         return new ValueIterator(getMap().values().iterator());
      }

      @Override
      public int size() {
         return getMap().size();
      }
   }

   private static class ValueIterator<K, V> implements Iterator<V> {
      Iterator<InternalCacheEntry<K, V>> currentIterator;

      private ValueIterator(Iterator<InternalCacheEntry<K, V>> it) {
         currentIterator = it;
      }

      @Override
      public boolean hasNext() {
         return currentIterator.hasNext();
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException();
      }

      @Override
      public V next() {
         return currentIterator.next().getValue();
      }
   }

   @Override
   public void executeTask(final KeyFilter<? super K> filter, final BiConsumer<? super K, InternalCacheEntry<K, V>> action)
         throws InterruptedException {
      if (filter == null)
         throw new IllegalArgumentException("No filter specified");
      if (action == null)
         throw new IllegalArgumentException("No action specified");

      ConcurrentMap<K, InternalCacheEntry<K, V>> map = getMap();
      BiConsumer<K, InternalCacheEntry<K, V>> consumer = (key, value) -> {
         if (filter.accept(key)) {
            action.accept(key, value);
         }
      };
      if (map instanceof ParallelIterableMap) {
         ((ParallelIterableMap<K, InternalCacheEntry<K, V>>) map).forEach(32, consumer);
      } else {
         map.forEach(consumer);
      }
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

      ConcurrentMap<K, InternalCacheEntry<K, V>> map = getMap();
      BiConsumer<K, InternalCacheEntry<K, V>> consumer = (key, value) -> {
         if (filter.accept(key, value.getValue(), value.getMetadata())) {
            action.accept(key, value);
         }
      };
      if (map instanceof ParallelIterableMap) {
         ((ParallelIterableMap<K, InternalCacheEntry<K, V>>) map).forEach(32, consumer);
      } else {
         map.forEach(consumer);
      }
      //TODO figure out the way how to do interruption better (during iteration)
      if(Thread.currentThread().isInterrupted()){
         throw new InterruptedException();
      }
   }
}
