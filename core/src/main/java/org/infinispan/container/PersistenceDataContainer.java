package org.infinispan.container;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.impl.ImmutableContext;
import org.infinispan.filter.KeyFilter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.filter.KeyValueFilterAsKeyFilter;
import org.infinispan.marshall.core.JBossMarshaller;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.TimeService;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.infinispan.persistence.PersistenceUtil.internalMetadata;

/**
 * // TODO: Document this
 *
 * @author wburns
 * @since 8.1
 */
public class PersistenceDataContainer<K, V> implements DataContainer<K, V> {
   private PersistenceManager persistenceManager;
   private InternalEntryFactory factory;
   private TimeService timeService;
   private JBossMarshaller marshaller;

   @Override
   public InternalCacheEntry<K, V> get(Object k) {
      MarshalledEntry<K, V> entry = PersistenceUtil.loadAndCheckExpiration(persistenceManager, k,
              ImmutableContext.INSTANCE, timeService);
      if (entry != null) {
         return PersistenceUtil.convert(entry, factory);
      }
      return null;
   }

   @Override
   public InternalCacheEntry<K, V> peek(Object k) {
      MarshalledEntry<K, V> entry = persistenceManager.loadFromAllStores(k, ImmutableContext.INSTANCE);
      if (entry != null) {
         return PersistenceUtil.convert(entry, factory);
      }
      return null;
   }

   @Override
   public void put(K k, V v, Metadata metadata) {
      persistenceManager.writeToAllStores(new MarshalledEntryImpl(k, v, new InternalMetadataImpl(metadata,
              timeService.time(), timeService.time()), marshaller), PersistenceManager.AccessMode.BOTH);
   }

   @Override
   public boolean containsKey(Object k) {
      return persistenceManager.loadFromAllStores(k, ImmutableContext.INSTANCE) != null;
   }

   @Override
   public InternalCacheEntry<K, V> remove(Object k) {
      // We rely on the fact that currently no one pays attention to remove returning a value
      persistenceManager.deleteFromAllStores(k, PersistenceManager.AccessMode.BOTH);
      return null;
   }

   @Override
   public int size() {
      // TODO: need to clarify which this does for size method
      return persistenceManager.size();
   }

   @Override
   public int sizeIncludingExpired() {
      // TODO: need to clarify which this does for size method
      return persistenceManager.size();
   }

   @Override
   public void clear() {
      persistenceManager.clearAllStores(PersistenceManager.AccessMode.BOTH);
   }

   @Override
   public Set<K> keySet() {
      // TODO: currently this method returns expired keys
      return PersistenceUtil.toKeySet(persistenceManager, KeyFilter.ACCEPT_ALL_FILTER);
   }

   @Override
   public Collection<V> values() {
      return entrySet().stream().map(InternalCacheEntry::getValue).collect(Collectors.toList());
   }

   @Override
   public Set<InternalCacheEntry<K, V>> entrySet() {
      // TODO: currently this method returns expired entries
      return PersistenceUtil.toEntrySet(persistenceManager, KeyFilter.ACCEPT_ALL_FILTER, factory);
   }

   @Override
   public void purgeExpired() {
      // TODO: call to expiration manager
   }

   @Override
   public void evict(K key) {
      // eviction does nothing in this container
   }

   @Override
   public InternalCacheEntry<K, V> compute(K key, ComputeAction<K, V> action) {
      // We rely on the Infinispan to hold the lock while doing this.
      InternalCacheEntry<K, V> prevEntry = get(key);
      InternalCacheEntry<K, V> newEntry = action.compute(key, prevEntry, factory);
      if (prevEntry != null ) {
         if (newEntry != null && !prevEntry.equals(newEntry))
            put(key, newEntry.getValue(), newEntry.getMetadata());
         else
            remove(key);
      } else {
         if (newEntry != null)
            put(key, newEntry.getValue(), newEntry.getMetadata());
      }
      return newEntry;
   }

   @Override
   public void executeTask(KeyFilter<? super K> filter, BiConsumer<? super K, InternalCacheEntry<K, V>> action) throws InterruptedException {
      persistenceManager.processOnAllStores(filter, (marshalledEntry, taskContext) -> {
         InternalCacheEntry<K, V> entry = PersistenceUtil.convert(marshalledEntry, factory);
         action.accept(entry.getKey(), entry);
      }, true, true);
   }

   @Override
   public void executeTask(KeyValueFilter<? super K, ? super V> filter, BiConsumer<? super K, InternalCacheEntry<K, V>> action) throws InterruptedException {
      executeTask(new KeyValueFilterAsKeyFilter<K>(filter), action);
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iterator() {
      // TODO: need to exclude expired
      return entrySet().iterator();
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iteratorIncludingExpired() {
      // TODO: make this not pull everything at once?
      return entrySet().iterator();
   }
}
