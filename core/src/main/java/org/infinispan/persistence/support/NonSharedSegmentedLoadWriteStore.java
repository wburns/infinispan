package org.infinispan.persistence.support;

import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.IntConsumer;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

import org.infinispan.Cache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.AbstractNonSharedSegmentedConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.HashConfiguration;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.persistence.InitializationContextImpl;
import org.infinispan.persistence.factory.CacheStoreFactoryRegistry;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.remoting.transport.Address;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;

/**
 * @author wburns
 * @since 9.4
 */
@Listener(observation = Listener.Observation.PRE)
public class NonSharedSegmentedLoadWriteStore<K, V, T extends AbstractNonSharedSegmentedConfiguration> extends AbstractSegmentedAdvancedLoadWriteStore<K, V> {
   private final AbstractNonSharedSegmentedConfiguration<T> configuration;
   Cache<K, V> cache;
   ExecutorService executorService;
   CacheStoreFactoryRegistry cacheStoreFactoryRegistry;
   KeyPartitioner keyPartitioner;
   InitializationContext ctx;
   Scheduler scheduler;
   Address localNode;

   AtomicReferenceArray<AdvancedLoadWriteStore<K, V>> stores;

   public NonSharedSegmentedLoadWriteStore(AbstractNonSharedSegmentedConfiguration<T> configuration) {
      this.configuration = configuration;
   }

   @Override
   public ToIntFunction<Object> getKeyMapper() {
      return keyPartitioner::getSegment;
   }

   @Override
   public MarshalledEntry<K, V> load(int segment, Object key) {
      return stores.get(segment).load(key);
   }

   @Override
   public boolean contains(int segment, Object key) {
      return stores.get(segment).contains(key);
   }

   @Override
   public void write(int segment, MarshalledEntry<? extends K, ? extends V> entry) {
      stores.get(segment).write(entry);
   }

   @Override
   public boolean delete(int segment, Object key) {
      return stores.get(segment).delete(key);
   }

   @Override
   public int size(int segment) {
      return stores.get(segment).size();
   }

   @Override
   public Publisher<K> publishKeys(int segment, Predicate<? super K> filter) {
      AdvancedLoadWriteStore<K, V> alws = stores.get(segment);
      if (alws == null) {
         return Flowable.empty();
      } else {
         return alws.publishKeys(filter);
      }
   }

   @Override
   public Publisher<MarshalledEntry<K, V>> publishEntries(int segment, Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata) {
      AdvancedLoadWriteStore<K, V> alws = stores.get(segment);
      if (alws == null) {
         return Flowable.empty();
      } else {
         return alws.publishEntries(filter, fetchValue, fetchMetadata);
      }
   }

   @Override
   public Publisher<MarshalledEntry<K, V>> publishEntries(Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata) {
      return Flowable.range(0, stores.length())
            .parallel()
            .runOn(scheduler)
            .flatMap(i -> {
               AdvancedLoadWriteStore<K, V> alws = stores.get(i);
               if (alws == null) {
                  return Flowable.empty();
               } else {
                  return alws.publishEntries(filter, fetchValue, fetchMetadata);
               }
            })
            .sequential();
   }

   @Override
   public void clear(int segment) {
      stores.get(segment).clear();
   }

   @Override
   public void purge(int segment, Executor threadPool, PurgeListener<? super K> listener) {
      stores.get(segment).purge(threadPool, listener);
   }

   @Override
   public void deleteBatch(Iterable<Object> keys) {
      // We use async methods, but we only ever use this thread so it is fine
      Flowable.fromIterable(keys)
            // Separate out batches by segment
            .groupBy(keyPartitioner::getSegment)
            .forEach(groupedFlowable ->
                  groupedFlowable
                        // Currently we have this iterable in memory so there is no reason to stream batches
                        .toList()
                        .subscribe(batch -> stores.get(groupedFlowable.getKey()).deleteBatch(batch))
            );
   }

   @Override
   public void writeBatch(Iterable<MarshalledEntry<? extends K, ? extends V>> marshalledEntries) {
      // We use async methods, but we only ever use this thread so it is fine
      Flowable.fromIterable(marshalledEntries)
            // Separate out batches by segment
            .groupBy(me -> keyPartitioner.getSegment(me.getKey()))
            .forEach(groupedFlowable ->
                  groupedFlowable
                        // Currently we have this iterable in memory so there is no reason to stream batches
                        .toList()
                        .subscribe(batch -> stores.get(groupedFlowable.getKey()).writeBatch(batch))
            );
   }

   @Override
   public void init(InitializationContext ctx) {
      this.ctx = ctx;
      cache = ctx.getCache();
      executorService = ctx.getExecutor();
   }

   @Override
   public void start() {
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      cacheStoreFactoryRegistry = componentRegistry.getComponent(CacheStoreFactoryRegistry.class);
      cache.getAdvancedCache().getDistributionManager();
      cache.addListener(this);

      scheduler = Schedulers.from(executorService);

      HashConfiguration hashConfiguration = cache.getCacheConfiguration().clustering().hash();
      keyPartitioner = hashConfiguration.keyPartitioner();
      stores = new AtomicReferenceArray<>(hashConfiguration.numSegments());

      CacheMode mode = cache.getCacheConfiguration().clustering().cacheMode();
      // Local or remote cache we just instantiate all the stores immediately
      if (!mode.isClustered() || mode.isReplicated()) {
         for (int i = 0; i < stores.length(); ++i) {
            startNewStoreForSegment(i);
         }
      }
   }

   private void startNewStoreForSegment(int segment) {
      if (stores.get(segment) == null) {
         T storeConfiguration = configuration.newConfigurationFrom(segment);
         AdvancedLoadWriteStore<K, V> newStore = (AdvancedLoadWriteStore<K, V>) cacheStoreFactoryRegistry.createInstance(storeConfiguration);
         newStore.init(new InitializationContextImpl(storeConfiguration, cache, ctx.getMarshaller(), ctx.getTimeService(),
               ctx.getByteBufferFactory(), ctx.getMarshalledEntryFactory(), ctx.getExecutor()));
         newStore.start();
         stores.set(segment, newStore);
      }
   }

   @Override
   public void stop() {

   }

   @TopologyChanged
   public void onTopologyChange(TopologyChangedEvent<K, V> topologyChangedEvent) {
      if (topologyChangedEvent.isPre()) {
         ConsistentHash ch = topologyChangedEvent.getWriteConsistentHashAtEnd();
         Set<Integer> segments = ch.getSegmentsForOwner(localNode);
         if (segments instanceof IntSet) {
            ((IntSet) segments).forEach((IntConsumer) this::startNewStoreForSegment);
         } else {
            segments.forEach(this::startNewStoreForSegment);
         }
      } else {
         // TODO: need to remove stores heres
      }
   }
}
