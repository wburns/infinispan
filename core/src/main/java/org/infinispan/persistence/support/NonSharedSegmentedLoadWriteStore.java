package org.infinispan.persistence.support;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.IntConsumer;
import java.util.function.ObjIntConsumer;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

import org.infinispan.Cache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.AbstractNonSharedSegmentedConfiguration;
import org.infinispan.configuration.cache.HashConfiguration;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.notifications.Listener;
import org.infinispan.persistence.InitializationContextImpl;
import org.infinispan.persistence.factory.CacheStoreFactoryRegistry;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.util.rxjava.FlowableFromIntSetFunction;
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
   boolean shouldStopSegments;

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
      AdvancedLoadWriteStore<K, V> store = stores.get(segment);
      if (store != null) {
         return store.load(key);
      }
      return null;
   }

   @Override
   public boolean contains(int segment, Object key) {
      AdvancedLoadWriteStore<K, V> store = stores.get(segment);
      return store != null && store.contains(key);
   }

   @Override
   public void write(int segment, MarshalledEntry<? extends K, ? extends V> entry) {
      AdvancedLoadWriteStore<K, V> store = stores.get(segment);
      if (store != null) {
         store.write(entry);
      }
   }

   @Override
   public boolean delete(int segment, Object key) {
      AdvancedLoadWriteStore<K, V> store = stores.get(segment);
      return store != null && store.delete(key);
   }

   @Override
   public int size(int segment) {
      AdvancedLoadWriteStore<K, V> store = stores.get(segment);
      if (store != null) {
         return store.size();
      }
      return 0;
   }

   @Override
   public int size() {
      int size = 0;
      for (int i = 0; i < stores.length(); ++i) {
         AdvancedLoadWriteStore<K, V> store = stores.get(i);
         if (store != null) {
            size += store.size();
            if (size < 0) {
               return Integer.MAX_VALUE;
            }
         }
      }
      return size;
   }

   @Override
   public Flowable<K> publishKeys(IntSet segments, Predicate<? super K> filter) {
      Flowable<Publisher<K>> flowable = new FlowableFromIntSetFunction<>(segments, i -> {
         AdvancedLoadWriteStore<K, V> alws = stores.get(i);
         if (alws != null) {
            return alws.publishKeys(filter);
         }
         return Flowable.empty();
      });
      // Can't chain with this, doesn't like the typing
      // Filter out empty so we don't use a thread for it
      flowable = flowable.filter(f -> f != Flowable.empty());
      return flowable.parallel()
            .runOn(scheduler)
            .flatMap(f -> f)
            .sequential();
   }

   @Override
   public Publisher<K> publishKeys(Predicate<? super K> filter) {
      Flowable<Publisher<K>> flowable = Flowable.range(0, stores.length())
            .map(i -> {
               AdvancedLoadWriteStore<K, V> alws = stores.get(i);
               if (alws == null) {
                  return Flowable.empty();
               } else {
                  return alws.publishKeys(filter);
               }
            });
      // Can't chain with this, doesn't like the typing
      // Filter out empty so we don't use a thread for it
      flowable = flowable.filter(f -> f != Flowable.empty());
      return flowable.parallel()
            .runOn(scheduler)
            .flatMap(f -> f)
            .sequential();
   }

   @Override
   public Publisher<MarshalledEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata) {
      Flowable<Publisher<MarshalledEntry<K, V>>> flowable = new FlowableFromIntSetFunction<>(segments, i -> {
         AdvancedLoadWriteStore<K, V> alws = stores.get(i);
         if (alws != null) {
            return alws.publishEntries(filter, fetchValue, fetchMetadata);
         }
         return Flowable.empty();
      });
      // Cast is required otherwise it complains I can't use != operator with 2 unlike types.... bug anyone?
      // Filter out empty so we don't use a thread for it
      flowable = flowable.filter(f -> (Object) f != Flowable.empty());
      return flowable.parallel()
            .runOn(scheduler)
            .flatMap(f -> f)
            .sequential();
   }

   @Override
   public Publisher<MarshalledEntry<K, V>> publishEntries(Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata) {
      Flowable<Publisher<MarshalledEntry<K, V>>> flowable = Flowable.range(0, stores.length())
            .map(i -> {
               AdvancedLoadWriteStore<K, V> alws = stores.get(i);
               if (alws == null) {
                  return Flowable.empty();
               } else {
                  return alws.publishEntries(filter, fetchValue, fetchMetadata);
               }
            });
      flowable = flowable.filter(f -> (Object) f != Flowable.empty());
      return flowable.parallel()
            .runOn(scheduler)
            .flatMap(f -> f)
            .sequential();
   }

   @Override
   public void clear() {
      for (int i = 0; i < stores.length(); ++i) {
         AdvancedLoadWriteStore<K, V> alws = stores.get(i);
         if (alws != null) {
            alws.clear();
         }
      }
   }

   @Override
   public void purge(Executor threadPool, PurgeListener<? super K> listener) {
      for (int i = 0; i < stores.length(); ++i) {
         AdvancedLoadWriteStore<K, V> alws = stores.get(i);
         if (alws != null) {
            alws.purge(threadPool, listener);
         }
      }
   }

   @Override
   public void clear(IntSet segments) {
      segments.forEach((int i) -> {
         AdvancedLoadWriteStore<K, V> alws = stores.get(i);
         if (alws != null) {
            alws.clear();
         }
      });
   }

   @Override
   public void purge(IntSet segments, Executor threadPool, PurgeListener<? super K> listener) {
      segments.forEach((int i) -> {
         AdvancedLoadWriteStore<K, V> alws = stores.get(i);
         if (alws != null) {
            alws.purge(threadPool, listener);
         }
      });
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

      scheduler = Schedulers.from(executorService);

      HashConfiguration hashConfiguration = cache.getCacheConfiguration().clustering().hash();
      keyPartitioner = componentRegistry.getComponent(KeyPartitioner.class);
      stores = new AtomicReferenceArray<>(hashConfiguration.numSegments());

      // Local (invalidation), replicated and scattered cache we just instantiate all the maps immediately
      // Scattered needs this for backups as they can be for any segment
      // Distributed needs them all only at beginning for preload of data - rehash event will remove others
      for (int i = 0; i < stores.length(); ++i) {
         startNewStoreForSegment(i);
      }

      // Distributed is the only mode that allows for dynamic addition/removal of maps as others own all segments
      // in some fashion
      shouldStopSegments = cache.getCacheConfiguration().clustering().cacheMode().isDistributed();
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

   private void stopStoreForSegment(int segment) {
      AdvancedLoadWriteStore<K, V> store = stores.getAndSet(segment, null);
      if (store != null) {
         store.stop();
      }
   }

   private void destroyStore(int segment) {
      AdvancedLoadWriteStore<K, V> store = stores.getAndSet(segment, null);
      if (store != null) {
         store.destroy();
      }
   }

   @Override
   public void stop() {
      for (int i = 0; i < stores.length(); ++i) {
         stopStoreForSegment(i);
      }
   }

   @Override
   public void addSegments(IntSet segments) {
      segments.forEach((IntConsumer) this::startNewStoreForSegment);
   }

   @Override
   public void removeSegments(IntSet segments) {
      if (shouldStopSegments) {
         segments.forEach((IntConsumer) this::destroyStore);
      } else {
         clear(segments);
      }
   }

   public void forEach(ObjIntConsumer<? super AdvancedLoadWriteStore> consumer) {
      for (int i = 0; i < stores.length(); ++i) {
         AdvancedLoadWriteStore store = stores.get(i);
         if (store != null) {
            consumer.accept(store, i);
         }
      }
   }
}
