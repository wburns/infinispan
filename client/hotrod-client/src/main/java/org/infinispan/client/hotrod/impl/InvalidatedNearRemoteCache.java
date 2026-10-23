package org.infinispan.client.hotrod.impl;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.lang.invoke.MethodHandles;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.configuration.NearCacheConfiguration;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.impl.operations.CacheOperationsFactory;
import org.infinispan.client.hotrod.impl.operations.ClientListenerOperation;
import org.infinispan.client.hotrod.impl.operations.GetWithMetadataOperation;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.topology.CacheInfo;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelRecord;
import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.near.NearCacheService;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.CompletableFutures;

import io.netty.channel.Channel;

/**
 * Near {@link org.infinispan.client.hotrod.RemoteCache} implementation enabling
 *
 * @param <K>
 * @param <V>
 */
public class InvalidatedNearRemoteCache<K, V> extends DelegatingRemoteCache<K, V> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final boolean trace = log.isTraceEnabled();

   private final NearCacheService<K, V> nearcache;
   private final ClientStatistics clientStatistics;
   // This field is used to control updating the near cache with updating the remote server's bloom filter
   // representation. This value will be non-null when bloom filter is enabled, otherwise null. When this
   // value is even it means there is no bloom filter update being sent and the near cache can be updated
   // locally from reads. When a bloom filter is being replicated though the value will be odd and the
   // near cache cannot be updated.
   private final AtomicInteger bloomFilterUpdateVersion;
   private final CacheOperationsFactory cacheOperationsFactory;
   private volatile Channel listenerChannel;

   public static class ServerListenerState {
      private volatile boolean active;
      private final AtomicInteger bloomVersion = new AtomicInteger();
      private final AtomicBoolean registering = new AtomicBoolean();

      public boolean isActive() {
         return active;
      }

      public void setActive(boolean active) {
         this.active = active;
      }

      public boolean startRegistering() {
         return registering.compareAndSet(false, true);
      }

      public void stopRegistering() {
         registering.set(false);
      }

      public int getVersion() {
         return bloomVersion.get();
      }

      public int incrementVersion() {
         return bloomVersion.incrementAndGet();
      }
   }

   private final ConcurrentMap<SocketAddress, ServerListenerState> serverListenerStates = new ConcurrentHashMap<>();
   private volatile Object nearListener;
   private volatile int nearBloomBits;

   InvalidatedNearRemoteCache(InternalRemoteCache<K, V> remoteCache, ClientStatistics clientStatistics,
                              NearCacheService<K, V> nearcache) {
      super(remoteCache);
      this.clientStatistics = clientStatistics;
      this.nearcache = nearcache;
      this.bloomFilterUpdateVersion = nearcache.getBloomFilterBits() > 0 ? new AtomicInteger() : null;
      this.cacheOperationsFactory = remoteCache.getOperationsFactory().newFactoryFor(this);
   }

   public CacheOperationsFactory getOperationsFactory() {
      return cacheOperationsFactory;
   }

   @Override
   <Key, Value> InternalRemoteCache<Key, Value> newDelegatingCache(InternalRemoteCache<Key, Value> innerCache) {
      return new InvalidatedNearRemoteCache<>(innerCache, clientStatistics, (NearCacheService<Key, Value>) nearcache);
   }

   public static <K, V> InvalidatedNearRemoteCache<K, V> delegatingNearCache(RemoteCacheImpl<K, V> remoteCache,
                                                                             NearCacheService<K, V> nearCacheService) {
      return new InvalidatedNearRemoteCache<>(remoteCache, remoteCache.clientStatistics, nearCacheService);
   }

   @Override
   public CompletableFuture<V> getAsync(Object key) {
      CompletableFuture<MetadataValue<V>> value = getWithMetadataAsync((K) key);
      return value.thenApply(v -> v != null ? v.getValue() : null);
   }

   public NearCacheService<K, V> getNearCacheService() {
      return nearcache;
   }

   public boolean isPrimaryMode() {
      if (!getDispatcher().isPrimaryNearCacheListenerSupported()) {
         return false;
      }
      Map<SocketAddress, Set<Integer>> primarySegments = getDispatcher().getPrimarySegmentsByAddress(getName());
      return primarySegments != null && !primarySegments.isEmpty();
   }

   public boolean hasActivePrimaryListeners() {
      return !serverListenerStates.isEmpty() && serverListenerStates.values().stream().anyMatch(ServerListenerState::isActive);
   }

   public SocketAddress getPrimaryOwner(Object key) {
      ConsistentHash ch = getDispatcher().getConsistentHash(getName());
      return ch != null ? ch.getServer(key) : null;
   }

   public static boolean sameAddress(SocketAddress a1, SocketAddress a2) {
      if (Objects.equals(a1, a2)) {
         return true;
      }
      if (a1 instanceof java.net.InetSocketAddress isa1 && a2 instanceof java.net.InetSocketAddress isa2) {
         return isa1.getPort() == isa2.getPort() && Objects.equals(isa1.getHostString(), isa2.getHostString());
      }
      return false;
   }

   public int getVersionIfActive(SocketAddress server) {
      if (server == null) {
         return -1;
      }
      for (Map.Entry<SocketAddress, ServerListenerState> entry : serverListenerStates.entrySet()) {
         if (sameAddress(entry.getKey(), server)) {
            ServerListenerState state = entry.getValue();
            if (!state.isActive()) {
               return -1;
            }
            int v = state.getVersion();
            return (v & 1) == 1 ? -1 : v;
         }
      }
      return -1;
   }

   public boolean isActiveAndVersionMatches(SocketAddress server, int expectedVersion) {
      if (server == null || expectedVersion < 0) {
         return false;
      }
      for (Map.Entry<SocketAddress, ServerListenerState> entry : serverListenerStates.entrySet()) {
         if (sameAddress(entry.getKey(), server)) {
            ServerListenerState state = entry.getValue();
            return state.isActive() && state.getVersion() == expectedVersion;
         }
      }
      return false;
   }

   public ServerListenerState getOrCreateServerListenerState(SocketAddress server) {
      for (Map.Entry<SocketAddress, ServerListenerState> entry : serverListenerStates.entrySet()) {
         if (sameAddress(entry.getKey(), server)) {
            return entry.getValue();
         }
      }
      return serverListenerStates.computeIfAbsent(server, k -> new ServerListenerState());
   }

   public void setServerListenerActive(SocketAddress server, boolean active) {
      ServerListenerState state = getOrCreateServerListenerState(server);
      state.setActive(active);
   }

   public ServerListenerState getServerListenerState(SocketAddress server) {
      for (Map.Entry<SocketAddress, ServerListenerState> entry : serverListenerStates.entrySet()) {
         if (sameAddress(entry.getKey(), server)) {
            return entry.getValue();
         }
      }
      return null;
   }

   public Map<SocketAddress, ServerListenerState> getServerListenerStates() {
      return Collections.unmodifiableMap(serverListenerStates);
   }

   public void handlePrimaryListenerDisconnected(SocketAddress address) {
      ServerListenerState state = getServerListenerState(address);
      if (state != null) {
         state.setActive(false);
         state.stopRegistering();
      }
      Map<SocketAddress, Set<Integer>> primarySegments = getDispatcher().getPrimarySegmentsByAddress(getName());
      if (primarySegments != null) {
         for (Map.Entry<SocketAddress, Set<Integer>> entry : primarySegments.entrySet()) {
            if (sameAddress(entry.getKey(), address)) {
               nearcache.invalidateSegments(entry.getValue());
            }
         }
      }
   }

   public int getCurrentVersion() {
      if (bloomFilterUpdateVersion != null) {
         return bloomFilterUpdateVersion.get();
      }
      return 0;
   }

   @Override
   public CompletableFuture<MetadataValue<V>> getWithMetadataAsync(K key) {
      MetadataValue<V> nearValue = nearcache.get(key);
      // We rely upon the fact that a MetadataValue will never be null from a remote lookup but our placeholder will be null
      if (nearValue == null || nearValue.getValue() == null) {
         clientStatistics.incrementNearCacheMisses();
         // Note that MetadataValueImpl does not implement equals method so we rely upon object identity in the replace below
         // We cannot cache this value as we could have 2 concurrent gets and an update and we could cache a previous one
         MetadataValue<V> calculatingPlaceholder = new MetadataValueImpl<>(-1, -1, -1, -1, -1, null);
         boolean cache = nearcache.putIfAbsent(key, calculatingPlaceholder);

         boolean isPrimary = isPrimaryMode();
         final SocketAddress primaryServer;
         final int prevVersion;
         final CompletionStage<GetWithMetadataOperation.GetWithMetadataResult<V>> remoteValue;

         if (isPrimary) {
            primaryServer = getPrimaryOwner(key);
            prevVersion = getVersionIfActive(primaryServer);
            if (!cache || prevVersion < 0) {
               if (trace) {
                  log.tracef("Unable to cache returned value for key %s (cache=%s, prevVersion=%d, primary=%s)",
                        Util.toStr(key), cache, prevVersion, primaryServer);
               }
               nearcache.remove(key, calculatingPlaceholder);
               return super.getWithMetadataAsync(key, null).toCompletableFuture()
                     .thenApply(GetWithMetadataOperation.GetWithMetadataResult::value);
            }
            remoteValue = super.getWithMetadataAsync(key, null);
         } else {
            primaryServer = null;
            prevVersion = getCurrentVersion();
            remoteValue = super.getWithMetadataAsync(key, listenerChannel);
            if (!cache || (prevVersion & 1) == 1) {
               if (trace) {
                  log.tracef("Unable to cache returned value for key %s as either has concurrent operation or during a bloom filter update",
                        Util.toStr(key));
               }
               nearcache.remove(key, calculatingPlaceholder);
               return remoteValue.toCompletableFuture()
                     .thenApply(GetWithMetadataOperation.GetWithMetadataResult::value);
            }
         }

         return remoteValue.thenApply(v -> {
            boolean shouldRemove = true;
            MetadataValue<V> value = v != null ? v.value() : null;
            // We cannot cache the value if a retry was required - which means we did not talk to the listener node
            if (value != null) {
               if (isPrimary) {
                  SocketAddress currentPrimary = getPrimaryOwner(key);
                  if (!Objects.equals(primaryServer, currentPrimary)) {
                     if (trace) {
                        log.tracef("Unable to cache returned value for key %s as primary owner changed from %s to %s",
                              Util.toStr(key), primaryServer, currentPrimary);
                     }
                  } else if (!isActiveAndVersionMatches(primaryServer, prevVersion)) {
                     if (trace) {
                        log.tracef("Unable to cache returned value for key %s as listener state/version on %s changed",
                              Util.toStr(key), primaryServer);
                     }
                  } else {
                     nearcache.replace(key, v.serializedKey(), calculatingPlaceholder, value);
                     if (value.getMaxIdle() > 0) {
                        HOTROD.nearCacheMaxIdleUnsupported();
                     }
                     shouldRemove = false;
                  }
               } else {
                  // We can't cache if the new version doesn't match the prior as it overlapped a bloom update.
                  if (prevVersion != getCurrentVersion()) {
                     if (trace) {
                        log.tracef("Unable to cache returned value for key %s as operation was performed during a" +
                              " bloom filter update", Util.toStr(key));
                     }
                  } else if (listenerChannel != null && v.serializedKey() == null) {
                     // Having a listener address means it has a bloom filter. When we have a bloom filter we cannot
                     // cache values upon a retry as we can't guarantee the bloom filter is updated on the server properly
                     if (trace) {
                        log.tracef("Unable to cache returned value for key %s as operation was retried",
                              Util.toStr(key));
                     }
                  } else {
                     nearcache.replace(key, v.serializedKey(), calculatingPlaceholder, value);
                     if (value.getMaxIdle() > 0) {
                        HOTROD.nearCacheMaxIdleUnsupported();
                     }
                     shouldRemove = false;
                  }
               }
            }
            if (shouldRemove) {
               nearcache.remove(key, calculatingPlaceholder);
            }
            return value;
         }).toCompletableFuture();
      } else {
         clientStatistics.incrementNearCacheHits();
         return CompletableFuture.completedFuture(nearValue);
      }
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      if (maxIdleTime > 0)
         HOTROD.nearCacheMaxIdleUnsupported();
      CompletableFuture<V> ret = super.putAsync(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return ret.thenApply(v -> {
         nearcache.remove(key);
         return v;
      });
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      if (maxIdleTime > 0)
         HOTROD.nearCacheMaxIdleUnsupported();
      return super.putAllAsync(map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit)
            .thenRun(() -> map.keySet().forEach(nearcache::remove));
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      if (maxIdleTime > 0)
         HOTROD.nearCacheMaxIdleUnsupported();
      return invalidateNearCacheIfNeeded(delegate.hasForceReturnFlag(), key,
            super.replaceAsync(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit)
      );
   }

   @Override
   public CompletableFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version, long lifespan, TimeUnit lifespanTimeUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      if (maxIdleTime > 0)
         HOTROD.nearCacheMaxIdleUnsupported();
      return super.replaceWithVersionAsync(key, newValue, version, lifespan, lifespanTimeUnit, maxIdleTime, maxIdleTimeUnit)
            .thenApply(removed -> {
               if (removed) nearcache.remove(key);
               return removed;
            });
   }

   @Override
   public CompletableFuture<V> removeAsync(Object key) {
      return invalidateNearCacheIfNeeded(delegate.hasForceReturnFlag(), key, super.removeAsync(key));
   }

   @Override
   public CompletableFuture<Boolean> removeWithVersionAsync(K key, long version) {
      return super.removeWithVersionAsync(key, version)
            .thenApply(removed -> {
               if (removed) nearcache.remove(key); // Eager invalidation to avoid race
               return removed;
            });
   }

   @Override
   public CompletableFuture<Void> clearAsync() {
      return super.clearAsync().thenRun(nearcache::clear);
   }

   @SuppressWarnings("unchecked")
   CompletableFuture<V> invalidateNearCacheIfNeeded(boolean hasForceReturnValue, Object key, CompletableFuture<V> prev) {
      return prev.thenApply(v -> {
         if (!hasForceReturnValue || v != null)
            nearcache.remove((K) key);
         return v;
      });
   }

   @Override
   public void resolveStorage(MediaType key, MediaType value) {
      if (key != null && !delegate.getMarshaller().mediaType().match(key)) {
         // The server has a storage type which the cache marshaller does not handle.
         // This could lead to losing events in the bloom filter, where the client and server see the key differently.
         // To avoid this issue, the client negotiate the type when installing the listener, but it needs a proper configuration.
         Class<? extends Marshaller> marshallerClass = delegate.getMarshaller().getClass();
         log.invalidateNearDefaultMarshallerMismatch(delegate.getName(), marshallerClass, key);
         listenerChannel = nearcache.start(this);
         delegate.resolveStorage(key, value);
      } else {
         delegate.resolveStorage(key, value);
         listenerChannel = nearcache.start(this);
      }
   }

   @Override
   public void stop() {
      nearcache.stop(this);
      super.stop();
   }

   public void clearNearCache() {
      nearcache.clear();
   }

   // Increments the bloom filter version if it is even and returns whether it was incremented
   private boolean incrementBloomVersionIfEven() {
      int prev;
      do {
         prev = bloomFilterUpdateVersion.get();
         // Odd number means we are already sending bloom filter
         if ((prev & 1) == 1) {
            return false;
         }
      } while (!bloomFilterUpdateVersion.compareAndSet(prev, prev + 1));
      return true;
   }

   CompletionStage<Void> incrementBloomVersionUponCompletion(CompletionStage<Void> stage) {
      if (bloomFilterUpdateVersion != null) {
         return stage
               .whenComplete((ignore, t) -> bloomFilterUpdateVersion.incrementAndGet());
      }
      return stage;
   }

   @Override
   public CompletionStage<Void> updateBloomFilter() {
      if (bloomFilterUpdateVersion == null && nearcache.getBloomFilterBits() <= 0) {
         return CompletableFutures.completedNull();
      }
      if (isPrimaryMode()) {
         byte[] bloomFilterBits = nearcache.calculateBloomBits();
         List<CompletableFuture<Void>> stages = new ArrayList<>();
         CacheOperationsFactory operationsFactory = getOperationsFactory();
         for (Map.Entry<SocketAddress, ServerListenerState> entry : serverListenerStates.entrySet()) {
            if (entry.getValue().isActive()) {
               ServerListenerState state = entry.getValue();
               state.incrementVersion();
               HotRodOperation<Void> op = operationsFactory.newUpdateBloomFilterOperation(bloomFilterBits);
               CompletionStage<Void> stage = getDispatcher().executeOnSingleAddress(op, entry.getKey())
                     .whenComplete((ignore, t) -> state.incrementVersion());
               stages.add(stage.toCompletableFuture());
            }
         }
         return CompletableFuture.allOf(stages.toArray(new CompletableFuture[0]));
      }
      // Not being able to increment the even version means we have a concurrent update - so skip
      if (!incrementBloomVersionIfEven()) {
         if (trace) {
            log.tracef("Already have a concurrent bloom filter update for listenerId(%s) - skipping",
                  Util.printArray(nearcache.getListenerId()));
         }
         return CompletableFuture.completedFuture(null);
      }
      byte[] bloomFilterBits = nearcache.calculateBloomBits();

      if (trace) {
         log.tracef("Sending bloom filter bits(%s) update to %s for listenerId(%s)",
               Util.printArray(bloomFilterBits), listenerChannel,
               Util.printArray(nearcache.getListenerId()));
      }
      CacheOperationsFactory operationsFactory = getOperationsFactory();
      HotRodOperation<Void> op = operationsFactory.newUpdateBloomFilterOperation(bloomFilterBits);
      return incrementBloomVersionUponCompletion(getDispatcher().executeOnSingleAddress(op, ChannelRecord.of(listenerChannel)));
   }

   public SocketAddress getBloomListenerAddress() {
      return ChannelRecord.of(listenerChannel);
   }

   public void setBloomListenerAddress(Channel channel) {
      this.listenerChannel = channel;
   }

   public NearCacheConfiguration getNearCacheConfiguration() {
      return nearcache.getConfig();
   }

   @Override
   public Channel addNearCacheListener(Object listener, int bloomBits) {
      this.nearListener = listener;
      this.nearBloomBits = bloomBits;
      if (isPrimaryMode()) {
         getDispatcher().addTopologyListener(getName(), this::onTopologyUpdate);
         return registerPrimaryListeners(listener, bloomBits);
      }
      ClientListenerOperation op = getOperationsFactory().newAddNearCacheListenerOperation(listener,
            bloomBits);
      return getDispatcher().await(getDispatcher().executeAddListener(op));
   }

   private Channel registerPrimaryListeners(Object listener, int bloomBits) {
      OperationDispatcher dispatcher = getDispatcher();
      ConsistentHash ch = dispatcher.getConsistentHash(getName());
      if (ch instanceof SegmentConsistentHash sch) {
         nearcache.setSegmentFunction(sch::getSegment, sch.getNumSegments());
      }
      Map<SocketAddress, Set<Integer>> primarySegments = dispatcher.getPrimarySegmentsByAddress(getName());
      if (primarySegments == null || primarySegments.isEmpty()) {
         return null;
      }
      List<CompletableFuture<Channel>> stages = new ArrayList<>();
      for (SocketAddress server : primarySegments.keySet()) {
         stages.add(registerPrimaryListenerOnServer(server, listener, bloomBits).toCompletableFuture());
      }
      return dispatcher.await(CompletableFutures.sequence(stages).thenApply(list -> {
         for (Channel c : list) {
            if (c != null) {
               return c;
            }
         }
         return null;
      }));
   }

   private CompletionStage<Channel> registerPrimaryListenerOnServer(SocketAddress server, Object listener, int bloomBits) {
      ServerListenerState state = getOrCreateServerListenerState(server);
      if (state.isActive() || !state.startRegistering()) {
         return CompletableFuture.completedFuture(null);
      }
      ClientListenerOperation op = getOperationsFactory().newAddPrimaryNearCacheListenerOperation(listener, bloomBits);
      return getDispatcher().executeAddListener(op, server).thenCompose(ch -> {
         if (bloomBits > 0) {
            byte[] currentBloomBits = nearcache.calculateBloomBits();
            if (currentBloomBits != null) {
               HotRodOperation<Void> updateOp = getOperationsFactory().newUpdateBloomFilterOperation(currentBloomBits);
               return getDispatcher().executeOnSingleAddress(updateOp, ChannelRecord.of(ch))
                     .thenApply(ignore -> {
                        state.setActive(true);
                        state.stopRegistering();
                        return ch;
                     });
            }
         }
         state.setActive(true);
         state.stopRegistering();
         return CompletableFuture.completedFuture(ch);
      }).whenComplete((ch, t) -> {
         if (t != null) {
            state.setActive(false);
            state.stopRegistering();
            if (trace) {
               log.tracef(t, "Failed to register primary listener on server %s", server);
            }
         }
      });
   }

   private void onTopologyUpdate(CacheInfo oldCacheInfo, CacheInfo newCacheInfo) {
      ConsistentHash oldCH = oldCacheInfo != null ? oldCacheInfo.getConsistentHash() : null;
      ConsistentHash newCH = newCacheInfo != null ? newCacheInfo.getConsistentHash() : null;

      if (newCH instanceof SegmentConsistentHash newSCH) {
         int numSegments = newSCH.getNumSegments();
         nearcache.setSegmentFunction(newSCH::getSegment, numSegments);

         if (oldCH instanceof SegmentConsistentHash oldSCH) {
            SocketAddress[][] oldOwners = oldSCH.getSegmentOwners();
            SocketAddress[][] newOwners = newSCH.getSegmentOwners();
            for (int seg = 0; seg < numSegments; ++seg) {
               SocketAddress oldPrimary = (oldOwners != null && seg < oldOwners.length && oldOwners[seg].length > 0) ? oldOwners[seg][0] : null;
               SocketAddress newPrimary = (newOwners != null && seg < newOwners.length && newOwners[seg].length > 0) ? newOwners[seg][0] : null;
               if (!Objects.equals(oldPrimary, newPrimary)) {
                  nearcache.invalidateSegment(seg);
               }
            }
         } else {
            for (int seg = 0; seg < numSegments; ++seg) {
               nearcache.invalidateSegment(seg);
            }
         }

         // Register listeners on any new primary servers
         Object listener = this.nearListener;
         if (listener != null) {
            Map<SocketAddress, Set<Integer>> primarySegments = newCacheInfo.getPrimarySegments();
            if (primarySegments != null) {
               for (SocketAddress server : primarySegments.keySet()) {
                  registerPrimaryListenerOnServer(server, listener, nearBloomBits);
               }
            }
         }
      }
   }

   @Override
   public Marshaller getMarshaller() {
      return delegate.getMarshaller();
   }
}
