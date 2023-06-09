package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.ServerStatistics;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.VersionedOperationResponse;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.iteration.KeyTracker;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.query.RemoteQuery;
import org.infinispan.client.hotrod.impl.transaction.entry.Modification;
import org.infinispan.client.hotrod.impl.transaction.operations.PrepareTransactionOperation;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.telemetry.impl.TelemetryService;
import org.infinispan.commons.util.IntSet;

import io.netty.channel.Channel;
import net.jcip.annotations.Immutable;

/**
 * Factory for {@link org.infinispan.client.hotrod.impl.operations.HotRodOperation} objects.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class OperationsFactory implements HotRodConstants {

   private static final Log log = LogFactory.getLog(OperationsFactory.class, Log.class);

   private final ThreadLocal<Integer> flagsMap = new ThreadLocal<>();

   private final ChannelFactory channelFactory;

   private final byte[] cacheNameBytes;

   private final AtomicReference<ClientTopology> clientTopologyRef;

   private final boolean forceReturnValue;

   private final ClientListenerNotifier listenerNotifier;

   private final String cacheName;

   private final Configuration cfg;

   private final ClientStatistics clientStatistics;

   private final TelemetryService telemetryService;

   public OperationsFactory(ChannelFactory channelFactory, String cacheName, boolean forceReturnValue, ClientListenerNotifier listenerNotifier, Configuration cfg, ClientStatistics clientStatistics) {
      this.channelFactory = channelFactory;
      this.cacheNameBytes = cacheName == null ? DEFAULT_CACHE_NAME_BYTES : RemoteCacheManager.cacheNameBytes(cacheName);
      this.cacheName = cacheName;
      clientTopologyRef = channelFactory != null
            ? channelFactory.createTopologyId(cacheNameBytes)
            : new AtomicReference<>(new ClientTopology(-1, cfg.clientIntelligence()));
      this.forceReturnValue = forceReturnValue;

      this.listenerNotifier = listenerNotifier;
      this.cfg = cfg;
      this.clientStatistics = clientStatistics;

      TelemetryService telemetryService = null;
      try {
         if (cfg.tracingPropagationEnabled()) {
            telemetryService = TelemetryService.create();
            log.openTelemetryPropagationEnabled();
         } else {
            log.openTelemetryPropagationDisabled();
         }
      } catch (Throwable e) {
         // missing dependency => no context to propagate to the server
         log.noOpenTelemetryAPI(e);
      }

      this.telemetryService = telemetryService;
   }

   public OperationsFactory(ChannelFactory channelFactory, ClientListenerNotifier listenerNotifier, Configuration cfg) {
      this(channelFactory, null, false, listenerNotifier, cfg, null);
   }

   public ClientListenerNotifier getListenerNotifier() {
      return listenerNotifier;
   }

   public String getCacheName() {
      return cacheName;
   }

   public ChannelFactory getChannelFactory() {
      return channelFactory;
   }

   public Codec getCodec() {
      return channelFactory.getNegotiatedCodec();
   }

   public <V> CompletableFuture<V> newGetKeyOperation(Object key, byte[] keyBytes, DataFormat dataFormat) {
      return new GetOperation<V>(getCodec(), channelFactory, key, keyBytes, cacheNameBytes, clientTopologyRef, flags(),
            cfg, dataFormat, clientStatistics)
            .execute();
   }

   public <K, V> CompletableFuture<Map<K, V>> newGetAllOperation(Set<byte[]> keys, DataFormat dataFormat) {
      return new GetAllParallelOperation<K, V>(getCodec(), channelFactory, keys, cacheNameBytes, clientTopologyRef, flags(),
            cfg, dataFormat, clientStatistics).execute();
   }

   public <V> CompletableFuture<V> newRemoveOperation(Object key, byte[] keyBytes, DataFormat dataFormat) {
      return new RemoveOperation<V>(
            getCodec(), channelFactory, key, keyBytes, cacheNameBytes, clientTopologyRef, flags(), cfg, dataFormat,
            clientStatistics, telemetryService).execute();
   }

   public <V> CompletableFuture<VersionedOperationResponse<V>> newRemoveIfUnmodifiedOperation(Object key, byte[] keyBytes, long version, DataFormat dataFormat) {
      return new RemoveIfUnmodifiedOperation<V>(
            getCodec(), channelFactory, key, keyBytes, cacheNameBytes, clientTopologyRef, flags(), cfg, version, dataFormat,
            clientStatistics, telemetryService).execute();
   }

   public <V> CompletableFuture<VersionedOperationResponse<V>> newReplaceIfUnmodifiedOperation(Object key, byte[] keyBytes,
                                                                                        byte[] value, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit, long version, DataFormat dataFormat) {
      return new ReplaceIfUnmodifiedOperation<V>(
            getCodec(), channelFactory, key, keyBytes, cacheNameBytes, clientTopologyRef, flags(lifespan, maxIdle),
            cfg, value, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, version, dataFormat, clientStatistics,
            telemetryService).execute();
   }

   public <V> CompletableFuture<MetadataValue<V>> newGetWithMetadataOperation(Object key, byte[] keyBytes, DataFormat dataFormat) {
      return this.<V>newGetWithMetadataOperation(key, keyBytes, dataFormat, null).toCompletableFuture();
   }

   public <V> RetryAwareCompletionStage<MetadataValue<V>> newGetWithMetadataOperation(Object key, byte[] keyBytes, DataFormat dataFormat,
                                                                      SocketAddress listenerServer) {
      var op = new GetWithMetadataOperation<V>(
            getCodec(), channelFactory, key, keyBytes, cacheNameBytes, clientTopologyRef, flags(), cfg, dataFormat, clientStatistics,
            listenerServer);
      op.execute();
      return op;
   }

   public CompletionStage<ServerStatistics> newStatsOperation() {
      return new StatsOperation(
            getCodec(), channelFactory, cacheNameBytes, clientTopologyRef, flags(), cfg).execute();
   }

   public <V> CompletableFuture<V> newPutKeyValueOperation(Object key, byte[] keyBytes, byte[] value,
                                                           long lifespan, TimeUnit lifespanTimeUnit, long maxIdle,
                                                           TimeUnit maxIdleTimeUnit, DataFormat dataFormat) {
      return new PutOperation<V>(
            getCodec(), channelFactory, key, keyBytes, cacheNameBytes, clientTopologyRef, flags(lifespan, maxIdle),
            cfg, value, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, dataFormat, clientStatistics,
            telemetryService).execute();
   }

   public CompletableFuture<Void> newPutAllOperation(Map<byte[], byte[]> map,
                                                     long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit, DataFormat dataFormat) {
      return new PutAllParallelOperation(
            getCodec(), channelFactory, map, cacheNameBytes, clientTopologyRef, flags(lifespan, maxIdle), cfg,
            lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, dataFormat, clientStatistics, telemetryService)
            .execute();
   }

   public <V> CompletableFuture<V> newPutIfAbsentOperation(Object key, byte[] keyBytes, byte[] value,
                                                              long lifespan, TimeUnit lifespanUnit, long maxIdleTime,
                                                              TimeUnit maxIdleTimeUnit, DataFormat dataFormat) {
      return new PutIfAbsentOperation<V>(
            getCodec(), channelFactory, key, keyBytes, cacheNameBytes, clientTopologyRef, flags(lifespan, maxIdleTime),
            cfg, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, dataFormat, clientStatistics,
            telemetryService).execute();
   }

   public <V> CompletableFuture<V> newReplaceOperation(Object key, byte[] keyBytes, byte[] values,
                                                      long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit, DataFormat dataFormat) {
      return new ReplaceOperation<V>(
            getCodec(), channelFactory, key, keyBytes, cacheNameBytes, clientTopologyRef, flags(lifespan, maxIdle),
            cfg, values, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, dataFormat, clientStatistics,
            telemetryService).execute();
   }

   public CompletableFuture<Boolean> newContainsKeyOperation(Object key, byte[] keyBytes, DataFormat dataFormat) {
      return new ContainsKeyOperation(getCodec(), channelFactory, key, keyBytes, cacheNameBytes, clientTopologyRef,
            flags(), cfg, dataFormat, clientStatistics).execute();
   }

   public CompletableFuture<Void> newClearOperation() {
      return new ClearOperation(
            getCodec(), channelFactory, cacheNameBytes, clientTopologyRef, flags(), cfg, telemetryService).execute();
   }

   public <K> BulkGetKeysOperation<K> newBulkGetKeysOperation(int scope, DataFormat dataFormat) {
      return new BulkGetKeysOperation<>(
            getCodec(), channelFactory, cacheNameBytes, clientTopologyRef, flags(), cfg, scope, dataFormat, clientStatistics);
   }

   public CompletionStage<SocketAddress> newAddClientListenerOperation(Object listener, DataFormat dataFormat) {
      return new AddClientListenerOperation(getCodec(), channelFactory,
            cacheName, clientTopologyRef, flags(), cfg, listenerNotifier,
            listener, null, null, dataFormat, null, telemetryService)
            .execute();
   }

   public CompletionStage<SocketAddress> newAddClientListenerOperation(
         Object listener, byte[][] filterFactoryParams, byte[][] converterFactoryParams, DataFormat dataFormat) {
      return new AddClientListenerOperation(getCodec(), channelFactory,
            cacheName, clientTopologyRef, flags(), cfg, listenerNotifier,
            listener, filterFactoryParams, converterFactoryParams, dataFormat, null, telemetryService).execute();
   }

   public CompletionStage<Void> newRemoveClientListenerOperation(Object listener) {
      return new RemoveClientListenerOperation(getCodec(), channelFactory,
            cacheNameBytes, clientTopologyRef, flags(), cfg, listenerNotifier, listener).execute();
   }

   public CompletionStage<SocketAddress> newAddNearCacheListenerOperation(Object listener, DataFormat dataFormat,
                                                                          int bloomFilterBits, InternalRemoteCache<?, ?> remoteCache) {
      return new AddBloomNearCacheClientListenerOperation(getCodec(), channelFactory, cacheName, clientTopologyRef, flags(), cfg, listenerNotifier,
            listener, dataFormat, bloomFilterBits, remoteCache).execute();
   }

   public CompletionStage<Void> newUpdateBloomFilterOperation(SocketAddress address, byte[] bloomBytes) {
      return new UpdateBloomFilterOperation(getCodec(), channelFactory, cacheNameBytes, clientTopologyRef, flags(), cfg, address,
            bloomBytes).execute();
   }

   /**
    * Construct a ping request directed to a particular node.
    *
    * @return a ping operation for a particular node
    * @param releaseChannel
    */
   public PingOperation newPingOperation(boolean releaseChannel) {
      return new PingOperation(getCodec(), clientTopologyRef, cfg, cacheNameBytes, channelFactory, releaseChannel);
   }

   /**
    * Construct a fault tolerant ping request. This operation should be capable
    * to deal with nodes being down, so it will find the first node successful
    * node to respond to the ping.
    *
    * @return a ping operation for the cluster
    */
   public CompletionStage<PingResponse> newFaultTolerantPingOperation() {
      return new FaultTolerantPingOperation(
            getCodec(), channelFactory, cacheNameBytes, clientTopologyRef, flags(), cfg).execute();
   }

   public QueryOperation newQueryOperation(RemoteQuery<?> remoteQuery, DataFormat dataFormat) {
      return new QueryOperation(
            getCodec(), channelFactory, cacheNameBytes, clientTopologyRef, flags(), cfg, remoteQuery, dataFormat);
   }

   public CompletableFuture<Integer> newSizeOperation() {
      return new SizeOperation(getCodec(), channelFactory, cacheNameBytes, clientTopologyRef, flags(), cfg, telemetryService)
            .execute();
   }

   public <T> CompletionStage<T> newExecuteOperation(String taskName, Map<String, byte[]> marshalledParams, Object key, DataFormat dataFormat) {
      return new ExecuteOperation<T>(getCodec(), channelFactory, cacheNameBytes,
            clientTopologyRef, flags(), cfg, taskName, marshalledParams, key, dataFormat).execute();
   }

   public AdminOperation newAdminOperation(String taskName, Map<String, byte[]> marshalledParams) {
      return new AdminOperation(getCodec(), channelFactory, cacheNameBytes,
            clientTopologyRef, flags(), cfg, taskName, marshalledParams);
   }

   private int flags(long lifespan, long maxIdle) {
      int intFlags = flags();
      if (lifespan == 0) {
         intFlags |= Flag.DEFAULT_LIFESPAN.getFlagInt();
      }
      if (maxIdle == 0) {
         intFlags |= Flag.DEFAULT_MAXIDLE.getFlagInt();
      }
      return intFlags;
   }

   public int flags() {
      Integer threadLocalFlags = this.flagsMap.get();
      this.flagsMap.remove();
      int intFlags = 0;
      if (threadLocalFlags != null) {
         intFlags |= threadLocalFlags;
      }
      if (forceReturnValue) {
         intFlags |= Flag.FORCE_RETURN_VALUE.getFlagInt();
      }
      return intFlags;
   }

   public void setFlags(Flag[] flags) {
      int intFlags = 0;
      for (Flag flag : flags)
         intFlags |= flag.getFlagInt();
      this.flagsMap.set(intFlags);
   }

   public void setFlags(int intFlags) {
      this.flagsMap.set(intFlags);
   }

   public boolean hasFlag(Flag flag) {
      Integer threadLocalFlags = this.flagsMap.get();
      return threadLocalFlags != null && (threadLocalFlags & flag.getFlagInt()) != 0;
   }

   public CacheTopologyInfo getCacheTopologyInfo() {
      return channelFactory.getCacheTopologyInfo(cacheNameBytes);
   }

   /**
    * Returns a map containing for each address all of its primarily owned segments. If the primary segments are not
    * known an empty map will be returned instead
    * @return map containing addresses and their primary segments
    */
   public Map<SocketAddress, Set<Integer>> getPrimarySegmentsByAddress() {
      return channelFactory.getPrimarySegmentsByAddress(cacheNameBytes);
   }

   public ConsistentHash getConsistentHash() {
      return channelFactory.getConsistentHash(cacheNameBytes);
   }

   public int getTopologyId() {
      return clientTopologyRef.get().getTopologyId();
   }

   public IterationStartOperation newIterationStartOperation(String filterConverterFactory, byte[][] filterParameters, IntSet segments, int batchSize, boolean metadata, DataFormat dataFormat, SocketAddress targetAddress) {
      return new IterationStartOperation(getCodec(), flags(), cfg, cacheNameBytes, clientTopologyRef, filterConverterFactory, filterParameters, segments, batchSize, channelFactory, metadata, dataFormat, targetAddress);
   }

   public IterationEndOperation newIterationEndOperation(byte[] iterationId, Channel channel) {
      return new IterationEndOperation(getCodec(), flags(), cfg, cacheNameBytes, clientTopologyRef, iterationId, channelFactory, channel);
   }

   public <K, E> IterationNextOperation<K, E> newIterationNextOperation(byte[] iterationId, Channel channel, KeyTracker segmentKeyTracker, DataFormat dataFormat) {
      return new IterationNextOperation<>(getCodec(), flags(), cfg, cacheNameBytes, clientTopologyRef, iterationId, channel, channelFactory, segmentKeyTracker, dataFormat);
   }

   public <K> GetStreamOperation newGetStreamOperation(K key, byte[] keyBytes, int offset) {
      return new GetStreamOperation(getCodec(), channelFactory, key, keyBytes, offset, cacheNameBytes, clientTopologyRef, flags(), cfg, clientStatistics);
   }

   public <K> PutStreamOperation newPutStreamOperation(K key, byte[] keyBytes, long version, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return new PutStreamOperation(getCodec(), channelFactory, key, keyBytes, cacheNameBytes, clientTopologyRef, flags(), cfg, version, lifespan, lifespanUnit, maxIdle, maxIdleUnit, clientStatistics, telemetryService);
   }

   public <K> PutStreamOperation newPutStreamOperation(K key, byte[] keyBytes, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return new PutStreamOperation(getCodec(), channelFactory, key, keyBytes, cacheNameBytes, clientTopologyRef, flags(), cfg, PutStreamOperation.VERSION_PUT, lifespan, lifespanUnit, maxIdle, maxIdleUnit, clientStatistics, telemetryService);
   }

   public <K> PutStreamOperation newPutIfAbsentStreamOperation(K key, byte[] keyBytes, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return new PutStreamOperation(getCodec(), channelFactory, key, keyBytes, cacheNameBytes, clientTopologyRef, flags(), cfg, PutStreamOperation.VERSION_PUT_IF_ABSENT, lifespan, lifespanUnit, maxIdle, maxIdleUnit, clientStatistics, telemetryService);
   }

   public AuthMechListOperation newAuthMechListOperation(Channel channel) {
      return new AuthMechListOperation(getCodec(), clientTopologyRef, cfg, channel, channelFactory);
   }

   public AuthOperation newAuthOperation(Channel channel, String saslMechanism, byte[] response) {
      return new AuthOperation(getCodec(), clientTopologyRef, cfg, channel, channelFactory, saslMechanism, response);
   }

   public PrepareTransactionOperation newPrepareTransactionOperation(Xid xid, boolean onePhaseCommit,
                                                                     List<Modification> modifications,
                                                                     boolean recoverable, long timeoutMs) {
      return new PrepareTransactionOperation(getCodec(), channelFactory, cacheNameBytes, clientTopologyRef, cfg, xid,
            onePhaseCommit, modifications, recoverable, timeoutMs);
   }
}
