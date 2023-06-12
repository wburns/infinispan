package org.infinispan.client.hotrod.impl.operations;

import java.io.OutputStream;
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
import org.infinispan.client.hotrod.impl.protocol.ChannelInputStream;
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
import org.infinispan.query.remote.client.impl.BaseQueryResponse;

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
      Codec codec = getCodec();
      return codec.executeCommand(new GetOperation<V>(codec, channelFactory, key, keyBytes, cacheNameBytes,
            clientTopologyRef, flags(), cfg, dataFormat, clientStatistics), channelFactory).toCompletableFuture();
   }

   public <K, V> CompletableFuture<Map<K, V>> newGetAllOperation(Set<byte[]> keys, DataFormat dataFormat) {
      Codec codec = getCodec();
      return codec.executeCommand(new GetAllParallelOperation<K, V>(codec, channelFactory, keys, cacheNameBytes,
            clientTopologyRef, flags(), cfg, dataFormat, clientStatistics), channelFactory).toCompletableFuture();
   }

   public <V> CompletableFuture<V> newRemoveOperation(Object key, byte[] keyBytes, DataFormat dataFormat) {
      Codec codec = getCodec();
      return codec.executeCommand(new RemoveOperation<V>(codec, channelFactory, key, keyBytes, cacheNameBytes,
            clientTopologyRef, flags(), cfg, dataFormat, clientStatistics, telemetryService), channelFactory)
            .toCompletableFuture();
   }

   public <V> CompletableFuture<VersionedOperationResponse<V>> newRemoveIfUnmodifiedOperation(Object key, byte[] keyBytes, long version, DataFormat dataFormat) {
      Codec codec = getCodec();
      return codec.executeCommand(new RemoveIfUnmodifiedOperation<V>(codec, channelFactory, key, keyBytes,
            cacheNameBytes, clientTopologyRef, flags(), cfg, version, dataFormat, clientStatistics, telemetryService),
            channelFactory).toCompletableFuture();
   }

   public <V> CompletableFuture<VersionedOperationResponse<V>> newReplaceIfUnmodifiedOperation(Object key, byte[] keyBytes,
                                                                                        byte[] value, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit, long version, DataFormat dataFormat) {
      Codec codec = getCodec();
      return codec.executeCommand(new ReplaceIfUnmodifiedOperation<V>(codec, channelFactory, key, keyBytes,
            cacheNameBytes, clientTopologyRef, flags(lifespan, maxIdle), cfg, value, lifespan, lifespanTimeUnit,
            maxIdle, maxIdleTimeUnit, version, dataFormat, clientStatistics, telemetryService), channelFactory)
            .toCompletableFuture();
   }

   public <V> CompletableFuture<MetadataValue<V>> newGetWithMetadataOperation(Object key, byte[] keyBytes, DataFormat dataFormat) {
      return this.<V>newGetWithMetadataOperation(key, keyBytes, dataFormat, null).toCompletableFuture();
   }

   public <V> RetryAwareCompletionStage<MetadataValue<V>> newGetWithMetadataOperation(Object key, byte[] keyBytes, DataFormat dataFormat,
                                                                      SocketAddress listenerServer) {
      Codec codec = getCodec();
      var op = new GetWithMetadataOperation<V>(codec, channelFactory, key, keyBytes, cacheNameBytes, clientTopologyRef,
            flags(), cfg, dataFormat, clientStatistics, listenerServer);
      // TODO: this relies upon the operation being the stage we complete.. may need to refactor later
      codec.executeCommand(op, channelFactory);
      return op;
   }

   public CompletionStage<ServerStatistics> newStatsOperation() {
      Codec codec = getCodec();
      return codec.executeCommand(new StatsOperation(codec, channelFactory, cacheNameBytes, clientTopologyRef, flags(),
            cfg), channelFactory);
   }

   public <V> CompletableFuture<V> newPutKeyValueOperation(Object key, byte[] keyBytes, byte[] value,
                                                           long lifespan, TimeUnit lifespanTimeUnit, long maxIdle,
                                                           TimeUnit maxIdleTimeUnit, DataFormat dataFormat) {
      Codec codec = getCodec();
      return codec.executeCommand(new PutOperation<V>(codec, channelFactory, key, keyBytes, cacheNameBytes,
            clientTopologyRef, flags(lifespan, maxIdle), cfg, value, lifespan, lifespanTimeUnit, maxIdle,
            maxIdleTimeUnit, dataFormat, clientStatistics, telemetryService), channelFactory)
            .toCompletableFuture();
   }

   public CompletableFuture<Void> newPutAllOperation(Map<byte[], byte[]> map,
                                                     long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit, DataFormat dataFormat) {
      Codec codec = getCodec();
      return codec.executeCommand(new PutAllParallelOperation(codec, channelFactory, map, cacheNameBytes,
            clientTopologyRef, flags(lifespan, maxIdle), cfg, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit,
            dataFormat, clientStatistics, telemetryService), channelFactory)
            .toCompletableFuture();
   }

   public <V> CompletableFuture<V> newPutIfAbsentOperation(Object key, byte[] keyBytes, byte[] value,
                                                              long lifespan, TimeUnit lifespanUnit, long maxIdleTime,
                                                              TimeUnit maxIdleTimeUnit, DataFormat dataFormat) {
      Codec codec = getCodec();
      return codec.executeCommand(new PutIfAbsentOperation<V>(codec, channelFactory, key, keyBytes, cacheNameBytes,
            clientTopologyRef, flags(lifespan, maxIdleTime), cfg, value, lifespan, lifespanUnit, maxIdleTime,
            maxIdleTimeUnit, dataFormat, clientStatistics, telemetryService), channelFactory)
            .toCompletableFuture();
   }

   public <V> CompletableFuture<V> newReplaceOperation(Object key, byte[] keyBytes, byte[] values,
                                                      long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit, DataFormat dataFormat) {
      Codec codec = getCodec();
      return codec.executeCommand(new ReplaceOperation<V>(codec, channelFactory, key, keyBytes, cacheNameBytes,
            clientTopologyRef, flags(lifespan, maxIdle), cfg, values, lifespan, lifespanTimeUnit, maxIdle,
            maxIdleTimeUnit, dataFormat, clientStatistics, telemetryService), channelFactory)
            .toCompletableFuture();
   }

   public CompletableFuture<Boolean> newContainsKeyOperation(Object key, byte[] keyBytes, DataFormat dataFormat) {
      Codec codec = getCodec();
      return codec.executeCommand(new ContainsKeyOperation(codec, channelFactory, key, keyBytes, cacheNameBytes,
            clientTopologyRef, flags(), cfg, dataFormat, clientStatistics), channelFactory)
            .toCompletableFuture();
   }

   public CompletableFuture<Void> newClearOperation() {
      Codec codec = getCodec();
      return codec.executeCommand(new ClearOperation(codec, channelFactory, cacheNameBytes, clientTopologyRef, flags(),
            cfg, telemetryService), channelFactory)
            .toCompletableFuture();
   }

   public <K> CompletionStage<Set<K>> newBulkGetKeysOperation(int scope, DataFormat dataFormat) {
      Codec codec = getCodec();
      return codec.executeCommand(new BulkGetKeysOperation<>(codec, channelFactory, cacheNameBytes, clientTopologyRef,
            flags(), cfg, scope, dataFormat, clientStatistics), channelFactory);
   }

   public CompletionStage<SocketAddress> newAddClientListenerOperation(Object listener, DataFormat dataFormat) {
      Codec codec = getCodec();
      return codec.executeCommand(new AddClientListenerOperation(codec, channelFactory, cacheName, clientTopologyRef,
            flags(), cfg, listenerNotifier, listener, null, null, dataFormat,
            null, telemetryService), channelFactory);
   }

   public CompletionStage<SocketAddress> newAddClientListenerOperation(
         Object listener, byte[][] filterFactoryParams, byte[][] converterFactoryParams, DataFormat dataFormat) {
      Codec codec = getCodec();
      return codec.executeCommand(new AddClientListenerOperation(codec, channelFactory, cacheName, clientTopologyRef,
            flags(), cfg, listenerNotifier, listener, filterFactoryParams, converterFactoryParams, dataFormat,
            null, telemetryService), channelFactory);
   }

   public CompletionStage<Void> newRemoveClientListenerOperation(Object listener) {
      Codec codec = getCodec();
      return codec.executeCommand(new RemoveClientListenerOperation(getCodec(), channelFactory, cacheNameBytes,
            clientTopologyRef, flags(), cfg, listenerNotifier, listener), channelFactory);
   }

   public CompletionStage<SocketAddress> newAddNearCacheListenerOperation(Object listener, DataFormat dataFormat,
                                                                          int bloomFilterBits, InternalRemoteCache<?, ?> remoteCache) {
      Codec codec = getCodec();
      return codec.executeCommand(new AddBloomNearCacheClientListenerOperation(codec, channelFactory, cacheName,
            clientTopologyRef, flags(), cfg, listenerNotifier, listener, dataFormat, bloomFilterBits, remoteCache),
            channelFactory);
   }

   public CompletionStage<Void> newUpdateBloomFilterOperation(SocketAddress address, byte[] bloomBytes) {
      Codec codec = getCodec();
      return codec.executeCommand(new UpdateBloomFilterOperation(getCodec(), channelFactory, cacheNameBytes, clientTopologyRef, flags(), cfg, address,
            bloomBytes), channelFactory);
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
      Codec codec = getCodec();
      return codec.executeCommand(new FaultTolerantPingOperation(codec, channelFactory, cacheNameBytes,
            clientTopologyRef, flags(), cfg), channelFactory);
   }

   public CompletionStage<BaseQueryResponse<?>> newQueryOperation(RemoteQuery<?> remoteQuery, DataFormat dataFormat) {
      Codec codec = getCodec();
      return codec.executeCommand(new QueryOperation(codec, channelFactory, cacheNameBytes, clientTopologyRef, flags(),
            cfg, remoteQuery, dataFormat), channelFactory);
   }

   public CompletableFuture<Integer> newSizeOperation() {
      Codec codec = getCodec();
      return codec.executeCommand(new SizeOperation(codec, channelFactory, cacheNameBytes, clientTopologyRef, flags(),
            cfg, telemetryService), channelFactory).toCompletableFuture();
   }

   public <T> CompletionStage<T> newExecuteOperation(String taskName, Map<String, byte[]> marshalledParams, Object key, DataFormat dataFormat) {
      Codec codec = getCodec();
      return codec.executeCommand(new ExecuteOperation<T>(codec, channelFactory, cacheNameBytes, clientTopologyRef,
            flags(), cfg, taskName, marshalledParams, key, dataFormat), channelFactory);
   }

   public CompletionStage<String> newAdminOperation(String taskName, Map<String, byte[]> marshalledParams) {
      Codec codec = getCodec();
      return codec.executeCommand(new AdminOperation(codec, channelFactory, cacheNameBytes, clientTopologyRef, flags(),
            cfg, taskName, marshalledParams), channelFactory);
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

   public CompletionStage<IterationStartResponse> newIterationStartOperation(String filterConverterFactory, byte[][] filterParameters, IntSet segments, int batchSize, boolean metadata, DataFormat dataFormat, SocketAddress targetAddress) {
      Codec codec = getCodec();
      return codec.executeCommand(new IterationStartOperation(codec, flags(), cfg, cacheNameBytes, clientTopologyRef,
            filterConverterFactory, filterParameters, segments, batchSize, channelFactory, metadata, dataFormat,
            targetAddress), channelFactory);
   }

   public CompletionStage<IterationEndResponse> newIterationEndOperation(byte[] iterationId, Channel channel) {
      Codec codec = getCodec();
      return codec.executeCommand(new IterationEndOperation(codec, flags(), cfg, cacheNameBytes, clientTopologyRef,
            iterationId, channelFactory, channel), channelFactory);
   }

   public <K, E> CompletionStage<IterationNextResponse<K, E>> newIterationNextOperation(byte[] iterationId, Channel channel, KeyTracker segmentKeyTracker, DataFormat dataFormat) {
      Codec codec = getCodec();
      return codec.executeCommand(new IterationNextOperation<>(codec, flags(), cfg, cacheNameBytes, clientTopologyRef,
            iterationId, channel, channelFactory, segmentKeyTracker, dataFormat), channelFactory);
   }

   public <K> CompletionStage<ChannelInputStream> newGetStreamOperation(K key, byte[] keyBytes, int offset) {
      Codec codec = getCodec();
      return codec.executeCommand(new GetStreamOperation(codec, channelFactory, key, keyBytes, offset, cacheNameBytes,
            clientTopologyRef, flags(), cfg, clientStatistics), channelFactory);
   }

   public <K> CompletionStage<OutputStream> newPutStreamOperation(K key, byte[] keyBytes, long version, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Codec codec = getCodec();
      return codec.executeCommand(new PutStreamOperation(codec, channelFactory, key, keyBytes, cacheNameBytes,
            clientTopologyRef, flags(), cfg, version, lifespan, lifespanUnit, maxIdle, maxIdleUnit, clientStatistics,
            telemetryService), channelFactory);
   }

   public <K> CompletionStage<OutputStream> newPutStreamOperation(K key, byte[] keyBytes, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Codec codec = getCodec();
      return codec.executeCommand(new PutStreamOperation(codec, channelFactory, key, keyBytes, cacheNameBytes,
            clientTopologyRef, flags(), cfg, PutStreamOperation.VERSION_PUT, lifespan, lifespanUnit, maxIdle,
            maxIdleUnit, clientStatistics, telemetryService), channelFactory);
   }

   public <K> CompletionStage<OutputStream> newPutIfAbsentStreamOperation(K key, byte[] keyBytes, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Codec codec = getCodec();
      return codec.executeCommand(new PutStreamOperation(codec, channelFactory, key, keyBytes, cacheNameBytes,
            clientTopologyRef, flags(), cfg, PutStreamOperation.VERSION_PUT_IF_ABSENT, lifespan, lifespanUnit, maxIdle,
            maxIdleUnit, clientStatistics, telemetryService), channelFactory);
   }

   public CompletionStage<List<String>> newAuthMechListOperation(Channel channel) {
      Codec codec = getCodec();
      return codec.executeCommand(new AuthMechListOperation(codec, clientTopologyRef, cfg, channel, channelFactory),
            channelFactory);
   }

   public CompletionStage<byte[]> newAuthOperation(Channel channel, String saslMechanism, byte[] response) {
      Codec codec = getCodec();
      return codec.executeCommand(new AuthOperation(codec, clientTopologyRef, cfg, channel, channelFactory,
            saslMechanism, response), channelFactory);
   }

   public PrepareTransactionOperation newPrepareTransactionOperation(Xid xid, boolean onePhaseCommit,
                                                                  List<Modification> modifications,
                                                                  boolean recoverable, long timeoutMs) {
      Codec codec = getCodec();
      var op = new PrepareTransactionOperation(codec, channelFactory, cacheNameBytes,
            clientTopologyRef, cfg, xid, onePhaseCommit, modifications, recoverable, timeoutMs);
      codec.executeCommand(op, channelFactory);
      // TODO: This is relying upon returning op
      return op;
   }
}
