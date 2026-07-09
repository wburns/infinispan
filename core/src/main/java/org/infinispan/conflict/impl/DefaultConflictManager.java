package org.infinispan.conflict.impl;

import static org.infinispan.util.logging.Log.CLUSTER;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.cache.impl.InvocationHelper;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.PartitionHandlingConfiguration;
import org.infinispan.conflict.EntryMergePolicy;
import org.infinispan.conflict.EntryMergePolicyFactoryRegistry;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.NullCacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.statetransfer.StateConsumer;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import jakarta.transaction.TransactionManager;

/**
 * @author Ryan Emerson
 */
@Scope(Scopes.NAMED_CACHE)
public class DefaultConflictManager<K, V> implements InternalConflictManager<K, V> {

   private static final Log log = LogFactory.getLog(DefaultConflictManager.class);

   private static final long localFlags = FlagBitSets.CACHE_MODE_LOCAL| FlagBitSets.SKIP_OWNERSHIP_CHECK| FlagBitSets.SKIP_LOCKING;
   private static final long userMergeFlags = FlagBitSets.IGNORE_RETURN_VALUES;
   private static final long autoMergeFlags = FlagBitSets.IGNORE_RETURN_VALUES | FlagBitSets.PUT_FOR_STATE_TRANSFER | FlagBitSets.SKIP_REMOTE_LOOKUP;

   @ComponentName(KnownComponentNames.CACHE_NAME)
   @Inject String cacheName;
   @Inject ComponentRef<AsyncInterceptorChain> interceptorChain;
   @Inject InvocationHelper invocationHelper;
   @Inject Configuration cacheConfiguration;
   @Inject CommandsFactory commandsFactory;
   @Inject DistributionManager distributionManager;
   @Inject InvocationContextFactory invocationContextFactory;
   @Inject RpcManager rpcManager;
   @Inject ComponentRef<StateConsumer> stateConsumer;
   @Inject StateReceiver<K, V> stateReceiver;
   @Inject EntryMergePolicyFactoryRegistry mergePolicyRegistry;
   @Inject TimeService timeService;
   @Inject InternalEntryFactory internalEntryFactory;
   @Inject TransactionManager transactionManager;
   @Inject KeyPartitioner keyPartitioner;

   private Address localAddress;
   private long conflictTimeout;
   private EntryMergePolicy<K, V> entryMergePolicy;
   private final AtomicBoolean streamInProgress = new AtomicBoolean();
   private final Map<K, VersionRequest> versionRequestMap = new HashMap<>();
   private final Queue<VersionRequest> retryQueue = new ConcurrentLinkedQueue<>();
   private volatile boolean running = false;
   private volatile Subscription conflictSubscription;
   private volatile CompletableFuture<Void> conflictFuture;

   @Start
   public void start() {
      this.localAddress = rpcManager.getAddress();

      PartitionHandlingConfiguration config = cacheConfiguration.clustering().partitionHandling();
      this.entryMergePolicy = mergePolicyRegistry.createInstance(config);

      // TODO make this an explicit configuration param in PartitionHandlingConfiguration
      this.conflictTimeout = cacheConfiguration.clustering().stateTransfer().timeout();
      this.running = true;
      if (log.isTraceEnabled()) log.tracef("Cache %s starting %s. isRunning=%s", cacheName, getClass().getSimpleName(), !running);
   }

   @Stop
   public void stop() {
      this.running = false;
      synchronized (versionRequestMap) {
         if (log.isTraceEnabled()) log.tracef("Cache %s stopping %s. isRunning=%s", getClass().getSimpleName(), cacheName, running);
         cancelVersionRequests();
         versionRequestMap.clear();
      }

      Subscription subscription = conflictSubscription;
      if (isConflictResolutionInProgress() && subscription != null)
         subscription.cancel();
   }

   @Override
   public StateReceiver getStateReceiver() {
      return stateReceiver;
   }

   @Override
   public void cancelVersionRequests() {
      if (!running)
         return;

      synchronized (versionRequestMap) {
         versionRequestMap.values().forEach(VersionRequest::cancelRequestIfOutdated);
      }
   }

   @Override
   public void restartVersionRequests() {
      if (!running)
         return;

      VersionRequest request;
      while ((request = retryQueue.poll()) != null) {
         if (log.isTraceEnabled()) log.tracef("Retrying %s", request);
         request.start();
      }
   }

   @Override
   public Map<Address, InternalCacheValue<V>> getAllVersions(final K key) {
      checkIsRunning();

      final VersionRequest request;
      synchronized (versionRequestMap) {
         request = versionRequestMap.computeIfAbsent(key, k -> new VersionRequest(k, stateConsumer.running().isStateTransferInProgress()));
      }

      try {
         return request.completableFuture.get();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      } catch (ExecutionException e) {
         if (e.getCause() instanceof CacheException)
            throw (CacheException) e.getCause();

         throw new CacheException(e.getCause());
      } finally {
         synchronized (versionRequestMap) {
            versionRequestMap.remove(key);
         }
      }
   }

   @Deprecated(forRemoval = true)
   @Override
   public Stream<Map<Address, CacheEntry<K, V>>> getConflicts() {
      return Flowable.fromPublisher(getConflictsPublisher()).blockingStream();
   }

   @Override
   public Publisher<Map<Address, CacheEntry<K, V>>> getConflictsPublisher() {
      checkIsRunning();
      return getConflictsFlowable(distributionManager.getCacheTopology());
   }

   private Flowable<Map<Address, CacheEntry<K, V>>> getConflictsFlowable(LocalizedCacheTopology topology) {
      if (log.isTraceEnabled()) log.tracef("getConflicts isStateTransferInProgress=%s, topology=%s", stateConsumer.running().isStateTransferInProgress(), topology);
      if (topology.getPhase() != CacheTopology.Phase.CONFLICT_RESOLUTION && stateConsumer.running().isStateTransferInProgress()) {
         throw CLUSTER.getConflictsStateTransferInProgress(cacheName);
      }

      int totalSegments = topology.getWriteConsistentHash().getNumSegments();
      long endTime = timeService.expectedEndTime(conflictTimeout, TimeUnit.MILLISECONDS);

      return Flowable.range(0, totalSegments)
            .concatMapSingle(segmentId -> {
               if (log.isTraceEnabled())
                  log.tracef("Cache %s attempting to receive all replicas for segment %s with topology %s", cacheName, segmentId, topology);
               long remainingTime = timeService.remainingTime(endTime, TimeUnit.MILLISECONDS);
               return Single.fromCompletionStage(
                     stateReceiver.getAllReplicasForSegment(segmentId, topology, remainingTime));
            })
            .concatMapIterable(list -> list)
            .filter(map -> map.values().stream().distinct().limit(2).count() > 1 || map.values().isEmpty())
            .onErrorResumeNext(e -> {
               if (log.isTraceEnabled()) log.tracef("Cache %s conflict publisher caught %s", cacheName, e);
               stateReceiver.cancelRequests();
               Throwable cause = e.getCause();
               if (e instanceof CancellationException || cause instanceof CancellationException)
                  return Flowable.empty();
               return Flowable.error(e instanceof CacheException ? e :
                     new CacheException(e.getMessage(), cause != null ? cause : e));
            })
            .doOnSubscribe(s -> {
               if (!streamInProgress.compareAndSet(false, true))
                  throw CLUSTER.getConflictsAlreadyInProgress();
            })
            .doFinally(() -> streamInProgress.set(false));
   }

   @Override
   public boolean isConflictResolutionInProgress() {
      return streamInProgress.get();
   }

   @Override
   public void resolveConflicts() {
      if (entryMergePolicy == null)
         throw new CacheException("Cannot resolve conflicts as no EntryMergePolicy has been configured");

      resolveConflicts(entryMergePolicy);
   }

   @Override
   public void resolveConflicts(EntryMergePolicy<K, V> mergePolicy) {
      checkIsRunning();
      CompletionStages.join(doResolveConflicts(distributionManager.getCacheTopology(), mergePolicy, null));
   }

   @Override
   public CompletionStage<Void> resolveConflicts(CacheTopology topology, Set<Address> preferredNodes) {
      if (!running)
         return CompletableFuture.completedFuture(null);

      LocalizedCacheTopology localizedTopology;
      if (topology instanceof LocalizedCacheTopology) {
         localizedTopology = (LocalizedCacheTopology) topology;
      } else {
         localizedTopology = distributionManager.createLocalizedCacheTopology(topology);
      }
      conflictFuture = doResolveConflicts(localizedTopology, entryMergePolicy, preferredNodes)
            .toCompletableFuture();
      return conflictFuture.whenComplete((v, t) -> {
         if (t != null) {
            Subscription subscription = conflictSubscription;
            if (subscription != null) {
               subscription.cancel();
               conflictSubscription = null;
            }
         }
      });
   }

   @Override
   public void cancelConflictResolution() {
      if (conflictFuture != null && !conflictFuture.isDone()) {
         if (log.isTraceEnabled()) log.tracef("Cache %s cancelling conflict resolution future", cacheName);
         conflictFuture.cancel(true);
      }
   }

   private CompletionStage<Void> doResolveConflicts(final LocalizedCacheTopology topology, final EntryMergePolicy<K, V> mergePolicy,
                                                     final Set<Address> preferredNodes) {
      boolean userCall = preferredNodes == null;
      final Set<Address> preferredPartition = userCall ? new HashSet<>(topology.getCurrentCH().getMembers()) : preferredNodes;

      if (log.isTraceEnabled())
         log.tracef("Cache %s attempting to resolve conflicts.  All Members %s, Installed topology %s, Preferred Partition %s",
               cacheName, topology.getMembers(), topology, preferredPartition);

      Flowable<Map<Address, CacheEntry<K, V>>> conflicts = getConflictsFlowable(topology);
      return conflicts
            .doOnSubscribe(s -> conflictSubscription = s)
            .concatMapCompletable(conflictMap -> {
               if (log.isTraceEnabled()) log.tracef("Cache %s conflict detected %s", cacheName, conflictMap);

               Collection<CacheEntry<K, V>> entries = conflictMap.values();
               Optional<K> optionalEntry = entries.stream()
                     .filter(entry -> !(entry instanceof NullCacheEntry))
                     .map(CacheEntry::getKey)
                     .findAny();

               final K key = optionalEntry.orElseThrow(() -> new CacheException("All returned conflicts are NullCacheEntries. This should not happen!"));
               Address primaryReplica = topology.getDistribution(key).primary();

               List<Address> preferredEntries = conflictMap.entrySet().stream()
                     .map(Map.Entry::getKey)
                     .filter(preferredPartition::contains)
                     .collect(Collectors.toList());

               CacheEntry<K, V> preferredEntry;
               if (preferredEntries.size() == 1) {
                  preferredEntry = conflictMap.remove(preferredEntries.get(0));
               } else {
                  preferredEntry = conflictMap.remove(primaryReplica);
               }

               if (log.isTraceEnabled()) log.tracef("Cache %s applying EntryMergePolicy %s to PreferredEntry %s, otherEntries %s",
                     cacheName, mergePolicy.getClass().getName(), preferredEntry, entries);

               CacheEntry<K, V> entry = preferredEntry instanceof NullCacheEntry ? null : preferredEntry;
               List<CacheEntry<K, V>> otherEntries = entries.stream().filter(e -> !(e instanceof NullCacheEntry)).collect(Collectors.toList());
               CacheEntry<K, V> mergedEntry = mergePolicy.merge(entry, otherEntries);

               return Completable.fromCompletionStage(
                     applyMergeResult(userCall, key, mergedEntry)
                           .whenComplete((responseMap, exception) -> {
                              if (log.isTraceEnabled()) log.tracef("Cache %s resolveConflicts future complete for key %s: ResponseMap=%s",
                                    cacheName, key, responseMap);
                              if (exception != null)
                                 log.exceptionDuringConflictResolution(key, exception);
                           })
               );
            })
            .doOnComplete(() -> {
               if (log.isTraceEnabled()) log.tracef("Cache %s finished resolving conflicts for topologyId=%s", cacheName, topology.getTopologyId());
            })
            .toCompletionStage(null);
   }

   private CompletableFuture<V> applyMergeResult(boolean userCall, K key, CacheEntry<K, V> mergedEntry) {
      long flags = userCall ? userMergeFlags : autoMergeFlags;
      VisitableCommand command;
      if (mergedEntry == null) {
         if (log.isTraceEnabled()) log.tracef("Cache %s executing remove on conflict: key %s", cacheName, key);
         command = commandsFactory.buildRemoveCommand(key, null, keyPartitioner.getSegment(key), flags);
      } else {
         if (log.isTraceEnabled()) log.tracef("Cache %s executing update on conflict: key %s with value %s", cacheName, key, mergedEntry
               .getValue());
         command = commandsFactory.buildPutKeyValueCommand(key, mergedEntry.getValue(), keyPartitioner.getSegment(key),
                                                           mergedEntry.getMetadata(), flags);
      }
      try {
         assert transactionManager == null || transactionManager.getTransaction() == null : "Transaction active on conflict resolution thread";
         InvocationContext ctx = invocationHelper.createInvocationContextWithImplicitTransaction(1, true);
         return invocationHelper.invokeAsync(ctx, command);
      } catch (Exception e) {
         return CompletableFuture.failedFuture(e);
      }
   }

   @Override
   public boolean isStateTransferInProgress() {
      return stateConsumer.running().isStateTransferInProgress();
   }

   private void checkIsRunning() {
      if (!running)
         throw new CacheException(String.format("Cache %s unable to process request as the ConflictManager has been stopped", cacheName));
   }

   private class VersionRequest {
      final K key;
      final boolean postpone;
      final CompletableFuture<Map<Address, InternalCacheValue<V>>> completableFuture = new CompletableFuture<>();
      volatile CompletableFuture<Map<Address, Response>> rpcFuture;
      volatile Collection<Address> keyOwners;

      VersionRequest(K key, boolean postpone) {
         this.key = key;
         this.postpone = postpone;

         if (log.isTraceEnabled()) log.tracef("Cache %s creating %s", cacheName,this);

         if (postpone) {
            retryQueue.add(this);
         } else {
            start();
         }
      }

      void cancelRequestIfOutdated() {
         Collection<Address> latestOwners = distributionManager.getCacheTopology().getWriteOwners(key);
         if (rpcFuture != null && !completableFuture.isDone() && !keyOwners.equals(latestOwners)) {
            rpcFuture = null;
            keyOwners.clear();
            if (rpcFuture.cancel(false)) {
               retryQueue.add(this);

               if (log.isTraceEnabled()) log.tracef("Cancelling %s for nodes %s. New write owners %s", this, keyOwners, latestOwners);
            }
         }
      }

      void start() {
         LocalizedCacheTopology topology = distributionManager.getCacheTopology();
         DistributionInfo info = topology.getDistribution(key);
         keyOwners = info.writeOwners();

         if (log.isTraceEnabled()) log.tracef("Attempting %s from owners %s", this, keyOwners);

         final Map<Address, InternalCacheValue<V>> versionsMap = new HashMap<>();
         if (keyOwners.contains(localAddress)) {
            GetCacheEntryCommand cmd = commandsFactory.buildGetCacheEntryCommand(key, info.segmentId(), localFlags);
            InvocationContext ctx = invocationContextFactory.createNonTxInvocationContext();
            CacheEntry<K, V> entry = (CacheEntry<K, V>) interceptorChain.running().invoke(ctx, cmd);
            InternalCacheValue<V> icv = entry != null ? internalEntryFactory.createValue(entry) : null;
            synchronized (versionsMap) {
               versionsMap.put(localAddress, icv);
            }
         }

         ClusteredGetCommand cmd = commandsFactory.buildClusteredGetCommand(key, info.segmentId(), FlagBitSets.SKIP_OWNERSHIP_CHECK);
         cmd.setTopologyId(topology.getTopologyId());
         MapResponseCollector collector = MapResponseCollector.ignoreLeavers(keyOwners.size());
         rpcFuture = rpcManager.invokeCommand(keyOwners, cmd, collector, rpcManager.getSyncRpcOptions()).toCompletableFuture();
         rpcFuture.whenComplete((responseMap, exception) -> {
            if (log.isTraceEnabled()) log.tracef("%s received responseMap %s, exception %s", this, responseMap, exception);

            if (exception != null) {
               String msg = String.format("%s encountered when attempting '%s' on cache '%s'", exception.getCause(), this, cacheName);
               completableFuture.completeExceptionally(new CacheException(msg, exception.getCause()));
               return;
            }

            for (Map.Entry<Address, Response> entry : responseMap.entrySet()) {
               if (log.isTraceEnabled()) log.tracef("%s received response %s from %s", this, entry.getValue(), entry.getKey());
               Response rsp = entry.getValue();
               if (rsp instanceof SuccessfulResponse) {
                  SuccessfulResponse response = (SuccessfulResponse) rsp;
                  Object rspVal = response.getResponseValue();
                  synchronized (versionsMap) {
                     versionsMap.put(entry.getKey(), (InternalCacheValue<V>) rspVal);
                  }
               } else if(rsp instanceof UnsureResponse) {
                  log.debugf("Received UnsureResponse, restarting request %s", this);
                  this.start();
                  return;
               } else if (rsp instanceof CacheNotFoundResponse) {
                  if (log.isTraceEnabled()) log.tracef("Ignoring CacheNotFoundResponse: %s", rsp);
               } else {
                  completableFuture.completeExceptionally(new CacheException(String.format("Unable to retrieve key %s from %s: %s", key, entry.getKey(), entry.getValue())));
                  return;
               }
            }
            completableFuture.complete(versionsMap);
         });
      }

      @Override
      public String toString() {
         return "VersionRequest{" +
               "key=" + key +
               ", postpone=" + postpone +
               '}';
      }
   }

}
