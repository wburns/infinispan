package org.infinispan.container.versioning.irac;

import static org.infinispan.remoting.transport.impl.VoidResponseCollector.ignoreLeavers;
import static org.infinispan.remoting.transport.impl.VoidResponseCollector.validOnly;
import static org.infinispan.util.concurrent.CompletableFutures.completedNull;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.irac.IracTombstoneCleanupCommand;
import org.infinispan.commands.irac.IracTombstonePrimaryCheckCommand;
import org.infinispan.commands.irac.IracTombstoneRemoteSiteCheckCommand;
import org.infinispan.commands.irac.IracTombstoneStateResponseCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.XSiteStateTransferConfiguration;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.ExponentialBackOff;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.irac.DefaultIracManager;
import org.infinispan.xsite.irac.IracExecutor;
import org.infinispan.xsite.irac.IracManager;
import org.infinispan.xsite.irac.IracXSiteBackup;
import org.infinispan.xsite.status.SiteState;
import org.infinispan.xsite.status.TakeOfflineManager;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.CompletableObserver;
import io.reactivex.rxjava3.core.CompletableSource;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.flowables.GroupedFlowable;
import io.reactivex.rxjava3.functions.Predicate;
import net.jcip.annotations.GuardedBy;

/**
 * A default implementation for {@link IracTombstoneManager}.
 * <p>
 * This class is responsible to keep track of the tombstones for the IRAC algorithm. Tombstones are used when a key is
 * removed but its metadata is necessary to detect possible conflicts in this and remote sites. When all sites have
 * updated the key, the tombstone can be removed.
 * <p>
 * Tombstones are removed periodically in the background.
 *
 * @since 14.0
 */
@Scope(Scopes.NAMED_CACHE)
public class DefaultIracTombstoneManager implements IracTombstoneManager {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   @Inject DistributionManager distributionManager;
   @Inject RpcManager rpcManager;
   @Inject CommandsFactory commandsFactory;
   @Inject TakeOfflineManager takeOfflineManager;
   @Inject ComponentRef<IracManager> iracManager;
   @ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR)
   @Inject ScheduledExecutorService scheduledExecutorService;
   @Inject @ComponentName(KnownComponentNames.BLOCKING_EXECUTOR) Executor blockingExecutor;
   private final Map<Object, IracTombstoneInfo> tombstoneMap;
   private final IracExecutor iracExecutor;
   private final Collection<IracXSiteBackup> asyncBackups;
   private final Scheduler scheduler;
   private volatile boolean stopped = true;
   private final int batchSize;

   public DefaultIracTombstoneManager(Configuration configuration) {
      iracExecutor = new IracExecutor(this::performCleanup);
      asyncBackups = DefaultIracManager.asyncBackups(configuration);
      tombstoneMap = new ConcurrentHashMap<>(configuration.sites().tombstoneMapSize());
      scheduler = new Scheduler(configuration.sites().tombstoneMapSize(), configuration.sites().maxTombstoneCleanupDelay());
      batchSize = configuration.sites().asyncBackupsStream()
            .map(BackupConfiguration::stateTransfer)
            .map(XSiteStateTransferConfiguration::chunkSize)
            .reduce(1, Integer::max);

   }

   @Start
   public void start() {
      Transport transport = rpcManager.getTransport();
      transport.checkCrossSiteAvailable();
      String localSiteName = transport.localSiteName();
      asyncBackups.removeIf(xSiteBackup -> localSiteName.equals(xSiteBackup.getSiteName()));
      iracExecutor.setBackOff(ExponentialBackOff.NO_OP);
      iracExecutor.setExecutor(blockingExecutor);
      stopped = false;
      scheduler.disabled = false;
      scheduler.scheduleWithCurrentDelay();
   }

   @Stop
   public void stop() {
      stopped = true;
      stopCleanupTask();
      // drop everything
      tombstoneMap.clear();
   }

   // for testing purposes only!
   public void stopCleanupTask() {
      scheduler.disable();
   }

   public void storeTombstone(int segment, Object key, IracMetadata metadata) {
      IracTombstoneInfo tombstone = new IracTombstoneInfo(key, segment, metadata);
      tombstoneMap.put(key, tombstone);
      if (log.isTraceEnabled()) {
         log.tracef("[IRAC] Tombstone stored: %s", tombstone);
      }
   }

   @Override
   public void storeTombstoneIfAbsent(IracTombstoneInfo tombstone) {
      if (tombstone == null) {
         return;
      }
      boolean added = tombstoneMap.putIfAbsent(tombstone.getKey(), tombstone) == null;
      if (log.isTraceEnabled()) {
         log.tracef("[IRAC] Tombstone stored? %s. %s", added, tombstone);
      }
   }

   @Override
   public IracMetadata getTombstone(Object key) {
      IracTombstoneInfo tombstone = tombstoneMap.get(key);
      return tombstone == null ? null : tombstone.getMetadata();
   }

   @Override
   public void removeTombstone(IracTombstoneInfo tombstone) {
      if (tombstone == null) {
         return;
      }
      boolean removed = tombstoneMap.remove(tombstone.getKey(), tombstone);
      if (log.isTraceEnabled()) {
         log.tracef("[IRAC] Tombstone removed? %s. %s", removed, tombstone);
      }
   }

   @Override
   public void removeTombstone(Object key) {
      IracTombstoneInfo tombstone = tombstoneMap.remove(key);
      if (tombstone != null && log.isTraceEnabled()) {
         log.tracef("[IRAC] Tombstone removed %s", tombstone);
      }
   }

   @Override
   public boolean isEmpty() {
      return tombstoneMap.isEmpty();
   }

   @Override
   public int size() {
      return tombstoneMap.size();
   }

   @Override
   public boolean isTaskRunning() {
      return scheduler.running;
   }

   @Override
   public long getCurrentDelayMillis() {
      return scheduler.currentDelayMillis;
   }

   @Override
   public void sendStateTo(Address requestor, IntSet segments) {
      StateTransferHelper helper = new StateTransferHelper(requestor, segments);
      Flowable.fromIterable(tombstoneMap.values())
            .filter(helper)
            .buffer(batchSize)
            .concatMapCompletableDelayError(helper)
            .subscribe(helper);
   }

   @Override
   public void checkStaleTombstone(Collection<? extends IracTombstoneInfo> tombstones) {
      boolean trace = log.isTraceEnabled();
      if (trace) {
         log.tracef("[IRAC] Checking for stale tombstones from backup owner. %s", tombstones);
      }
      LocalizedCacheTopology topology = distributionManager.getCacheTopology();
      IntSet segments = IntSets.mutableEmptySet();
      IracTombstoneCleanupCommand cmd = commandsFactory.buildIracTombstoneCleanupCommand(tombstones.size());
      for (IracTombstoneInfo tombstone : tombstones) {
         IracTombstoneInfo data = tombstoneMap.get(tombstone.getKey());
         if (!topology.getSegmentDistribution(tombstone.getSegment()).isPrimary() || (tombstone.equals(data))) {
            // not a primary owner or the data is the same (i.e. it is valid)
            continue;
         }
         segments.add(tombstone.getSegment());
         cmd.add(tombstone);
      }
      if (cmd.isEmpty()) {
         if (trace) {
            log.trace("[IRAC] Nothing to send.");
         }
         return;
      }
      Collection<Address> owners = segments.stream()
            .flatMap(segment -> getSegmentDistribution(segment).writeOwners().stream())
            .distinct()
            .collect(Collectors.toList());
      if (trace) {
         log.tracef("[IRAC] Cleaning up %d tombstones: %s", cmd.getTombstonesToRemove().size(), cmd.getTombstonesToRemove());
      }
      rpcManager.sendToMany(owners, cmd, DeliverOrder.NONE);
   }

   // Testing purposes
   public void startCleanupTombstone() {
      iracExecutor.run();
   }

   // Testing purposes
   public void runCleanupAndWait() {
      performCleanup().toCompletableFuture().join();
   }

   // Testing purposes
   public boolean contains(IracTombstoneInfo tombstone) {
      return tombstone.equals(tombstoneMap.get(tombstone.getKey()));
   }

   private CompletionStage<Void> performCleanup() {
      if (stopped) {
         return CompletableFutures.completedNull();
      }
      boolean trace = log.isTraceEnabled();

      if (trace) {
         log.trace("[IRAC] Starting tombstone cleanup round.");
      }

      scheduler.onTaskStarted(tombstoneMap.size());
      CompletionStageObservable cso = new CompletionStageObservable();

      Flowable.fromIterable(tombstoneMap.values())
            .groupBy(this::classifyTombstone, t -> t)
            .concatMapCompletableDelayError(group -> {
               switch (group.getKey()) {
                  case REMOVE_TOMBSTONE:
                     return removeAllTombstones(group);
                  case NOTIFY_PRIMARY_OWNER:
                     return notifyPrimaryOwner(group);
                  case CHECK_REMOTE_SITE:
                     return checkRemoteSite(group);
                  case KEEP_TOMBSTONE:
                  default:
                     return Completable.complete();
               }
            })
            .subscribe(cso);

      return cso.whenComplete(scheduler);
   }

   private DistributionInfo getSegmentDistribution(int segment) {
      return distributionManager.getCacheTopology().getSegmentDistribution(segment);
   }

   private CompletableSource removeAllTombstones(GroupedFlowable<Action, IracTombstoneInfo> flowable) {
      return flowable.concatMapCompletableDelayError(tombstone -> {
         removeTombstone(tombstone);
         return Completable.complete();
      });
   }

   private CompletableSource notifyPrimaryOwner(GroupedFlowable<Action, IracTombstoneInfo> flowable) {
      return flowable.groupBy(IracTombstoneInfo::getSegment, t -> t)
            .concatMapCompletableDelayError(segment -> segment.buffer(batchSize)
                  .concatMapCompletableDelayError(tombstones -> new PrimaryOwnerCheckTask(segment.getKey(), tombstones).check()));
   }

   private CompletableSource checkRemoteSite(GroupedFlowable<Action, IracTombstoneInfo> flowable) {
      return flowable.buffer(batchSize)
            .concatMapCompletableDelayError(tombstoneMap -> new CleanupTask(tombstoneMap).check());
   }

   private Action classifyTombstone(IracTombstoneInfo tombstone) {
      DistributionInfo info = getSegmentDistribution(tombstone.getSegment());
      if (!info.isWriteOwner() && !info.isReadOwner()) {
         // not an owner, remove tombstone from local map
         return Action.REMOVE_TOMBSTONE;
      } else if (!info.isPrimary()) {
         // backup owner, notify primary owner to check if the tombstone can be cleanup
         return iracManager.running().containsKey(tombstone.getKey()) ? Action.KEEP_TOMBSTONE : Action.NOTIFY_PRIMARY_OWNER;
      } else {
         // primary owner, check all remote sites.
         return iracManager.running().containsKey(tombstone.getKey()) ? Action.KEEP_TOMBSTONE : Action.CHECK_REMOTE_SITE;
      }
   }

   private final class CleanupTask implements Function<Void, CompletionStage<Void>>, Runnable {
      private final Collection<IracTombstoneInfo> tombstoneToCheck;
      private final IntSet tombstoneToKeep;
      private final int id;
      private volatile boolean failedToCheck;

      private CleanupTask(Collection<IracTombstoneInfo> tombstoneToCheck) {
         this.tombstoneToCheck = tombstoneToCheck;
         tombstoneToKeep = IntSets.concurrentSet(tombstoneToCheck.size());
         failedToCheck = false;
         id = tombstoneToCheck.hashCode();
      }

      CompletableSource check() {
         if (log.isTraceEnabled()) {
            log.tracef("[cleanup-task-%d] Running cleanup task with %s tombstones to check", id, tombstoneToCheck.size());
         }
         if (tombstoneToCheck.isEmpty()) {
            return Completable.complete();
         }
         List<Object> keys = tombstoneToCheck.stream()
               .map(IracTombstoneInfo::getKey)
               .collect(Collectors.toList());
         IracTombstoneRemoteSiteCheckCommand cmd = commandsFactory.buildIracTombstoneRemoteSiteCheckCommand(keys);
         // if one of the site return true (i.e. the key is in updateKeys map, then do not remove it)
         AggregateCompletionStage<Void> stage = CompletionStages.aggregateCompletionStage();
         for (XSiteBackup backup : asyncBackups) {
            if (takeOfflineManager.getSiteState(backup.getSiteName()) == SiteState.OFFLINE) {
               continue; // backup is offline
            }
            // we don't need the tombstone to query the remote site
            stage.dependsOn(rpcManager.invokeXSite(backup, cmd).thenAccept(this::mergeIntSet));
         }
         // in case of exception, keep the tombstone
         return Completable.fromCompletionStage(
               stage.freeze()
                     .exceptionally(this::onException)
                     .thenComposeAsync(this, blockingExecutor));
      }

      private void mergeIntSet(IntSet rsp) {
         if (log.isTraceEnabled()) {
            log.tracef("[cleanup-task-%d] Received response: %s", id, rsp);
         }
         tombstoneToKeep.addAll(rsp);
      }

      private Void onException(Throwable ignored) {
         if (log.isTraceEnabled()) {
            log.tracef(ignored, "[cleanup-task-%d] Received exception", id);
         }
         failedToCheck = true;
         return null;
      }

      @Override
      public CompletionStage<Void> apply(Void aVoid) {
         IracTombstoneCleanupCommand cmd = commandsFactory.buildIracTombstoneCleanupCommand(tombstoneToCheck.size());
         forEachTombstoneToRemove(cmd::add);

         if (log.isTraceEnabled()) {
            log.tracef("[cleanup-task-%d] Removing %d tombstones.", id, cmd.getTombstonesToRemove().size());
         }

         if (cmd.isEmpty()) {
            // nothing to remove
            return completedNull();
         }

         Collection<Address> owners = tombstoneToCheck.stream()
               .map(IracTombstoneInfo::getSegment)
               .distinct()
               .flatMap(integer -> getSegmentDistribution(integer).writeOwners().stream())
               .collect(Collectors.toList());

         // send cleanup to all write owner
         return rpcManager.invokeCommand(owners, cmd, validOnly(), rpcManager.getSyncRpcOptions())
               .thenRunAsync(this, blockingExecutor); //removes from local map
      }

      @Override
      public void run() {
         forEachTombstoneToRemove(DefaultIracTombstoneManager.this::removeTombstone);
      }

      void forEachTombstoneToRemove(Consumer<IracTombstoneInfo> consumer) {
         if (failedToCheck) {
            return;
         }
         int index = 0;
         for (IracTombstoneInfo tombstone : tombstoneToCheck) {
            if (tombstoneToKeep.contains(index++)) {
               continue;
            }
            consumer.accept(tombstone);
         }
      }
   }

   private class StateTransferHelper implements Predicate<IracTombstoneInfo>,
         io.reactivex.rxjava3.functions.Function<Collection<IracTombstoneInfo>, CompletableSource>,
         CompletableObserver {
      private final Address requestor;
      private final IntSet segments;

      private StateTransferHelper(Address requestor, IntSet segments) {
         this.requestor = requestor;
         this.segments = segments;
      }

      @Override
      public boolean test(IracTombstoneInfo tombstone) {
         return segments.contains(tombstone.getSegment());
      }

      @Override
      public CompletableSource apply(Collection<IracTombstoneInfo> state) {
         RpcOptions rpcOptions = rpcManager.getSyncRpcOptions();
         IracTombstoneStateResponseCommand cmd = commandsFactory.buildIracTombstoneStateResponseCommand(state);
         CompletionStage<Void> rsp = rpcManager.invokeCommand(requestor, cmd, ignoreLeavers(), rpcOptions);
         return Completable.fromCompletionStage(rsp);
      }

      @Override
      public void onSubscribe(@NonNull Disposable d) {
         //no-op
      }

      @Override
      public void onComplete() {
         if (log.isDebugEnabled()) {
            log.debugf("Tombstones transferred to %s for segments %s", requestor, segments);
         }
      }

      @Override
      public void onError(@NonNull Throwable e) {
         log.failedToTransferTombstones(requestor, segments, e);
      }
   }

   private final class Scheduler implements BiConsumer<Void, Throwable> {
      final int targetSize;
      final long maxDelayMillis;

      int preCleanupSize;
      int previousPostCleanupSize;

      long currentDelayMillis;
      volatile boolean running;
      volatile boolean disabled;
      @GuardedBy("this")
      ScheduledFuture<?> future;

      private Scheduler(int targetSize, long maxDelayMillis) {
         this.targetSize = targetSize;
         this.maxDelayMillis = maxDelayMillis;
         currentDelayMillis = maxDelayMillis / 2;
      }

      void onTaskStarted(int size) {
         running = true;
         preCleanupSize = size;
      }

      void onTaskCompleted(int postCleanupSize) {
         if (postCleanupSize >= targetSize) {
            // The tombstones map is already at or above the target size, start a new cleanup round immediately
            // Keep the delay >= 1 to simplify the tombstoneCreationRate calculation
            currentDelayMillis = 1;
         } else {
            // Estimate how long it would take for the tombstones map to reach the target size
            double tombstoneCreationRate = (preCleanupSize - previousPostCleanupSize) * 1.0 / currentDelayMillis;
            double estimationMillis;
            if (tombstoneCreationRate <= 0) {
               // The tombstone map will never reach the target size, use the maximum delay
               estimationMillis = maxDelayMillis;
            } else {
               // Ensure that 1 <= estimation <= maxDelayMillis
               estimationMillis = Math.min((targetSize - postCleanupSize) / tombstoneCreationRate + 1, maxDelayMillis);
            }
            // Use a geometric average between the current estimation and the previous one
            // to dampen the changes as the rate changes from one interval to the next
            // (especially when the interval duration is very short)
            currentDelayMillis = Math.round(Math.sqrt(currentDelayMillis * estimationMillis));
         }
         previousPostCleanupSize = postCleanupSize;
         scheduleWithCurrentDelay();
      }

      synchronized void scheduleWithCurrentDelay() {
         running = false;
         if (stopped || disabled) {
            return;
         }
         if (future != null) {
            future.cancel(true);
         }
         future = scheduledExecutorService.schedule(iracExecutor, currentDelayMillis, TimeUnit.MILLISECONDS);
      }

      synchronized void disable() {
         disabled = true;
         if (future != null) {
            future.cancel(true);
            future = null;
         }
      }

      @Override
      public void accept(Void unused, Throwable throwable) {
         // invoked after the cleanup round
         onTaskCompleted(tombstoneMap.size());
      }
   }

   private class PrimaryOwnerCheckTask {
      private final int segment;
      private final Collection<IracTombstoneInfo> tombstones;

      private PrimaryOwnerCheckTask(int segment, Collection<IracTombstoneInfo> tombstones) {
         this.segment = segment;
         this.tombstones = tombstones;
         assert consistencyCheck();
      }

      CompletableSource check() {
         if (tombstones.isEmpty()) {
            return Completable.complete();
         }
         IracTombstonePrimaryCheckCommand cmd = commandsFactory.buildIracTombstonePrimaryCheckCommand(tombstones);
         RpcOptions rpcOptions = rpcManager.getSyncRpcOptions();
         CompletionStage<Void> rsp = rpcManager.invokeCommand(getSegmentDistribution(segment).primary(), cmd, ignoreLeavers(), rpcOptions);
         return Completable.fromCompletionStage(rsp);
      }

      private boolean consistencyCheck() {
         return tombstones.stream().allMatch(tombstoneInfo -> tombstoneInfo.getSegment() == segment);
      }
   }

   private static class CompletionStageObservable extends CompletableFuture<Void> implements CompletableObserver {

      @Override
      public void onSubscribe(@NonNull Disposable d) {
         //no-op
      }

      @Override
      public void onComplete() {
         if (log.isTraceEnabled()) {
            log.trace("[IRAC] Tombstone cleanup round finished!");
         }
         complete(null);
      }

      @Override
      public void onError(@NonNull Throwable e) {
         if (log.isTraceEnabled()) {
            log.trace("[IRAC] Tombstone cleanup round failed!", e);
         }
         complete(null);
      }
   }

   private enum Action {
      KEEP_TOMBSTONE,
      REMOVE_TOMBSTONE,
      CHECK_REMOTE_SITE,
      NOTIFY_PRIMARY_OWNER
   }
}
