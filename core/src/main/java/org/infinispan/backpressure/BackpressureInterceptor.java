package org.infinispan.backpressure;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.commands.DataCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.IracPutKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.expiration.impl.TouchCommand;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.CacheTopologyUtil;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class BackpressureInterceptor extends DDAsyncInterceptor implements BackpressureNotifier.BackpressureListener {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private final ConcurrentMap<Address, CompletableFuture<Void>> pressuredAddresses = new ConcurrentHashMap<>();

   @Inject protected DistributionManager distributionManager;
   @Inject protected BlockingManager blockingManager;
   @Inject protected BackpressureNotifier notifier;

   @Start
   public void start() {
      notifier.registerListener(this);
   }

   @Stop
   public void stop() {
      notifier.unregisterListener(this);
   }

   @Override
   public void memberPressured(Address address) {
      pressuredAddresses.put(address, new CompletableFuture<>());
   }

   @Override
   public void memberRelieved(Address address) {
      CompletableFuture<Void> future = pressuredAddresses.remove(address);
      if (future != null) {
         // Note all registered futures are already resuming on different non blocking threads so we just call
         // complete to start them all off
         future.complete(null);
      } else {
         log.warnf("Possible bug when relieving back pressure for address %s", address);
      }
   }

   protected Object handleDataCommand(InvocationContext ctx, DataCommand command) {
      return handleDataCommand(ctx, command, false);
   }

   protected Object handleDataCommand(InvocationContext ctx, DataCommand command, boolean skipIfOwner) {
      if (!ctx.isOriginLocal() || command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL) ||
            pressuredAddresses.isEmpty()) {
         return invokeNext(ctx, command);
      }

      LocalizedCacheTopology cacheTopology = CacheTopologyUtil.checkTopology(command, distributionManager.getCacheTopology());
      if (skipIfOwner && cacheTopology.isSegmentReadOwner(command.getSegment())) {
         log.tracef("Command %s is a read of a locally owned key so ignoring backpressure", command);
         return invokeNext(ctx, command);
      }
      Collection<Address> owners = cacheTopology.getWriteOwners(command.getSegment());
      for (Address owner : owners) {
         CompletionStage<Void> stage = pressuredAddresses.get(owner);
         if (stage != null) {
            log.tracef("Delaying execution of command %s as an owner %s is currently pressured", command, owner);
            return asyncInvokeNext(ctx, command, blockingManager.continueOnNonBlockingThread(stage, "BackPressureInterceptor"));
         }
      }
      log.tracef("A node is pressured, but none of our owners for command %s so invoking directly", command);
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command)
         throws Throwable {
      return handleDataCommand(ctx, command, true);
   }

   @Override
   public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command)
         throws Throwable {
      return handleDataCommand(ctx, command, true);
   }
   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
         throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      // TODO: need to always disable
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      // TODO: need to check all keys
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      // TODO: need to check all keys
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      // TODO: Local only afaik
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitSizeCommand(InvocationContext ctx, SizeCommand command) throws Throwable {
      // TODO: need to always disable
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitIracPutKeyValueCommand(InvocationContext ctx, IracPutKeyValueCommand command) throws Throwable {
      // TODO: unknown, need to look at it
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command)
         throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command command)
         throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command)
         throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitUnknownCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command)
         throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command)
         throws Throwable {
      // TODO: need to check all keys
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command)
         throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command)
         throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command)
         throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx,
                                                  WriteOnlyManyEntriesCommand command) throws Throwable {
      // TODO: need to see all keys
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command)
         throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command)
         throws Throwable {
      // TODO: need to see all keys
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command)
         throws Throwable {
      // TODO: need to see all keys
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx,
                                                  ReadWriteManyEntriesCommand command) throws Throwable {
      // TODO: need to see all keys
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitTouchCommand(InvocationContext ctx, TouchCommand command) throws Throwable {
      return handleDataCommand(ctx, command);
   }
}
