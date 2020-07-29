package org.infinispan.interceptors.xsite;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.remoting.rpc.RpcManager;

/**
 * Handles x-site data backups for optimistic transactional caches.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class OptimisticBackupInterceptor extends BaseBackupInterceptor {

   @Inject
   protected RpcManager rpcManager;

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      //if this is a read-only tx or state transfer tx, no point replicating it to other sites
      if (!shouldInvokeRemoteTxCommand(ctx) || isTxFromRemoteSite(command.getGlobalTransaction())) {
         return invokeNext(ctx, command);
      }

      InvocationStage stage = backupSender.backupPrepare(command, ctx.getCacheTransaction(), ctx.getTransaction());
      return invokeNextAndWaitForCrossSite(ctx, command, stage);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (isTxFromRemoteSite(command.getGlobalTransaction())) {
         return invokeNext(ctx, command);
      }

      InvocationStage stage;
      if (shouldInvokeRemoteTxCommand(ctx)) { // this assumes sync.
         // for sync, the originator is the one who backups the transaction to the remote site.
         // we can send the update to the remote site in parallel with the local cluster update.
         stage = backupSender.backupCommit(command, ctx.getTransaction());
      } else {
         //for async, all nodes need to keep track of the updates keys.
         stage = InvocationStage.completedNullStage();
      }

      return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
         //we need to track the keys only after it is applied in the local node!
         iracManager.trackKeysFromTransaction(getModificationsFrom(rCommand), rCommand.getGlobalTransaction());
         return stage.thenReturn(rCtx, rCommand, rv);
      });
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (!shouldRollbackRemoteTxCommand(ctx))
         return invokeNext(ctx, command);

      if (isTxFromRemoteSite(command.getGlobalTransaction())) {
         return invokeNext(ctx, command);
      }

      InvocationStage stage = backupSender.backupRollback(command, ctx.getTransaction());
      return invokeNextAndWaitForCrossSite(ctx, command, stage);
   }

   @Override
   protected Object visitBackupRemoveExpired(DistributionInfo info, InvocationContext ctx, RemoveExpiredCommand command) {
      return checkRemoteSiteIfMaxIdleExpired(ctx, command);
   }

   private boolean shouldRollbackRemoteTxCommand(TxInvocationContext<?> ctx) {
      return shouldInvokeRemoteTxCommand(ctx) && hasBeenPrepared((LocalTxInvocationContext) ctx);
   }

   /**
    * This 'has been prepared' logic only applies to optimistic transactions, hence it is not present in the
    * LocalTransaction object itself.
    */
   private boolean hasBeenPrepared(LocalTxInvocationContext ctx) {
      return !ctx.getRemoteLocksAcquired().isEmpty();
   }
}
