package org.infinispan.commands.remote.recovery;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseTopologyRpcCommand;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Command for removing recovery related information from the cluster.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class TxCompletionNotificationCommand extends BaseTopologyRpcCommand implements TopologyAffectedCommand {
   private static final Log log = LogFactory.getLog(TxCompletionNotificationCommand.class);

   public static final int COMMAND_ID = 22;

   private XidImpl xid;
   private long internalId;
   private GlobalTransaction gtx;

   @SuppressWarnings("unused")
   private TxCompletionNotificationCommand() {
      this(null); // For command id uniqueness test
   }

   public TxCompletionNotificationCommand(XidImpl xid, GlobalTransaction gtx, ByteString cacheName) {
      this(cacheName);
      this.xid = xid;
      this.gtx = gtx;
   }

   public TxCompletionNotificationCommand(long internalId, ByteString cacheName) {
      this(cacheName);
      this.internalId = internalId;
   }

   public TxCompletionNotificationCommand(ByteString cacheName) {
      super(cacheName, EnumUtil.EMPTY_BIT_SET);
   }

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) throws Throwable {
      if (log.isTraceEnabled())
         log.tracef("Processing completed transaction %s", gtx);
      RemoteTransaction remoteTx = null;
      RecoveryManager recoveryManager = componentRegistry.getRecoveryManager().running();
      if (recoveryManager != null) { //recovery in use
         if (xid != null) {
            remoteTx = (RemoteTransaction) recoveryManager.removeRecoveryInformation(xid);
         } else {
            remoteTx = (RemoteTransaction) recoveryManager.removeRecoveryInformation(internalId);
         }
      }
      if (remoteTx == null && gtx != null) {
         TransactionTable txTable = componentRegistry.getTransactionTableRef().running();
         remoteTx = txTable.removeRemoteTransaction(gtx);
      }
      if (remoteTx == null) return CompletableFutures.completedNull();
      forwardCommandRemotely(remoteTx, componentRegistry);

      LockManager lockManager = componentRegistry.getLockManager().running();
      lockManager.unlockAll(remoteTx.getLockedKeys(), remoteTx.getGlobalTransaction());
      return CompletableFutures.completedNull();
   }

   public GlobalTransaction getGlobalTransaction() {
      return gtx;
   }

   /**
    * This only happens during state transfer.
    */
   private void forwardCommandRemotely(RemoteTransaction remoteTx, ComponentRegistry registry) {
      DistributionManager distributionManager = registry.getDistributionManager();
      RpcManager rpcManager = registry.getRpcManager().running();
      Set<Object> affectedKeys = remoteTx.getAffectedKeys();
      if (log.isTraceEnabled())
         log.tracef("Invoking forward of TxCompletionNotification for transaction %s. Affected keys: %s", gtx,
               toStr(affectedKeys));
      LocalizedCacheTopology cacheTopology = distributionManager.getCacheTopology();
      if (cacheTopology == null) {
         if (log.isTraceEnabled()) {
            log.tracef("Not Forwarding command %s because topology is null.", this);
         }
         return;
      }
      // forward commands with older topology ids to their new targets
      // but we need to make sure we have the latest topology
      int localTopologyId = cacheTopology.getTopologyId();
      // if it's a tx/lock/write command, forward it to the new owners
      if (log.isTraceEnabled()) {
         log.tracef("CommandTopologyId=%s, localTopologyId=%s", topologyId, localTopologyId);
      }

      if (topologyId >= localTopologyId) {
         return;
      }

      Collection<Address> newTargets = new HashSet<>(cacheTopology.getWriteOwners(affectedKeys));
      newTargets.remove(rpcManager.getAddress());
      // Forwarding to the originator would create a cycle
      // TODO This may not be the "real" originator, but one of the original recipients
      // or even one of the nodes that one of the original recipients forwarded the command to.
      // In non-transactional caches, the "real" originator keeps a lock for the duration
      // of the RPC, so this means we could get a deadlock while forwarding to it.
      newTargets.remove(origin);
      if (!newTargets.isEmpty()) {
         // Update the topology id to prevent cycles
         topologyId = localTopologyId;
         if (log.isTraceEnabled()) {
            log.tracef("Forwarding command %s to new targets %s", this, newTargets);
         }
         // TxCompletionNotificationCommands are the only commands being forwarded now,
         // and they must be OOB + asynchronous
         rpcManager.sendToMany(newTargets, this, DeliverOrder.NONE);
      }
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      if (xid == null) {
         output.writeBoolean(true);
         output.writeLong(internalId);
      } else {
         output.writeBoolean(false);
         XidImpl.writeTo(output, xid);
      }
      output.writeObject(gtx);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      if (input.readBoolean()) {
         internalId = input.readLong();
      } else {
         xid = XidImpl.readFrom(input);
      }
      gtx = (GlobalTransaction) input.readObject();
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() +
            "{ xid=" + xid +
            ", internalId=" + internalId +
            ", topologyId=" + topologyId +
            ", gtx=" + gtx +
            ", cacheName=" + cacheName + "} ";
   }
}
