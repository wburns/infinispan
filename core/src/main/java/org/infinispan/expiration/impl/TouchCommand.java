package org.infinispan.expiration.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.InitializableCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.time.TimeService;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * This command can be invoked to update a cache entry's recent access. This can involve updating its last access
 * with max idle as well as recent eviction access times.
 * <p>
 * This command returns a Boolean that is whether this command was able to touch the value or not.
 */
public class TouchCommand extends BaseRpcCommand implements InitializableCommand, TopologyAffectedCommand {
   public static final byte COMMAND_ID = 66;

   private Object key;
   private int segment;
   private int topologyId = -1;

   private InternalDataContainer internalDataContainer;
   private TimeService timeService;
   private DistributionManager distributionManager;

   // This is not replicated - here solely to avoid additional time lookups in callers
   private long accessTime = -1;

   // Only here for CommandIdUniquenessTest
   private TouchCommand() { super(null); }

   public TouchCommand(ByteString cacheName) {
      super(cacheName);
   }

   public TouchCommand(ByteString cacheName, Object key, int segment) {
      super(cacheName);
      this.key = key;
      this.segment = segment;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      UnsignedNumeric.writeUnsignedInt(output, segment);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      segment = UnsignedNumeric.readUnsignedInt(input);
   }

   @Override
   public void init(ComponentRegistry componentRegistry, boolean isRemote) {
      internalDataContainer = componentRegistry.getInternalDataContainer().running();
      timeService = componentRegistry.getTimeService();
      // Invalidation cache doesn't set topology id - so we don't want to throw OTE in invokeAsync
      if (!componentRegistry.getConfiguration().clustering().cacheMode().isInvalidation()) {
         distributionManager = componentRegistry.getDistributionManager();
      }
   }

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   public void setAccessTime(long currentTimeMillis) {
      this.accessTime = currentTimeMillis;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() {
      long accessTime = this.accessTime == -1 ? timeService.wallClockTime() : this.accessTime;
      boolean touched = internalDataContainer.touch(segment, key, accessTime);
      // Hibernate currently disables clustered expiration manager, which means we can have a topology id of -1
      // when using a clustered cache mode
      if (distributionManager != null && topologyId != -1) {
         LocalizedCacheTopology lct = distributionManager.getCacheTopology();
         int currentTopologyId = lct.getTopologyId();
         if (currentTopologyId != topologyId) {
            return CompletableFutures.completedExceptionFuture(OutdatedTopologyException.RETRY_NEXT_TOPOLOGY);
         }
         DistributionInfo di = lct.getSegmentDistribution(segment);
         // If our node is a write owner but not read owner, that means we may not have the value yet - so we just
         // say we were touched anyways
         // TODO: is this is an issue with concurrent state response and not touching the new value?
         if (di.isWriteOwner() && !di.isReadOwner()) {
            return CompletableFuture.completedFuture(Boolean.TRUE);
         }
      }
      return CompletableFuture.completedFuture(touched);
   }
}
