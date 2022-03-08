package org.infinispan.commands.irac;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ValidSingleResponseCollector;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.infinispan.xsite.irac.IracManager;

/**
 * A {@link XSiteReplicateCommand} to check tombstones for IRAC algorithm.
 * <p>
 * Periodically, the primary owner sends this command to the remote sites where they check if the tombstone for this key
 * is still necessary.
 *
 * @since 14.0
 */
public class IracTombstoneRemoteSiteCheckCommand extends XSiteReplicateCommand<IntSet> {

   public static final byte COMMAND_ID = 38;

   private List<Object> keys;

   @SuppressWarnings("unused")
   public IracTombstoneRemoteSiteCheckCommand() {
      super(COMMAND_ID, null);
   }

   public IracTombstoneRemoteSiteCheckCommand(ByteString cacheName) {
      super(COMMAND_ID, cacheName);
   }

   public IracTombstoneRemoteSiteCheckCommand(ByteString cacheName, List<Object> keys) {
      super(COMMAND_ID, cacheName);
      this.keys = keys;
   }

   @Override
   public ByteString getCacheName() {
      return cacheName;
   }

   @Override
   public CompletionStage<IntSet> invokeAsync(ComponentRegistry registry) {
      IntSet toKeepIndexes = IntSets.mutableEmptySet(keys.size());
      LocalizedCacheTopology topology = registry.getDistributionManager().getCacheTopology();
      IracManager iracManager = registry.getIracManager().running();
      int index = 0;
      for (Object key : keys) {
         // if we are not the primary owner mark the tombstone to keep
         // if we have a pending update to send, mark the tombstone to keep
         if (!topology.getDistribution(key).isPrimary() || iracManager.containsKey(key)) {
            toKeepIndexes.set(index);
         }
         ++index;
      }
      return CompletableFuture.completedFuture(toKeepIndexes);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public CompletionStage<IntSet> performInLocalSite(ComponentRegistry registry, boolean preserveOrder) {
      LocalizedCacheTopology topology = registry.getDistributionManager().getCacheTopology();
      IracManager iracManager = registry.getIracManager().running();
      RpcManager rpcManager = registry.getRpcManager().running();
      RpcOptions rpcOptions = rpcManager.getSyncRpcOptions();

      Map<Address, IntSetResponseCollector> primaryOwnerKeys = new HashMap<>(rpcManager.getMembers().size());
      IntSetHolder holder = new IntSetHolder(keys.size());
      int index = 0;

      for (Object key : keys) {
         DistributionInfo dInfo = topology.getDistribution(key);
         if (dInfo.isPrimary()) {
            if (iracManager.containsKey(key)) {
               holder.set(index);
            }
         } else {
            IntSetResponseCollector collector = primaryOwnerKeys.computeIfAbsent(dInfo.primary(), a -> new IntSetResponseCollector(keys.size(), holder));
            collector.add(index, key);
         }
         ++index;
      }
      if (primaryOwnerKeys.isEmpty()) {
         return CompletableFuture.completedFuture(holder.get());
      }

      AggregateCompletionStage<IntSetHolder> stage = CompletionStages.aggregateCompletionStage(holder);
      for (Map.Entry<Address, IntSetResponseCollector> entry : primaryOwnerKeys.entrySet()) {
         IracTombstoneRemoteSiteCheckCommand cmd = new IracTombstoneRemoteSiteCheckCommand(cacheName, entry.getValue().getKeys());
         stage.dependsOn(rpcManager.invokeCommand(entry.getKey(), cmd, entry.getValue(), rpcOptions));
      }
      return stage.freeze().thenApply(IntSetHolder::get);
   }

   @Override
   public CompletionStage<IntSet> performInLocalSite(BackupReceiver receiver, boolean preserveOrder) {
      throw new IllegalStateException("Should never be invoked!");
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallCollection(keys, output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      keys = MarshallUtil.unmarshallCollection(input, ArrayList::new);
   }

   @Override
   public Address getOrigin() {
      //not needed
      return null;
   }

   @Override
   public void setOrigin(Address origin) {
      //no-op
   }

   @Override
   public String toString() {
      return "IracSiteTombstoneCheckCommand{" +
            "cacheName=" + cacheName +
            ", keys=" + keys.stream().map(Util::toStr).collect(Collectors.joining(",")) +
            '}';
   }

   private static class IntSetResponseCollector extends ValidSingleResponseCollector<Void> {

      private final List<Object> keys;
      private final IntSet indexes;
      private final IntSetHolder holder;

      private IntSetResponseCollector(int maxCapacity, IntSetHolder holder) {
         keys = new ArrayList<>(maxCapacity);
         this.holder = holder;
         indexes = IntSets.mutableEmptySet(maxCapacity);
      }

      void add(int index, Object key) {
         keys.add(key);
         indexes.add(index);
      }

      List<Object> getKeys() {
         return keys;
      }

      @Override
      protected Void withValidResponse(Address sender, ValidResponse response) {
         Object rsp = response.getResponseValue();
         assert rsp instanceof IntSet;
         IntSet toKeep = (IntSet) rsp;
         int toKeepIndex = 0;
         for (int globalIndex : indexes) {
            if (toKeep.contains(toKeepIndex++)) {
               holder.set(globalIndex);
            }
         }
         return null;
      }

      @Override
      protected Void withException(Address sender, Exception exception) {
         markAllToKeep();
         return null;
      }

      @Override
      protected Void targetNotFound(Address sender) {
         markAllToKeep();
         return null;
      }

      private void markAllToKeep() {
         for (int globalIndex : indexes) {
            holder.set(globalIndex);
         }
      }
   }

   private static class IntSetHolder {
      private final IntSet toKeepIndexes;

      private IntSetHolder(int capacity) {
         toKeepIndexes = IntSets.concurrentSet(capacity);
      }

      void set(int index) {
         toKeepIndexes.set(index);
      }

      IntSet get() {
         return toKeepIndexes;
      }
   }
}
