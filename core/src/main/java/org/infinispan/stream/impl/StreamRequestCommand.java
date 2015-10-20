package org.infinispan.stream.impl;

import org.infinispan.commands.CancellableCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.transport.Address;

import java.util.Set;
import java.util.UUID;

/**
 * Stream request command that is sent to remote nodes handle execution of remote intermediate and terminal operations.
 * @param <K> the key type
 */
public class StreamRequestCommand<K> extends BaseRpcCommand implements CancellableCommand, TopologyAffectedCommand {
   public static final byte COMMAND_ID = 47;

   private LocalStreamManager lsm;

   private UUID id;
   private TerminalType type;
   private boolean parallelStream;
   private Set<Integer> segments;
   private Set<K> keys;
   private Set<K> excludedKeys;
   private boolean includeLoader;
   private Object terminalOperation;
   private int topologyId = -1;

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   @Override
   public UUID getUUID() {
      return id;
   }

   public enum TerminalType {
      NORMAL,
      REHASH,
      KEY,
      KEY_REHASH,
      SORTED,
      SORTED_REHASH
   }

   // Only here for CommandIdUniquenessTest
   private StreamRequestCommand() { super(null); }

   public StreamRequestCommand(String cacheName) {
      super(cacheName);
   }

   public StreamRequestCommand(String cacheName, Address origin, UUID id, boolean parallelStream, TerminalType type,
                               Set<Integer> segments, Set<K> keys, Set<K> excludedKeys, boolean includeLoader,
                               Object terminalOperation) {
      super(cacheName);
      setOrigin(origin);
      this.id = id;
      this.parallelStream = parallelStream;
      this.type = type;
      this.segments = segments;
      this.keys = keys;
      this.excludedKeys = excludedKeys;
      this.includeLoader = includeLoader;
      this.terminalOperation = terminalOperation;
   }

   @Inject
   public void inject(LocalStreamManager lsm) {
      this.lsm = lsm;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      switch (type) {
         case NORMAL:
            lsm.streamOperation(id, getOrigin(), parallelStream, segments, keys, excludedKeys, includeLoader,
                    (TerminalOperation) terminalOperation);
            break;
         case REHASH:
            lsm.streamOperationRehashAware(id, getOrigin(), parallelStream, segments, keys, excludedKeys, includeLoader,
                    (TerminalOperation) terminalOperation);
            break;
         case KEY:
            lsm.streamOperation(id, getOrigin(), parallelStream, segments, keys, excludedKeys, includeLoader,
                    (KeyTrackingTerminalOperation) terminalOperation);
            break;
         case KEY_REHASH:
            lsm.streamOperationRehashAware(id, getOrigin(), parallelStream, segments, keys, excludedKeys, includeLoader,
                    (KeyTrackingTerminalOperation) terminalOperation);
            break;
         case SORTED:
            lsm.sortedRehashOperation(id, getOrigin(), segments, keys, excludedKeys, includeLoader,
                    (SortedMapTerminalOperation)terminalOperation);
         case SORTED_REHASH:
         default:
            throw new IllegalArgumentException("Unsupported terminal type: " + type);
      }
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{getOrigin(), id, parallelStream, type, segments, keys, excludedKeys, includeLoader,
              terminalOperation};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      int i = 0;
      setOrigin((Address) parameters[i++]);
      id = (UUID) parameters[i++];
      parallelStream = (Boolean) parameters[i++];
      type = (TerminalType) parameters[i++];
      segments = (Set<Integer>) parameters[i++];
      keys = (Set<K>) parameters[i++];
      excludedKeys = (Set<K>) parameters[i++];
      includeLoader = (Boolean) parameters[i++];
      terminalOperation = parameters[i++];
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public boolean canBlock() {
      return true;
   }
}
