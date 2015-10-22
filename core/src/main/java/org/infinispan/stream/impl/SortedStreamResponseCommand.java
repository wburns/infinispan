package org.infinispan.stream.impl;

import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.Address;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

/**
 * A stream response command that also includes highest object in sorted values.  This is required for operations that
 * may have performed another map operation after the sort.
 * @param <R> the response type
 * @param <E> the sorted type
 */
public class SortedStreamResponseCommand<R, E> extends StreamResponseCommand<Iterable<R>> {
   public static final byte COMMAND_ID = 59;

   protected E lastSeen;

   // Only here for CommandIdUniquenessTest
   protected SortedStreamResponseCommand() { }

   public SortedStreamResponseCommand(String cacheName) {
      super(cacheName);
   }

   public SortedStreamResponseCommand(String cacheName, Address origin, UUID id, boolean complete,
           Iterable<R> response, E lastSeen) {
      super(cacheName, origin, id, complete, response);
      this.lastSeen = lastSeen;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      // Expects a Map.Entry<Iterable<R>, E>
      csm.receiveResponse(id, getOrigin(), complete, Collections.emptySet(), new ImmortalCacheEntry(response, lastSeen));
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{getOrigin(), id, complete, response, lastSeen};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      int i = 0;
      setOrigin((Address) parameters[i++]);
      id = (UUID) parameters[i++];
      complete = (Boolean) parameters[i++];
      response = (Iterable<R>) parameters[i++];
      lastSeen = (E) parameters[i++];
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
