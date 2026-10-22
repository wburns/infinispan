package org.infinispan.client.hotrod.event.impl;

import java.lang.annotation.Annotation;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCache;
import org.infinispan.client.hotrod.impl.operations.ClientListenerOperation;
import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;

public final class PrimaryNearCacheEventDispatcher extends ClientEventDispatcher {

   PrimaryNearCacheEventDispatcher(ClientListenerOperation op,
                                   Map<Class<? extends Annotation>, List<ClientListenerInvocation>> invocables,
                                   InvalidatedNearRemoteCache<?, ?> inrc) {
      super(op, null, invocables, inrc.getName(), null, inrc);
   }

   public static PrimaryNearCacheEventDispatcher create(ClientListenerOperation op,
                                                        InvalidatedNearRemoteCache<?, ?> inrc) {
      Map<Class<? extends Annotation>, List<ClientListenerInvocation>> invocables = findMethods(op.listener);
      return new PrimaryNearCacheEventDispatcher(op, invocables, inrc);
   }

   private InvalidatedNearRemoteCache<?, ?> inrc() {
      return (InvalidatedNearRemoteCache<?, ?>) remoteCache;
   }

   @Override
   public CompletableFuture<Void> executeFailover(SocketAddress failedAddress) {
      InvalidatedNearRemoteCache<?, ?> inrc = inrc();
      if ((!isRunning() && !isStoppedByFailover()) || !inrc.isPrimaryMode()) {
         return CompletableFuture.completedFuture(null);
      }
      OperationDispatcher dispatcher = inrc.getDispatcher();
      Map<SocketAddress, Set<Integer>> primarySegments = dispatcher.getPrimarySegmentsByAddress(inrc.getName());
      if (primarySegments == null || !primarySegments.containsKey(failedAddress)) {
         return CompletableFuture.completedFuture(null);
      }

      dispatcher.getClientListenerNotifier().addDispatcher(this);
      return inrc.registerPrimaryListenerOnServer(failedAddress)
            .thenCompose(channel -> {
               Map<SocketAddress, Set<Integer>> current = dispatcher.getPrimarySegmentsByAddress(inrc.getName());
               if (current == null || !current.containsKey(failedAddress) || !inrc.isPrimaryMode()) {
                  return inrc.removePrimaryListenerFromServer(failedAddress, listenerId)
                        .thenRun(() -> inrc.handlePrimaryListenerDisconnected(failedAddress));
               }
               return CompletableFuture.completedFuture(null);
            })
            .toCompletableFuture();
   }

   @Override
   protected void invokeFailoverEvent(SocketAddress failedAddress) {
      inrc().handlePrimaryListenerDisconnected(failedAddress);
   }
}
