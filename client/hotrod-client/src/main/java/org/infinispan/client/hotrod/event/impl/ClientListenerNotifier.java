package org.infinispan.client.hotrod.event.impl;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCache;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.commons.util.Util;

/**
 * @author Galder Zamarreño
 */
// Note: this class was moved to impl package as it was not meant to be public
public class ClientListenerNotifier {
   private static final Log log = LogFactory.getLog(ClientListenerNotifier.class);
   public static final AtomicInteger counter = new AtomicInteger(0);

   // Time for trying to reconnect listeners when all connections are down.
   public static final int RECONNECT_PERIOD = 5000;

   private final ConcurrentMap<WrappedByteArray, List<EventDispatcher<?>>> dispatchers = new ConcurrentHashMap<>();
   private final ScheduledThreadPoolExecutor reconnectExecutor;

   private final Marshaller marshaller;
   private final ClassAllowList allowList;

   public ClientListenerNotifier(Marshaller marshaller, Configuration configuration) {
      this.marshaller = marshaller;
      this.allowList = configuration.getClassAllowList();

      TypedProperties defaultAsyncExecutorProperties = configuration.asyncExecutorFactory().properties();
      ConfigurationProperties cp = new ConfigurationProperties(defaultAsyncExecutorProperties);
      final String threadNamePrefix = cp.getDefaultExecutorFactoryThreadNamePrefix();
      final String threadNameSuffix = cp.getDefaultExecutorFactoryThreadNameSuffix();
      reconnectExecutor = new ScheduledThreadPoolExecutor(1, r -> {
         // Reuse the DefaultAsyncExecutorFactory thread name settings
         Thread th = new Thread(r, threadNamePrefix + "-" + counter.getAndIncrement() + threadNameSuffix);
         th.setDaemon(true);
         return th;
      });
      reconnectExecutor.setKeepAliveTime(2 * RECONNECT_PERIOD, TimeUnit.MILLISECONDS);
      reconnectExecutor.allowCoreThreadTimeOut(true);
   }

   public Marshaller marshaller() {
      return marshaller;
   }

   public void addDispatcher(EventDispatcher<?> dispatcher) {
      List<EventDispatcher<?>> list = dispatchers.computeIfAbsent(new WrappedByteArray(dispatcher.listenerId),
            k -> new CopyOnWriteArrayList<>());
      list.removeIf(d -> InvalidatedNearRemoteCache.sameAddress(d.address(), dispatcher.address()));
      list.add(dispatcher);
      if (log.isTraceEnabled())
         log.tracef("Add dispatcher %s for client listener with id %s, for listener %s",
               dispatcher, Util.printArray(dispatcher.listenerId), dispatcher.listener);
   }

   public void failoverClientListener(byte[] listenerId, SocketAddress address) {
      EventDispatcher<?> dispatcher;
      if (address != null) {
         dispatcher = removeClientListener(listenerId, address);
      } else {
         dispatcher = removeClientListener(listenerId);
      }
      if (dispatcher == null) {
         return;
      }
      // Invoke failover event callback, if presents
      dispatcher.invokeFailoverEvent();
      // Re-execute adding client listener in one of the remaining nodes

      dispatcher.executeFailover().whenComplete((ignore, throwable) -> {
         if (throwable != null) {
            if (throwable instanceof RejectedExecutionException) {
               log.debug("Client listener failover rejected, not retrying", throwable);
            } else {
               log.debug("Unable to failover client listener, so ignore connection reset", throwable);
               ReconnectTask reconnectTask = new ReconnectTask(dispatcher);
               ScheduledFuture<?> scheduledFuture = reconnectExecutor.scheduleAtFixedRate(reconnectTask, RECONNECT_PERIOD, RECONNECT_PERIOD, TimeUnit.MILLISECONDS);
               reconnectTask.setCancellationFuture(scheduledFuture);
            }
         } else {
            if (log.isTraceEnabled()) {
               log.tracef("Fallback listener id %s from a failed server %s",
                     Util.printArray(listenerId), dispatcher.address());
            }
         }
      });
   }

   public void startClientListener(byte[] listenerId) {
      List<EventDispatcher<?>> list = dispatchers.get(new WrappedByteArray(listenerId));
      if (list != null) {
         for (EventDispatcher<?> dispatcher : list) {
            dispatcher.start();
         }
      }
   }

   public EventDispatcher<?> removeClientListener(byte[] listenerId) {
      return removeClientListener(new WrappedByteArray(listenerId));
   }

   public EventDispatcher<?> removeClientListener(byte[] listenerId, SocketAddress address) {
      WrappedByteArray wrapped = new WrappedByteArray(listenerId);
      List<EventDispatcher<?>> list = dispatchers.get(wrapped);
      if (list == null) {
         return null;
      }
      EventDispatcher<?> removed = null;
      for (EventDispatcher<?> d : list) {
         if (address == null || InvalidatedNearRemoteCache.sameAddress(d.address(), address)) {
            list.remove(d);
            d.stop();
            removed = d;
            if (address != null) {
               break;
            }
         }
      }
      if (list.isEmpty()) {
         dispatchers.remove(wrapped, list);
      }
      if (removed != null && log.isTraceEnabled()) {
         log.tracef("Remove client listener with id %s on %s", Util.printArray(listenerId), address);
      }
      return removed;
   }

   private EventDispatcher<?> removeClientListener(WrappedByteArray listenerId) {
      List<EventDispatcher<?>> list = dispatchers.remove(listenerId);
      if (list == null || list.isEmpty()) {
         if (log.isTraceEnabled()) {
            log.tracef("Client listener %s not present (removed concurrently?)", Util.printArray(listenerId.getBytes()));
         }
         return null;
      }
      for (EventDispatcher<?> dispatcher : list) {
         dispatcher.stop();
      }
      if (log.isTraceEnabled())
         log.tracef("Remove client listener with id %s", Util.printArray(listenerId.getBytes()));
      return list.get(0);
   }

   public byte[] findListenerId(Object listener) {
      for (List<EventDispatcher<?>> list : dispatchers.values()) {
         for (EventDispatcher<?> dispatcher : list) {
            if (dispatcher.listener.equals(listener))
               return dispatcher.listenerId;
         }
      }
      return null;
   }

   public boolean isListenerConnected(byte[] listenerId) {
      List<EventDispatcher<?>> list = dispatchers.get(new WrappedByteArray(listenerId));
      if (list == null || list.isEmpty()) {
         return false;
      }
      for (EventDispatcher<?> dispatcher : list) {
         if (dispatcher.isRunning()) {
            return true;
         }
      }
      return false;
   }

   public SocketAddress findAddress(byte[] listenerId) {
      List<EventDispatcher<?>> list = dispatchers.get(new WrappedByteArray(listenerId));
      if (list != null && !list.isEmpty()) {
         return list.get(0).address();
      }
      return null;
   }

   public Set<SocketAddress> findAddresses(byte[] listenerId) {
      List<EventDispatcher<?>> list = dispatchers.get(new WrappedByteArray(listenerId));
      if (list == null || list.isEmpty()) {
         return Collections.emptySet();
      }
      Set<SocketAddress> addresses = new HashSet<>(list.size());
      for (EventDispatcher<?> dispatcher : list) {
         addresses.add(dispatcher.address());
      }
      return addresses;
   }

   public String findCacheName(byte[] listenerId) {
      List<EventDispatcher<?>> list = dispatchers.get(new WrappedByteArray(listenerId));
      return (list != null && !list.isEmpty()) ? list.get(0).cacheName : null;
   }

   public SocketAddress findAddressByListener(Object listener) {
      return findAddress(findListenerId(listener));
   }

   public Set<Object> getListeners(String cacheName) {
      Set<Object> ret = new HashSet<>(dispatchers.size());
      for (List<EventDispatcher<?>> list : dispatchers.values()) {
         for (EventDispatcher<?> dispatcher : list) {
            if (dispatcher.cacheName.equals(cacheName))
               ret.add(dispatcher.listener);
         }
      }

      return ret;
   }

   public void stop() {
      for (WrappedByteArray listenerId : dispatchers.keySet()) {
         if (log.isTraceEnabled())
            log.tracef("Remote cache manager stopping, remove client listener id %s", Util.printArray(listenerId.getBytes()));

         removeClientListener(listenerId);
      }
      reconnectExecutor.shutdownNow();
   }

   public <T> void invokeEvent(byte[] listenerId, T event) {
      List<EventDispatcher<?>> list = dispatchers.get(new WrappedByteArray(listenerId));
      if (list == null || list.isEmpty()) {
         throw HOTROD.unexpectedListenerId(Util.printArray(listenerId));
      }
      EventDispatcher<T> eventDispatcher = (EventDispatcher<T>) list.get(0);
      eventDispatcher.invokeEvent(event);
   }

   public DataFormat getCacheDataFormat(byte[] listenerId) {
      List<EventDispatcher<?>> list = dispatchers.get(new WrappedByteArray(listenerId));
      if (list == null || list.isEmpty()) {
         throw HOTROD.unexpectedListenerId(Util.printArray(listenerId));
      }
      ClientEventDispatcher clientEventDispatcher = (ClientEventDispatcher) list.get(0);
      return clientEventDispatcher.getDataFormat();
   }

   public ClassAllowList allowList() {
      return allowList;
   }

}
