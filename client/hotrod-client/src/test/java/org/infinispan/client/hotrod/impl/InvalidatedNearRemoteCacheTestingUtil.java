package org.infinispan.client.hotrod.impl;

import java.net.SocketAddress;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.near.NearCacheService;

import io.netty.channel.Channel;

public final class InvalidatedNearRemoteCacheTestingUtil {

   private InvalidatedNearRemoteCacheTestingUtil() {
   }

   @SuppressWarnings("unchecked")
   public static <K, V> NearCacheService<K, V> getNearCacheService(RemoteCache<K, V> cache) {
      if (cache instanceof InvalidatedNearRemoteCache<K, V> inrc) {
         return inrc.getNearCacheService();
      }
      throw new IllegalArgumentException("Cache is not an InvalidatedNearRemoteCache: " + cache);
   }

   public static boolean hasActiveServerListener(RemoteCache<?, ?> cache, SocketAddress server) {
      if (cache instanceof InvalidatedNearRemoteCache<?, ?> inrc) {
         InvalidatedNearRemoteCache.ServerListenerState state = inrc.getServerListenerState(server);
         return state != null && state.isActive();
      }
      return false;
   }

   public static boolean hasServerListenerState(RemoteCache<?, ?> cache, SocketAddress server) {
      if (cache instanceof InvalidatedNearRemoteCache<?, ?> inrc) {
         return inrc.getServerListenerState(server) != null;
      }
      return false;
   }

   public static Channel getServerListenerChannel(RemoteCache<?, ?> cache, SocketAddress server) {
      if (cache instanceof InvalidatedNearRemoteCache<?, ?> inrc) {
         InvalidatedNearRemoteCache.ServerListenerState state = inrc.getServerListenerState(server);
         return state != null ? state.getChannel() : null;
      }
      return null;
   }

   public static int getServerListenerCount(RemoteCache<?, ?> cache) {
      if (cache instanceof InvalidatedNearRemoteCache<?, ?> inrc) {
         return inrc.getServerListenerStates().size();
      }
      return 0;
   }

   public static long getActiveServerListenerCount(RemoteCache<?, ?> cache) {
      if (cache instanceof InvalidatedNearRemoteCache<?, ?> inrc) {
         return inrc.getServerListenerStates().values().stream()
               .filter(InvalidatedNearRemoteCache.ServerListenerState::isActive)
               .count();
      }
      return 0;
   }

   public static boolean allActiveServerListenersHaveEvenBloomVersion(RemoteCache<?, ?> cache) {
      if (cache instanceof InvalidatedNearRemoteCache<?, ?> inrc) {
         return inrc.getServerListenerStates().values().stream()
               .filter(InvalidatedNearRemoteCache.ServerListenerState::isActive)
               .allMatch(state -> (state.getVersion() & 1) == 0);
      }
      return false;
   }
}
