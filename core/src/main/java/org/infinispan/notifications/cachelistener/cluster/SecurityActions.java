package org.infinispan.notifications.cachelistener.cluster;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetCacheComponentRegistryAction;
import org.infinispan.security.actions.GetCacheGlobalComponentRegistryAction;
import org.infinispan.security.actions.GetCacheManagerAddress;
import org.infinispan.security.actions.GetCacheManagerMembers;
import org.infinispan.security.actions.GetDefaultExecutorServiceAction;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;

/**
 * SecurityActions for the org.infinispan.notifications.cachelistener.cluster package.
 *
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author William Burns
 * @since 7.0
 */
final class SecurityActions {
   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static ComponentRegistry getCacheComponentRegistry(final AdvancedCache<?, ?> cache) {
      GetCacheComponentRegistryAction action = new GetCacheComponentRegistryAction(cache);
      return doPrivileged(action);
   }

   static GlobalComponentRegistry getCacheGlobalComponentRegistry(final AdvancedCache<?, ?> cache) {
      GetCacheGlobalComponentRegistryAction action = new GetCacheGlobalComponentRegistryAction(cache);
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static DefaultExecutorService getDefaultExecutorService(final Cache<?, ?> cache) {
      GetDefaultExecutorServiceAction action = new GetDefaultExecutorServiceAction(cache);
      return doPrivileged(action);
   }

   static Address getCacheManagerLocalAddress(EmbeddedCacheManager cacheManager) {
      GetCacheManagerAddress action = new GetCacheManagerAddress(cacheManager);
      return doPrivileged(action);
   }

   static List<Address> getCacheManagerMembers(EmbeddedCacheManager cacheManager) {
      GetCacheManagerMembers action = new GetCacheManagerMembers(cacheManager);
      return doPrivileged(action);
   }
}
