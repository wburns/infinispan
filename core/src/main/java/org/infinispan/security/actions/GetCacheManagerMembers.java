package org.infinispan.security.actions;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;

import java.util.List;

/**
 * GetCacheManagerMembers.
 *
 * @author William Burns
 * @since 7.0
 */
public class GetCacheManagerMembers extends AbstractEmbeddedCacheManagerAction<List<Address>> {

   public GetCacheManagerMembers(EmbeddedCacheManager cacheManager) {
      super(cacheManager);
   }

   @Override
   public List<Address> run() {
      return cacheManager.getMembers();
   }
}
