package org.infinispan.server.hotrod.test;

import org.infinispan.server.hotrod.ServerAddress;

import java.util.Collection;

/**
 * @author wburns
 * @since 9.0
 */
public abstract class AbstractTestTopologyAwareResponse {public final int topologyId;
   public final Collection<ServerAddress> members;

   protected AbstractTestTopologyAwareResponse(int topologyId, Collection<ServerAddress> members) {
      this.topologyId = topologyId;
      this.members = members;
   }

}
