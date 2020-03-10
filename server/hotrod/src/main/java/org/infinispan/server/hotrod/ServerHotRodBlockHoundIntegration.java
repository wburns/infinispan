package org.infinispan.server.hotrod;

import org.kohsuke.MetaInfServices;

import io.netty.util.concurrent.GlobalEventExecutor;
import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

@MetaInfServices
public class ServerHotRodBlockHoundIntegration implements BlockHoundIntegration {
   @Override
   public void applyTo(BlockHound.Builder builder) {
      builder.allowBlockingCallsInside(GlobalEventExecutor.class.getName(), "takeTask");
   }
}
