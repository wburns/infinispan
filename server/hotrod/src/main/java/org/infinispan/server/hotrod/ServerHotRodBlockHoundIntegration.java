package org.infinispan.server.hotrod;

import org.infinispan.server.core.utils.SslUtils;
import org.kohsuke.MetaInfServices;

import io.netty.util.concurrent.GlobalEventExecutor;
import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

@MetaInfServices
public class ServerHotRodBlockHoundIntegration implements BlockHoundIntegration {
   @Override
   public void applyTo(BlockHound.Builder builder) {
      builder.allowBlockingCallsInside(GlobalEventExecutor.class.getName(), "addTask");
      builder.allowBlockingCallsInside(GlobalEventExecutor.class.getName(), "takeTask");

      questionableBlockingMethod(builder);
   }

   private static void questionableBlockingMethod(BlockHound.Builder builder) {
      // Starting a cache is blocking
      builder.allowBlockingCallsInside(HotRodServer.class.getName(), "obtainAnonymizedCache");
      builder.allowBlockingCallsInside(Encoder2x.class.getName(), "getCounterCacheTopology");

      // Size method is blocking when a store is installed
      builder.allowBlockingCallsInside(CacheRequestProcessor.class.getName(), "stats");

      // Stream method is blocking
      builder.allowBlockingCallsInside(Encoder2x.class.getName(), "generateTopologyResponse");

      // Loads a file on ssl connect to read the key store
      builder.allowBlockingCallsInside(SslUtils.class.getName(), "createNettySslContext");

      // Wildfly open ssl reads a properties file
      builder.allowBlockingCallsInside("org.wildfly.openssl.OpenSSLEngine", "unwrap");
   }
}
