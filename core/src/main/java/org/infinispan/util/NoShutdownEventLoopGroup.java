package org.infinispan.util;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.util.DelegatingEventLoopGroup;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.SucceededFuture;

public class NoShutdownEventLoopGroup extends DelegatingEventLoopGroup {
   private final EventLoopGroup eventLoopGroup;

   public NoShutdownEventLoopGroup(EventLoopGroup eventLoopGroup) {
      this.eventLoopGroup = eventLoopGroup;
   }

   @Override
   protected EventLoopGroup delegate() {
      return eventLoopGroup;
   }

   @Override
   public Future<?> shutdownGracefully() {
      // Prevent shut down as this is a suite resource
      return new SucceededFuture<>(null, null);
   }

   @Override
   public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
      return shutdownGracefully();
   }

   @Override
   public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      // This is inconsistent with other termination methods (e.g. isTerminated | isShutdown), but should be okay
      return true;
   }

   @Override
   public List<Runnable> shutdownNow() {
      return Collections.emptyList();
   }

   @Override
   public void shutdown() {
   }
}
