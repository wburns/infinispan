package org.infinispan.server.core.transport;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.core.logging.Log;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;

public abstract class BaseBackpressureHandler extends ChannelDuplexHandler {
   protected final Log log = LogFactory.getLog(this.getClass(), Log.class);
   // This can only ever be incremented from the event loop, however it can be decremented from any thread
   protected final AtomicInteger pendingOperations = new AtomicInteger();
   @Override
   public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
      super.channelWritabilityChanged(ctx);
      // When a channel is no longer writeable we shouldn't read anything from it to prevent overwhelming the
      // client, give it some time to consume our output and when it is available again we resume reading
      boolean writeable = ctx.channel().isWritable();
      log.tracef("Channel %s writeability has changed to %s, setting auto read to same", ctx.channel(), writeable);
      ctx.channel().config().setAutoRead(writeable);
   }

   @Override
   public final void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (pendingOperations.incrementAndGet() ==
   }

   abstract boolean actualRead(ChannelHandlerContext ctx, Object msg) throws Exception {

   }
}
