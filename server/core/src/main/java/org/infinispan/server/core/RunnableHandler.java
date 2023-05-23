package org.infinispan.server.core;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

@ChannelHandler.Sharable
public class RunnableHandler extends ChannelInboundHandlerAdapter {
   private RunnableHandler() {

   }
   public static RunnableHandler INSTANCE = new RunnableHandler();
   @Override
   public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (!(msg instanceof Runnable)) {
         throw new IllegalArgumentException("RunnableHandler only supports Runnable instances, but received: " + msg);
      }
      ((Runnable) msg).run();
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      super.exceptionCaught(ctx, cause);
   }
}
