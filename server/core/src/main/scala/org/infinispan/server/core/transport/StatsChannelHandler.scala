package org.infinispan.server.core.transport

import java.net.SocketAddress

import io.netty.buffer.ByteBuf
import io.netty.channel._

/**
 * Input/Output ChannelHandler to keep statistics
 *
 * @author gustavonalle
 * @since 7.1
 */
class StatsChannelHandler(transport: NettyTransport) extends ChannelDuplexHandler {

   override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
      transport.updateTotalBytesRead(msg.asInstanceOf[ByteBuf].readableBytes())
      super.channelRead(ctx, msg)
   }

   override def channelActive(ctx: ChannelHandlerContext) {
      transport.acceptedChannels.add(ctx.channel)
      super.channelActive(ctx)
   }

   override def write(ctx: ChannelHandlerContext, msg: scala.Any, promise: ChannelPromise): Unit = {
      val readable = msg.asInstanceOf[ByteBuf].readableBytes()
      ctx.write(msg, promise.addListener(new ChannelFutureListener {
         def operationComplete(future: ChannelFuture): Unit = {
            if (future.isSuccess) {
               transport.updateTotalBytesWritten(readable)
            }
         }
      }))
   }
}
