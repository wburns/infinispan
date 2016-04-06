package org.infinispan.server.hotrod.transport

import io.netty.channel.{Channel, ChannelInitializer}
import io.netty.channel.ChannelOutboundHandler
import org.infinispan.server.core.ProtocolServer
import org.infinispan.server.core.transport.{NettyChannelInitializer, NettyTransport}
import org.infinispan.server.hotrod.logging.HotRodLoggingHandler

/**
  * HotRod specific channel initializer
  *
  * @author wburns
  * @since 9.0
  */
class HotRodChannelInitializer(val server: ProtocolServer, val transport: NettyTransport,
      val encoder: ChannelOutboundHandler) extends ChannelInitializer[Channel] with NettyChannelInitializer {

   override def initChannel(ch: Channel): Unit = {
      super.initChannel(ch)
      ch.pipeline.addLast("logging", new HotRodLoggingHandler)
   }
}