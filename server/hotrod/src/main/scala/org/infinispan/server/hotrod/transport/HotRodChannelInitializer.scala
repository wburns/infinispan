package org.infinispan.server.hotrod.transport

import io.netty.channel.{Channel, ChannelInitializer}
import io.netty.channel.ChannelOutboundHandler
import io.netty.util.concurrent.DefaultEventExecutorGroup
import org.infinispan.server.core.ProtocolServer
import org.infinispan.server.core.transport.{NettyChannelInitializer, NettyTransport}
import org.infinispan.server.hotrod.ContextHandler
import org.infinispan.server.hotrod.logging.HotRodLoggingHandler

/**
  * HotRod specific channel initializer
  *
  * @author wburns
  * @since 9.0
  */
class HotRodChannelInitializer(val server: ProtocolServer, transport: => NettyTransport,
      val encoder: ChannelOutboundHandler) extends NettyChannelInitializer(server, transport, encoder) {

   override def initChannel(ch: Channel): Unit = {
      super.initChannel(ch)
      ch.pipeline.addLast(new DefaultEventExecutorGroup(transport.configuration.workerThreads), "handler", new ContextHandler)
      ch.pipeline.addLast("logging", new HotRodLoggingHandler)
   }
}