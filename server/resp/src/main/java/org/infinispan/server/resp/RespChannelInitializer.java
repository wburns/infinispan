package org.infinispan.server.resp;

import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.server.core.transport.NettyInitializer;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.FixedLengthFrameDecoder;

/**
 * Creates Netty Channels for the resp server.
 *
 * @author William Burns
 */
public class RespChannelInitializer implements NettyInitializer {

   private final RespServer respServer;

   /**
    * Creates new {@link RespChannelInitializer}.
    *
    * @param respServer Resp Server this initializer belongs to.
    */
   public RespChannelInitializer(RespServer respServer) {
      this.respServer = respServer;
   }

   private AtomicBoolean first = new AtomicBoolean(true);

   @Override
   public void initializeChannel(Channel ch) {
      ChannelPipeline pipeline = ch.pipeline();
      RespRequestHandler initialHandler;
      if (respServer.getConfiguration().authentication().enabled()) {
         initialHandler = new Resp3AuthHandler(respServer);
      } else {
         initialHandler = respServer.newHandler();
      }

      pipeline.addLast(new RespDecoder());
      pipeline.addLast(new RespHandler(initialHandler));
   }
}
