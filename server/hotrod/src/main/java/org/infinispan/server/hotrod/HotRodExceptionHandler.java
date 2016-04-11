package org.infinispan.server.hotrod;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderException;
import io.netty.util.CharsetUtil;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.core.logging.JavaLog;
import scala.Tuple2;

import javax.security.sasl.SaslException;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.security.PrivilegedActionException;

/**
 * // TODO: Document this
 *
 * @author wburns
 * @since 9.0
 */
public class HotRodExceptionHandler extends ChannelInboundHandlerAdapter {
   private final static JavaLog log = LogFactory.getLog(HotRodExceptionHandler.class, JavaLog.class);

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable t) throws Exception {
      Channel ch = ctx.channel();
      NewHotRodDecoder decoder = ctx.pipeline().get(NewHotRodDecoder.class);
      CacheDecodeContext decodeCtx = decoder.decodeCtx;

      log.debug("Exception caught", t);
      if (t instanceof DecoderException) {
         t = t.getCause();
      }
      if (t instanceof HotRodException) {
         // HotRodException is already translated to response
         ch.writeAndFlush(((HotRodException) t).response());
      } else {
         ch.writeAndFlush(decodeCtx.createExceptionResponse(t));
      }
   }
}
