package org.infinispan.server.hotrod.logging;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.core.Operation;
import org.infinispan.server.core.transport.StatsChannelHandler;
import org.infinispan.server.hotrod.CacheDecodeContext;
import scala.Enumeration;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

/**
 * Logging handler for hotrod to log request messages
 *
 * @author wburns
 * @since 9.0
 */
public class HotRodLoggingHandler extends ChannelInboundHandlerAdapter {
   private static final JavaLog log = LogFactory.getLog(HotRodLoggingHandler.class, JavaLog.class);

   @Override
   public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      CacheDecodeContext cacheDecodeContext = (CacheDecodeContext) msg;

      // IP
      String remoteAddress = ctx.channel().remoteAddress().toString();
      // Date
      Instant startInstant = ctx.channel().attr(StatsChannelHandler.startInstant).get();
      LocalDateTime ldt;
      if (startInstant != null) {
         ldt = LocalDateTime.ofInstant(startInstant, ZoneId.systemDefault());
      }  else {
         ldt = null;
      }
      // Method
      Enumeration.Value op = cacheDecodeContext.header().op();
      // Cache name
      String cacheName = cacheDecodeContext.header().cacheName();
      // Status
      Throwable t = cacheDecodeContext.error();
      String status;
      if (t != null) {
         status = t.getClass().getSimpleName();
      } else {
         status = "OK";
      }
      // Length
      Integer bytesWritten = ctx.channel().attr(StatsChannelHandler.bytesRead).get();
      // Duration
      long ms;
      if (startInstant != null) {
         ms = ChronoUnit.MILLIS.between(startInstant, Instant.now());
      } else {
         ms = -1L;
      }

      log.fatalf("%s [%s] \"%s %s\" %s %s %s ms", remoteAddress, ldt, op, cacheName, status, bytesWritten, ms);
      super.channelRead(ctx, msg);
   }
}
