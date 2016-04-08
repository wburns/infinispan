package org.infinispan.server.hotrod;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.CharsetUtil;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.hotrod.logging.JavaLog;

/**
 * // TODO: Document this
 *
 * @author wburns
 * @since 4.0
 */
public class ContextHandler extends SimpleChannelInboundHandler<CacheDecodeContext> {
   private final static JavaLog log = LogFactory.getLog(ContextHandler.class, JavaLog.class);

   @Override
   protected void channelRead0(ChannelHandlerContext ctx, CacheDecodeContext msg) throws Exception {
      switch (msg.header().op()) {
         case PutRequest:
            writeResponse(msg, ctx.channel(), msg.put());
            break;
         case PutIfAbsentRequest:
            break;
         case ReplaceRequest:
            break;
         case ReplaceIfUnmodifiedRequest:
            break;
         case ContainsKeyRequest:
            break;
         case GetRequest:
            break;
         case GetWithVersionRequest:
            break;
         case GetWithMetadataRequest:
            break;
         case RemoveRequest:
            break;
         case RemoveIfUnmodifiedRequest:
            break;
         case PingRequest:
            break;
         case StatsRequest:
            break;
         case ClearRequest:
            break;
         case QuitRequest:
            break;
         case SizeRequest:
            break;
         case AuthMechListRequest:
            break;
         case AuthRequest:
            break;
         case ExecRequest:
            break;
         case BulkGetRequest:
            break;
         case BulkGetKeysRequest:
            break;
         case QueryRequest:
            break;
         case AddClientListenerRequest:
            break;
         case RemoveClientListenerRequest:
            break;
         case IterationStartRequest:
            break;
         case IterationNextRequest:
            break;
         case IterationEndRequest:
            break;
         case PutAllRequest:
            break;
         case GetAllRequest:
            break;
         default:
            throw new IllegalArgumentException("Missing case " + msg.header().op());
      }
   }

   private void writeResponse(CacheDecodeContext ctx, Channel ch, Object response) {
      if (response != null) {
         if (ctx.isTrace()) {
            log.tracef("Write response %s", response);
         }
         if (response instanceof ByteBuf[]) {
            for (ByteBuf buf : (ByteBuf[]) response) {
               ch.write(buf);
            }
            ch.flush();
         } else if (response instanceof byte[]) {
            ch.writeAndFlush(Unpooled.wrappedBuffer((byte[]) response));
         } else if (response instanceof CharSequence) {
            ch.writeAndFlush(Unpooled.copiedBuffer((CharSequence) response, CharsetUtil.UTF_8));
         } else {
            ch.writeAndFlush(response);
         }
      }
   }

   @Override
   public boolean acceptInboundMessage(Object msg) throws Exception {
      return msg instanceof CacheDecodeContext;
   }
}
