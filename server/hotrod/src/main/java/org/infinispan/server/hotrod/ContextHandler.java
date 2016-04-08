package org.infinispan.server.hotrod;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.CharsetUtil;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import org.infinispan.server.hotrod.logging.JavaLog;

/**
 * // TODO: Document this
 *
 * @author wburns
 * @since 4.0
 */
public class ContextHandler extends SimpleChannelInboundHandler<CacheDecodeContext> {
   private final static JavaLog log = LogFactory.getLog(ContextHandler.class, JavaLog.class);

   private final HotRodServer server;
   private final NettyTransport transport;

   public ContextHandler(HotRodServer server, NettyTransport transport) {
      this.server = server;
      this.transport = transport;
   }

   private HotRodServerConfiguration config() {
      return (HotRodServerConfiguration) server.configuration();
   }

   @Override
   protected void channelRead0(ChannelHandlerContext ctx, CacheDecodeContext msg) throws Exception {
      HotRodHeader h = msg.header();
      switch (h.op()) {
         case PutRequest:
            writeResponse(msg, ctx.channel(), msg.put());
            break;
         case PutIfAbsentRequest:
            writeResponse(msg, ctx.channel(), msg.putIfAbsent());
            break;
         case ReplaceRequest:
            writeResponse(msg, ctx.channel(), msg.replace());
            break;
         case ReplaceIfUnmodifiedRequest:
            writeResponse(msg, ctx.channel(), msg.replaceIfUnmodified());
            break;
         case ContainsKeyRequest:
            writeResponse(msg, ctx.channel(), msg.containsKey());
            break;
         case GetRequest:
            writeResponse(msg, ctx.channel(), msg.get());
            break;
         case GetWithVersionRequest:
            writeResponse(msg, ctx.channel(), msg.get());
            break;
         case GetWithMetadataRequest:
            writeResponse(msg, ctx.channel(), msg.getKeyMetadata());
            break;
         case RemoveRequest:
            writeResponse(msg, ctx.channel(), msg.remove());
            break;
         case RemoveIfUnmodifiedRequest:
            writeResponse(msg, ctx.channel(), msg.removeIfUnmodified());
            break;
         case PingRequest:
            writeResponse(msg, ctx.channel(), new Response(h.version(), h.messageId(), h.cacheName(),
                    h.clientIntel(), OperationResponse.PingResponse(), OperationStatus.Success(), h.topologyId()));
            break;
         case StatsRequest:
            writeResponse(msg, ctx.channel(), msg.decoder().createStatsResponse(msg, transport));
            break;
         case ClearRequest:
            writeResponse(msg, ctx.channel(), msg.clear());
            break;
         case SizeRequest:
            writeResponse(msg, ctx.channel(), new SizeResponse(h.version(), h.messageId(), h.cacheName(),
                    h.clientIntel(), h.topologyId(), msg.cache().size()));
            break;
         case AuthMechListRequest:
            writeResponse(msg, ctx.channel(), new AuthMechListResponse(h.version(), h.messageId(), h.cacheName(),
                    h.clientIntel(), config().authentication().allowedMechs(), h.topologyId()));
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
