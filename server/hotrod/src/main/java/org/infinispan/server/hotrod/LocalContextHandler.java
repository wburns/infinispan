package org.infinispan.server.hotrod;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.infinispan.security.Security;
import org.infinispan.server.core.transport.NettyTransport;

import javax.security.auth.Subject;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.ExecutionException;

import static org.infinispan.server.hotrod.ResponseWriting.writeResponse;

/**
 * Handler that performs actual cache operations.  Note this handler should be on a separate executor group than
 * the decoder.
 *
 * @author wburns
 * @since 9.0
 */
public class LocalContextHandler extends ChannelInboundHandlerAdapter {
   private final ClientListenerRegistry registry;
   private final CommonHandler handler;

   public LocalContextHandler(HotRodServer server, CommonHandler handler) {
      this.registry = server.getClientListenerRegistry();
      this.handler = handler;
   }

   @Override
   public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (msg instanceof CacheDecodeContext) {
         CacheDecodeContext cdc = (CacheDecodeContext) msg;
         Subject subject = ((CacheDecodeContext) msg).getSubject();
         if (subject == null)
            realChannelRead(ctx, cdc);
         else Security.doAs(subject, (PrivilegedExceptionAction<Void>) () -> {
            realChannelRead(ctx, cdc);
            return null;
         });
      } else {
         super.channelRead(ctx, msg);
      }
   }

   private void realChannelRead(ChannelHandlerContext ctx, CacheDecodeContext cdc) throws Exception {
      // If we have no listener we can invoke most operations in this thread without checking
      if (registry.listenerCount() == 0) {
         if (!handler.handle(ctx, cdc)) {
            // If the operation couldn't be handled it might HAVE to be done in another thread
            super.channelRead(ctx, cdc);
         }
      } else {
         HotRodHeader h = cdc.header();
         switch (h.op()) {
            // The following operations are always on this thread as they don't block
            case ContainsKeyRequest:
            case GetRequest:
            case GetWithVersionRequest:
            case GetWithMetadataRequest:
            case PingRequest:
            case StatsRequest:
               if (!handler.handle(ctx, cdc)) {
                  throw handler.operationNotSupported(cdc.header().op());
               }
               break;
            default:
               super.channelRead(ctx, cdc);
         }
      }
   }
}
