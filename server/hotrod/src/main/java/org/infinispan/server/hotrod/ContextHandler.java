package org.infinispan.server.hotrod;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.security.Security;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.hotrod.iteration.IterableIterationResult;
import org.infinispan.server.hotrod.logging.JavaLog;
import org.infinispan.server.hotrod.util.BulkUtil;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskManager;
import scala.None$;
import scala.Option;
import scala.Tuple2;
import scala.Tuple4;

import javax.security.auth.Subject;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.BitSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import static org.infinispan.server.hotrod.ResponseWriting.writeResponse;

/**
 * Handler that performs actual cache operations.  Note this handler should be on a separate executor group than
 * the decoder.
 *
 * @author wburns
 * @since 9.0
 */
public class ContextHandler extends SimpleChannelInboundHandler<CacheDecodeContext> {
   private final static JavaLog log = LogFactory.getLog(ContextHandler.class, JavaLog.class);

   private final HotRodServer server;
   private final CommonHandler handler;
   private final Executor executor;

   public ContextHandler(HotRodServer server, CommonHandler handler, Executor executor) {
      this.server = server;
      this.handler = handler;
      this.executor = executor;
   }

   @Override
   protected void channelRead0(ChannelHandlerContext ctx, CacheDecodeContext msg) throws Exception {
      executor.execute(() -> {
         try {
            Subject subject = msg.getSubject();
            if (subject == null)
               realRead(ctx, msg);
            else Security.doAs(subject, (PrivilegedExceptionAction<Void>) () -> {
               realRead(ctx, msg);
               return null;
            });
         } catch (PrivilegedActionException e) {
            ctx.fireExceptionCaught(e.getCause());
         } catch (Exception e) {
            ctx.fireExceptionCaught(e);
         }
      });
   }

   protected void realRead(ChannelHandlerContext ctx, CacheDecodeContext msg) throws Exception {
      if (!handler.handle(ctx, msg)) {
         HotRodHeader h = msg.header();
         switch (h.op()) {
            case AddClientListenerRequest:
               ClientListenerRequestContext clientContext = (ClientListenerRequestContext) msg.operationDecodeContext();
               server.getClientListenerRegistry().addClientListener(msg.decoder(), ctx.channel(), h, clientContext.listenerId(),
                       msg.cache(), clientContext.includeCurrentState(), new Tuple2<>(clientContext.filterFactoryInfo(),
                               clientContext.converterFactoryInfo()), clientContext.useRawData());
               break;
            case RemoveClientListenerRequest:
               byte[] listenerId = (byte[]) msg.operationDecodeContext();
               if (server.getClientListenerRegistry().removeClientListener(listenerId, msg.cache())) {
                  writeResponse(msg, ctx.channel(), msg.decoder().createSuccessResponse(h, null));
               } else {
                  writeResponse(msg, ctx.channel(), msg.decoder().createNotExecutedResponse(h, null));
               }
               break;
            default:
               throw handler.operationNotSupported(msg.header().op());
         }
      }
   }

   @Override
   public void channelActive(ChannelHandlerContext ctx) throws Exception {
      super.channelActive(ctx);
      log.tracef("Channel %s became active", ctx.channel());
      server.getClientListenerRegistry().findAndWriteEvents(ctx.channel());
   }

   @Override
   public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
      super.channelWritabilityChanged(ctx);
      log.tracef("Channel %s writability changed", ctx.channel());
      server.getClientListenerRegistry().findAndWriteEvents(ctx.channel());
   }

   @Override
   public boolean acceptInboundMessage(Object msg) throws Exception {
      // Faster than netty matcher
      return msg instanceof CacheDecodeContext;
   }
}
