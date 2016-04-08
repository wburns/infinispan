package org.infinispan.server.hotrod;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.CharsetUtil;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.marshall.core.JBossMarshaller;
import org.infinispan.server.core.Operation;
import org.infinispan.server.core.security.AuthorizingCallbackHandler;
import org.infinispan.server.core.security.InetAddressPrincipal;
import org.infinispan.server.core.security.ServerAuthenticationProvider;
import org.infinispan.server.core.security.simple.SimpleUserPrincipal;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.core.transport.SaslQopHandler;
import org.infinispan.server.hotrod.configuration.AuthenticationConfiguration;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import org.infinispan.server.hotrod.iteration.IterableIterationResult;
import org.infinispan.server.hotrod.logging.JavaLog;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskManager;
import scala.None;
import scala.None$;
import scala.Option;
import scala.Tuple2;
import scala.Tuple4;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;
import java.net.InetSocketAddress;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

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

   private SaslServer saslServer;
   private AuthorizingCallbackHandler callbackHandler;
   private Subject subject;

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
            if (!config().authentication().enabled()) {
               msg.getDecoder().createErrorResponse(h, log.invalidOperation());
            } else {
               // Retrieve the authorization context
               Tuple2<String, byte[]> opContext = (Tuple2<String, byte[]>) msg.operationDecodeContext();
               if (saslServer == null) {
                  AuthenticationConfiguration authConf = config().authentication();

                  ServerAuthenticationProvider sap = authConf.serverAuthenticationProvider();
                  String mech = opContext._1();
                  callbackHandler = sap.getCallbackHandler(mech, authConf.mechProperties());
                  SaslServerFactory ssf = server.getSaslServerFactory(opContext._1());
                  if (authConf.serverSubject() != null) {
                     Subject.doAs(authConf.serverSubject(), (PrivilegedExceptionAction<SaslServer>) () ->
                                ssf.createSaslServer(mech, "hotrod", authConf.serverName(),
                                        authConf.mechProperties(), callbackHandler));
                  } else {
                     saslServer = ssf.createSaslServer(mech, "hotrod", authConf.serverName(),
                             authConf.mechProperties(), callbackHandler);
                  }
               }
               byte[] serverChallenge = saslServer.evaluateResponse(opContext._2());
               writeResponse(msg, ctx.channel(), new AuthResponse(h.version(), h.messageId(), h.cacheName(),
                       h.clientIntel(), serverChallenge, h.topologyId()));
               if (saslServer.isComplete()) {
                  List<Principal> extraPrincipals = new ArrayList<>();
                  String id = normalizeAuthorizationId(saslServer.getAuthorizationID());
                  extraPrincipals.add(new SimpleUserPrincipal(id));
                  extraPrincipals.add(new InetAddressPrincipal(((InetSocketAddress) ctx.channel().remoteAddress()).getAddress()));
                  SslHandler sslHandler = (SslHandler) ctx.pipeline().get("ssl");
                  try {
                     if (sslHandler != null) extraPrincipals.add(sslHandler.engine().getSession().getPeerPrincipal());
                  } catch (SSLPeerUnverifiedException e) {
                     // Ignore any SSLPeerUnverifiedExceptions
                  }
                  subject = callbackHandler.getSubjectUserInfo(extraPrincipals).getSubject();
                  String qop = (String) saslServer.getNegotiatedProperty(Sasl.QOP);
                  if (qop != null && (qop.equalsIgnoreCase("auth-int") || qop.equalsIgnoreCase("auth-conf"))) {
                     SaslQopHandler qopHandler = new SaslQopHandler(saslServer);
                     ctx.pipeline().addBefore("decoder", "saslQop", qopHandler);
                  } else {
                     saslServer.dispose();
                     callbackHandler = null;
                     saslServer = null;
                  }
               }
            }
            break;
         case ExecRequest:
            ExecRequestContext execContext = (ExecRequestContext)msg.operationDecodeContext();
            TaskManager taskManager = SecurityActions.getCacheGlobalComponentRegistry(msg.cache()).getComponent(TaskManager.class);
            Marshaller marshaller;
            if (server.getMarshaller() != null) {
               marshaller = server.getMarshaller();
            } else {
               marshaller = new GenericJBossMarshaller();
            }
            byte[] result = (byte[]) taskManager.runTask(execContext.name(),
                    new TaskContext().marshaller(marshaller).cache(msg.cache()).parameters(execContext.params())).get();
            writeResponse(msg, ctx.channel(),
                    new ExecResponse(h.version(), h.messageId(), h.cacheName(), h.clientIntel(), h.topologyId(), result));
            break;
         case BulkGetRequest:
            int size = (int) msg.operationDecodeContext();
            if (msg.isTrace()) {
               log.tracef("About to create bulk response count = %d", size);
            }
            writeResponse(msg, ctx.channel(), new BulkGetResponse(h.version(), h.messageId(), h.cacheName(), h.clientIntel(),
                    h.topologyId(), size));
            break;
         case BulkGetKeysRequest:
            int scope = (int) msg.operationDecodeContext();
            if (msg.isTrace()) {
               log.tracef("About to create bulk get keys response scope = %d", scope);
            }
            writeResponse(msg, ctx.channel(), new BulkGetKeysResponse(h.version(), h.messageId(), h.cacheName(), h.clientIntel(),
                    h.topologyId(), scope));
            break;
         case QueryRequest:
            byte[] queryResult = server.query(msg.cache(), (byte[]) msg.operationDecodeContext());
            writeResponse(msg, ctx.channel(),
                    new QueryResponse(h.version(), h.messageId(), h.cacheName(), h.clientIntel(), h.topologyId(), queryResult));
            break;
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
         case IterationStartRequest:
            Tuple4<Option<byte[]>, Option<Tuple2<String, scala.collection.immutable.List<byte[]>>>, Integer, Boolean> iterationStart =
                    (Tuple4<Option<byte[]>, Option<Tuple2<String, scala.collection.immutable.List<byte[]>>>, Integer, Boolean>) msg.operationDecodeContext();

            Option<BitSet> optionBitSet;
            if (iterationStart._1().isDefined()) {
               optionBitSet = Option.apply(BitSet.valueOf(iterationStart._1().get()));
            } else {
               optionBitSet = None$.empty();
            }
            String iterationId = server.iterationManager().start(msg.cache().getName(), optionBitSet,
                    iterationStart._2(), iterationStart._3(), iterationStart._4());
            writeResponse(msg, ctx.channel(), new IterationStartResponse(h.version(), h.messageId(), h.cacheName(),
                    h.clientIntel(), h.topologyId(), iterationId));
            break;
         case IterationNextRequest:
            iterationId = (String) msg.operationDecodeContext();
            IterableIterationResult iterationResult = server.iterationManager().next(msg.cache().getName(), iterationId);
            writeResponse(msg, ctx.channel(), new IterationNextResponse(h.version(), h.messageId(), h.cacheName(),
                    h.clientIntel(), h.topologyId(), iterationResult));
            break;
         case IterationEndRequest:
            iterationId = (String) msg.operationDecodeContext();
            boolean removed = server.iterationManager().close(msg.cache().getName(), iterationId);
               writeResponse(msg, ctx.channel(), msg.decoder().createSuccessResponse(h, null));
            writeResponse(msg, ctx.channel(), new Response(h.version(), h.messageId(), h.cacheName(), h.clientIntel(),
                    OperationResponse.IterationEndResponse(),
                    removed ? OperationStatus.Success() : OperationStatus.InvalidIteration(), h.topologyId()));
            break;
         case PutAllRequest:
            msg.cache().putAll(msg.putAllMap(), msg.buildMetadata());
            writeResponse(msg, ctx.channel(), msg.decoder().createSuccessResponse(h, null));
            break;
         case GetAllRequest:
            Map<byte[], byte[]> map = msg.cache().getAll(msg.getAllSet());
            writeResponse(msg, ctx.channel(), new GetAllResponse(h.version(), h.messageId(), h.cacheName(),
                    h.clientIntel(), h.topologyId(), map));
            break;
         default:
            throw new IllegalArgumentException("Missing case " + msg.header().op());
      }
   }

   String normalizeAuthorizationId(String id) {
      int realm = id.indexOf('@');
      if (realm >= 0) return id.substring(0, realm); else return id;
   }

   private void writeResponse(CacheDecodeContext ctx, Channel ch, Object response) {
      if (response != null) {
         if (ctx.isTrace()) {
            log.tracef("Write response %s", response);
         }
         if (response instanceof Response) {
            ch.writeAndFlush(response);
         } else if (response instanceof ByteBuf[]) {
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
      // Faster than netty matcher
      return msg instanceof CacheDecodeContext;
   }
}
