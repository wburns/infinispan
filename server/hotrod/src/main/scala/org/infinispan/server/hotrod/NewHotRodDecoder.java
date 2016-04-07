package org.infinispan.server.hotrod;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;
import org.infinispan.server.core.NewServerConstants;
import org.infinispan.server.core.Operation;
import org.infinispan.server.core.logging.JavaLog;
import org.infinispan.server.core.transport.ExtendedByteBuf;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.core.transport.VLong;
import org.infinispan.server.hotrod.configuration.AuthenticationConfiguration;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import scala.Enumeration;
import scala.Option;
import scala.Tuple2;

import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

/**
 * // TODO: Document this
 *
 * @author wburns
 * @since 4.0
 */
public class NewHotRodDecoder extends ReplayingDecoder<HotRodDecoderState> {
   private final static JavaLog log = LogFactory.getLog(NewHotRodDecoder.class, JavaLog.class);

   private final EmbeddedCacheManager cacheManager;
   private final NettyTransport transport;
   private final HotRodServer server;
   private final Predicate<? super String> ignoreCache;

   private final AuthenticationConfiguration authenticationConfig;
   private final boolean secure;
   private final boolean requireAuthentication;

   private final CacheDecodeContext decodeCtx;

   private Subject subject = NewServerConstants.ANONYMOUS;


   private volatile boolean resetRequested;

   public NewHotRodDecoder(EmbeddedCacheManager cacheManager, NettyTransport transport, HotRodServer server,
           Predicate<? super String> ignoreCache) {
      this.cacheManager = cacheManager;
      this.transport = transport;
      this.server = server;
      this.ignoreCache = ignoreCache;

      authenticationConfig = ((HotRodServerConfiguration) server.getConfiguration()).authentication();
      secure = authenticationConfig.enabled();
      requireAuthentication = secure && authenticationConfig.mechProperties().containsKey(Sasl.POLICY_NOANONYMOUS)
              && authenticationConfig.mechProperties().get(Sasl.POLICY_NOANONYMOUS).equals("true");

      this.decodeCtx = new CacheDecodeContext(server);
   }

   void resetNow() {
      decodeCtx.resetParams();
      decodeCtx.setHeader(new HotRodHeader());
      super.state(HotRodDecoderState.DECODE_HEADER);
   }

   @Override
   protected HotRodDecoderState state(HotRodDecoderState newState) {
      throw new UnsupportedOperationException("Call state(HotRodDecoderState, ByteBuf)");
   }

   /**
    * Should be called instead of {@link #state(HotRodDecoderState, ByteBuf)}.  This also marks the buffer read
    * position so when we can't read bytes it will be reset to here.
    * @param newState
    * @param buf
    * @return
    */
   protected HotRodDecoderState state(HotRodDecoderState newState, ByteBuf buf) {
      buf.markReaderIndex();
      return super.state(newState);
   }

   @Override
   protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
      try {
         if (decodeCtx.isTrace()) {
            log.tracef("Decode using instance @%x", System.identityHashCode(this));
         }

         if (resetRequested) {
            resetNow();
         }

         Callable<Void> callable = () -> {
            switch (state()) {
               // These are terminal cases, meaning they only perform this operation and break
               case DECODE_HEADER_CUSTOM:
                  readCustomHeader(in, out);
                  break;
               case DECODE_KEY_CUSTOM:
                  readCustomKey(in, out);
                  break;
               // These are all fall through cases which means they call to the one below if they needed additional
               // processing
               case DECODE_HEADER:
                  if (!decodeHeader(in, out)) {
                     return null;
                  }
                  state(HotRodDecoderState.DECODE_KEY, in);
               case DECODE_KEY:
                  if (!decodeKey(ctx, in, out)) {
                     return null;
                  }
                  state(HotRodDecoderState.DECODE_PARAMETERS, in);
               case DECODE_PARAMETERS:
                  if (!decodeParameters(ctx, in, out)) {
                     return null;
                  }
                  state(HotRodDecoderState.DECODE_VALUE, in);
               case DECODE_VALUE:
                  if (!decodeValue(ctx, in, out)) {
                     return null;
                  }
                  state(HotRodDecoderState.DECODE_HEADER, in);
            }
            return null;
         };
         if (secure) {
            Security.doAs(subject, (PrivilegedExceptionAction<Object>) callable::call);
         } else {
            callable.call();
         }

      } catch (PrivilegedActionException | SecurityException e) {
         Tuple2<HotRodException, Object> tuple = decodeCtx.createServerException(e, in);
         ctx.pipeline().fireExceptionCaught(tuple._1()).close();
      } catch (Exception e) {
         Tuple2<HotRodException, Object> tuple = decodeCtx.createServerException(e, in);
         HotRodException serverException = tuple._1();
         if (tuple._2() == Boolean.TRUE) {
            ctx.pipeline().fireExceptionCaught(serverException);
         } else {
            throw serverException;
         }
      }
   }

   boolean decodeHeader(ByteBuf in, List<Object> out) throws Exception {
      boolean shouldContinue = readHeader(in);
      // If there was nothing present it means we throw this decoding away and start fresh
      if (!shouldContinue) {
         resetRequested = true;
         return false;
      }
      HotRodHeader header = decodeCtx.getHeader();
      // Check if this cache can be accessed or not
      if (ignoreCache.test(header.cacheName())) {
         CacheUnavailableException excp = new CacheUnavailableException();
         decodeCtx.setError(excp);
         throw excp;
      }
      NewHotRodOperation op = header.op();
      switch (op.getDecoderRequirements()) {
         case HEADER_CUSTOM:
            state(HotRodDecoderState.DECODE_HEADER_CUSTOM, in);
            readCustomHeader(in, out);
            return false;
         case HEADER:
            // If all we needed was header, we have everything already!
            out.add(decodeCtx);
            resetRequested = true;
            return false;
         default:
            return true;
      }
   }

   private void readCustomHeader(ByteBuf in, List<Object> out) {
      decodeCtx.decoder().customReadHeader(decodeCtx.header(), in, decodeCtx, out);
      // If out was written to, it means we read everything, else we have to reread again
      if (!out.isEmpty()) {
         resetRequested = true;
      }
   }

   /**
    * Reads the header and returns whether we should try to continue
    * @param buffer
    * @return
    * @throws Exception
    */
   Boolean readHeader(ByteBuf buffer) throws Exception {
      short magic = buffer.readUnsignedByte();
      if (magic != Constants$.MODULE$.MAGIC_REQ()) {
         if (decodeCtx.getError() == null) {
            throw new InvalidMagicIdException("Error reading magic byte or message id: " + magic);
         } else {
            log.tracef("Error happened previously, ignoring %d byte until we find the magic number again", magic);
            return false;
         }
      }

      long messageId = VLong.read(buffer);
      byte version = (byte) buffer.readUnsignedByte();

      try {
         AbstractVersionedDecoder decoder;
         if (Constants$.MODULE$.isVersion2x(version)) {
            decoder = Decoder2x$.MODULE$;
         } else if (Constants$.MODULE$.isVersion1x(version)) {
            decoder = Decoder10$.MODULE$;
         } else {
            throw new UnknownVersionException("Unknown version:" + version, version, messageId);
         }
         HotRodHeader header = decodeCtx.getHeader();
         decoder.readHeader(buffer, version, messageId, header, requireAuthentication
                 && subject == NewServerConstants.ANONYMOUS);
         decodeCtx.setDecoder(decoder);
         if (decodeCtx.isTrace()) {
            log.tracef("Decoded header %s", header);
         }
         return true;
      } catch (HotRodUnknownOperationException | SecurityException e) {
         decodeCtx.setError(e);
         throw e;
      } catch (Exception e) {
         decodeCtx.setError(e);
         throw new RequestParsingException("Unable to parse header", version, messageId, e);
      }
   }

   boolean decodeKey(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
      NewHotRodOperation op = decodeCtx.getHeader().op();
      // If we want a single key read that - else we do custom read
      if (op.requireSingleKey()) {
         Option<byte[]> bytes = ExtendedByteBuf.readMaybeRangedBytes(in);
         if (bytes.exists(a -> {
            decodeCtx.key_$eq(a);
            return true;
         }));
      } else {

      }
      return true;
   }

   private void readCustomKey(ByteBuf in, List<Object> out) {
      decodeCtx.decoder().customReadKey(decodeCtx.header(), in, decodeCtx, out);
      // If out was written to, it means we read everything, else we have to reread again
      if (!out.isEmpty()) {
         resetRequested = true;
      }
   }

   boolean decodeParameters(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
      return true;
   }

   boolean decodeValue(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
      return true;
   }
}
