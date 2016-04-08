package org.infinispan.server.hotrod;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;
import org.infinispan.server.core.NewServerConstants;
import org.infinispan.server.core.UnsignedNumeric;
import org.infinispan.server.core.logging.JavaLog;
import org.infinispan.server.core.transport.ExtendedByteBuf;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.hotrod.configuration.AuthenticationConfiguration;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import scala.Option;
import scala.Tuple2;

import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

/**
 * // TODO: Document this
 *
 * @author wburns
 * @since 4.0
 */
public class NewHotRodDecoder extends ByteToMessageDecoder {
   private final static JavaLog log = LogFactory.getLog(NewHotRodDecoder.class, JavaLog.class);

   private final EmbeddedCacheManager cacheManager;
   private final NettyTransport transport;
   private final Predicate<? super String> ignoreCache;

   private final AuthenticationConfiguration authenticationConfig;
   private final boolean secure;
   private final boolean requireAuthentication;

   private final CacheDecodeContext decodeCtx;

   private Subject subject = NewServerConstants.ANONYMOUS;

   private HotRodDecoderState state = HotRodDecoderState.DECODE_HEADER;

   private volatile boolean resetRequested = true;

   public NewHotRodDecoder(EmbeddedCacheManager cacheManager, NettyTransport transport, HotRodServer server,
           Predicate<? super String> ignoreCache) {
      this.cacheManager = cacheManager;
      this.transport = transport;
      this.ignoreCache = ignoreCache;

      authenticationConfig = ((HotRodServerConfiguration) server.getConfiguration()).authentication();
      secure = authenticationConfig.enabled();
      requireAuthentication = secure && authenticationConfig.mechProperties().containsKey(Sasl.POLICY_NOANONYMOUS)
              && authenticationConfig.mechProperties().get(Sasl.POLICY_NOANONYMOUS).equals("true");

      this.decodeCtx = new CacheDecodeContext(server);
   }

   public NettyTransport getTransport() {
      return transport;
   }

   void resetNow() {
      decodeCtx.resetParams();
      decodeCtx.setHeader(new HotRodHeader());
      state = HotRodDecoderState.DECODE_HEADER;
      resetRequested = false;
   }

   /**
    * Should be called when state is transferred.  This also marks the buffer read
    * position so when we can't read bytes it will be reset to here.
    * @param newState new hotrod decoder state
    * @param buf the byte buffer to mark
    */
   protected void state(HotRodDecoderState newState, ByteBuf buf) {
      buf.markReaderIndex();
      state = newState;
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

         // Mark the index to the beginning, just in case
         in.markReaderIndex();

         Callable<Void> callable = () -> {
            switch (state) {
               // These are all fall through cases which means they call to the one below if they needed additional
               // processing
               case DECODE_HEADER:
                  if (!decodeHeader(in, out)) {
                     break;
                  }
                  state(HotRodDecoderState.DECODE_KEY, in);
               case DECODE_KEY:
                  if (!decodeKey(in, out)) {
                     break;
                  }
                  state(HotRodDecoderState.DECODE_PARAMETERS, in);
               case DECODE_PARAMETERS:
                  if (!decodeParameters(in, out)) {
                     break;
                  }
                  state(HotRodDecoderState.DECODE_VALUE, in);
               case DECODE_VALUE:
                  if (!decodeValue(in, out)) {
                     break;
                  }
                  state(HotRodDecoderState.DECODE_HEADER, in);
                  break;

               // These are terminal cases, meaning they only perform this operation and break
               case DECODE_HEADER_CUSTOM:
                  readCustomHeader(in, out);
                  break;
               case DECODE_KEY_CUSTOM:
                  readCustomKey(in, out);
                  break;
               case DECODE_VALUE_CUSTOM:
                  readCustomValue(in, out);
                  break;
            }
            int remainingBytes;
            if (!out.isEmpty() && (remainingBytes = in.readableBytes()) > 0) {
               // Clear out the request and wind bytes up to last so next caller isn't corrupted
               out.clear();
               in.readerIndex(in.writerIndex());
               HotRodHeader header = decodeCtx.header();
               throw new RequestParsingException("There are too many bytes for op " + header.op() +
                       " for version " + header.version() + " - had " + remainingBytes + " left over",
                       header.version(), header.messageId());
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
         return false;
      }
      HotRodHeader header = decodeCtx.getHeader();
      // Check if this cache can be accessed or not
      if (ignoreCache.test(header.cacheName())) {
         CacheUnavailableException excp = new CacheUnavailableException();
         decodeCtx.setError(excp);
         throw excp;
      }
      decodeCtx.obtainCache(cacheManager);
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
            // Continue to key
            return true;
      }
   }

   /**
    * Reads the header and returns whether we should try to continue
    * @param buffer the buffer to read the header from
    * @return whether or not we should continue
    * @throws Exception
    */
   boolean readHeader(ByteBuf buffer) throws Exception {
      if (buffer.readableBytes() < 1) {
         return false;
      }
      short magic = buffer.readUnsignedByte();
      if (magic != Constants$.MODULE$.MAGIC_REQ()) {
         if (decodeCtx.getError() == null) {
            Exception excp = new InvalidMagicIdException("Error reading magic byte or message id: " + magic);
            decodeCtx.setError(excp);
            throw excp;
         } else {
            log.tracef("Error happened previously, ignoring %d byte until we find the magic number again", magic);
            return false;
         }
      } else {
         decodeCtx.setError(null);
      }

      OptionalLong optLong = UnsignedNumeric.readOptionalUnsignedLong(buffer);
      if (!optLong.isPresent()) {
         return false;
      }
      long messageId = optLong.getAsLong();
      if (buffer.readableBytes() < 1) {
         return false;
      }
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
         if (!decoder.readHeader(buffer, version, messageId, header, requireAuthentication
                 && subject == NewServerConstants.ANONYMOUS)) {
            return false;
         }
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

   private void readCustomHeader(ByteBuf in, List<Object> out) {
      decodeCtx.decoder().customReadHeader(decodeCtx.header(), in, decodeCtx, out);
      // If out was written to, it means we read everything, else we have to reread again
      if (!out.isEmpty()) {
         resetRequested = true;
      }
   }

   boolean decodeKey(ByteBuf in, List<Object> out) {
      NewHotRodOperation op = decodeCtx.getHeader().op();
      // If we want a single key read that - else we do try for custom read
      if (op.requiresKey()) {
         Option<byte[]> bytes = ExtendedByteBuf.readMaybeRangedBytes(in);
         // If the bytes don't exist then we need to reread
         if (bytes.isDefined()) {
            decodeCtx.key_$eq(bytes.get());
         } else {
            return false;
         }
      }
      switch (op.getDecoderRequirements()) {
         case KEY_CUSTOM:
            state(HotRodDecoderState.DECODE_KEY_CUSTOM, in);
            readCustomKey(in, out);
            return false;
         case KEY:
            out.add(decodeCtx);
            resetRequested = true;
            return false;
         default:
            return true;
      }
   }

   private void readCustomKey(ByteBuf in, List<Object> out) {
      decodeCtx.decoder().customReadKey(decodeCtx.header(), in, decodeCtx, out);
      // If out was written to, it means we read everything, else we have to reread again
      if (!out.isEmpty()) {
         resetRequested = true;
      }
   }

   boolean decodeParameters(ByteBuf in, List<Object> out) {
      Option<RequestParameters> params = decodeCtx.decoder().readParameters(decodeCtx.header(), in);
      if (params.isDefined()) {
         decodeCtx.params_$eq(params.get());
         if (decodeCtx.header().op().getDecoderRequirements() == DecoderRequirements.PARAMETERS) {
            out.add(decodeCtx);
            resetRequested = true;
            return false;
         }
         return true;
      } else {
         return false;
      }
   }

   boolean decodeValue(ByteBuf in, List<Object> out) {
      NewHotRodOperation op = decodeCtx.header().op();
      if (op.requireValue()) {
         int valueLength = decodeCtx.params().valueLength();
         if (in.readableBytes() < valueLength) {
            return false;
         }
         byte[] bytes = new byte[valueLength];
         in.readBytes(bytes);
         decodeCtx.rawValue_$eq(bytes);
      }
      switch (op.getDecoderRequirements()) {
         case VALUE_CUSTOM:
            state(HotRodDecoderState.DECODE_VALUE_CUSTOM, in);
            readCustomValue(in, out);
            return false;
         case VALUE:
            out.add(decodeCtx);
            resetRequested = true;
            return false;
      }

      return true;
   }

   private void readCustomValue(ByteBuf in, List<Object> out) {
      decodeCtx.decoder().customReadValue(decodeCtx.header(), in, decodeCtx, out);
      // If out was written to, it means we read everything, else we have to reread again
      if (!out.isEmpty()) {
         resetRequested = true;
      }
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      super.exceptionCaught(ctx, cause);
      decodeCtx.exceptionCaught(ctx, cause);
      resetRequested = true;
   }
}
