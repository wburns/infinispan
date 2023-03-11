package org.infinispan.server.resp;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.server.core.transport.NativeTransport;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.function.TriConsumer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.FastThreadLocal;

public abstract class RespRequestHandler {
   protected final CompletionStage<RespRequestHandler> myStage = CompletableFuture.completedStage(this);
   protected final RespServer respServer;
   protected AdvancedCache<byte[], byte[]> cache;

   protected RespRequestHandler(RespServer respServer) {
      this.respServer = respServer;
      this.cache = respServer.getCache()
            .withMediaType(MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_OCTET_STREAM);
   }

   /**
    * Handles the RESP request returning a stage that when complete notifies the command has completed as well as
    * providing the request handler for subsequent commands.
    *
    * @param ctx Netty context pipeline for this request
    * @param type The command type
    * @param arguments The remaining arguments to the command
    * @return stage that when complete returns the new handler to instate. This stage <b>must</b> be completed on the event loop
    */
   public CompletionStage<RespRequestHandler> handleRequest(ChannelHandlerContext ctx, String type, List<byte[]> arguments) {
      if ("QUIT".equals(type)) {
         ctx.close();
         return myStage;
      }
      ctx.writeAndFlush(stringToByteBuf("-ERR unknown command\r\n", ctx.alloc()));
      return myStage;
   }

   public void handleChannelDisconnect(ChannelHandlerContext ctx) {
      // Do nothing by default
   }

   protected <E> CompletionStage<RespRequestHandler> stageToReturn(CompletionStage<E> stage, ChannelHandlerContext ctx,
         TriConsumer<? super E, ChannelHandlerContext, Throwable> triConsumer) {
      return stageToReturn(stage, ctx, Objects.requireNonNull(triConsumer), null);
   }

   protected <E> CompletionStage<RespRequestHandler> stageToReturn(CompletionStage<E> stage, ChannelHandlerContext ctx,
         Function<E, RespRequestHandler> handlerWhenComplete) {
      return stageToReturn(stage, ctx, null, Objects.requireNonNull(handlerWhenComplete));
   }

   /**
    * Handles ensuring that a stage TriConsumer is only invoked on the event loop. The TriConsumer can then do things
    * such as writing to the underlying channel or update variables in a thread safe manner without additional
    * synchronization or volatile variables.
    * <p>
    * If the TriConsumer provided is null the provided stage <b>must</b> be completed only on the event loop of the
    * provided ctx.
    * This can be done via the Async methods on {@link CompletionStage} such as {@link CompletionStage#thenApplyAsync(Function, Executor)};
    * <p>
    * If the returned stage was completed exceptionally, any exception is ignored, assumed to be handled in the
    * <b>biConsumer</b> or further up the stage.
    * The <b>handlerWhenComplete</b> is not invoked if the stage was completed exceptionally as well.
    * If the <b>triConsumer</b> or <b>handlerWhenComplete</b> throw an exception when invoked the returned stage will
    * complete with that exception.
    * <p>
    * This method can only be invoked on the event loop for the provided ctx.
    *
    * @param <E> The stage return value
    * @param stage The stage that will complete. Note that if biConsumer is null this stage may only be completed on the event loop
    * @param ctx The context used for this request, normally the event loop is the thing used
    * @param triConsumer The consumer to be ran on
    * @param handlerWhenComplete A function to map the result to which handler will be used for future requests. Only ever invoked on the event loop. May be null, if so the current handler is returned
    * @return A stage that is only ever completed on the event loop that provides the new handler to use
    */
   private <E> CompletionStage<RespRequestHandler> stageToReturn(CompletionStage<E> stage, ChannelHandlerContext ctx,
         TriConsumer<? super E, ChannelHandlerContext, Throwable> triConsumer, Function<E, RespRequestHandler> handlerWhenComplete) {
      assert ctx.channel().eventLoop().inEventLoop();
      // Only one or the other can be null
      assert (triConsumer != null && handlerWhenComplete == null) || (triConsumer == null && handlerWhenComplete != null) :
            "triConsumer was: " + triConsumer + " and handlerWhenComplete was: " + handlerWhenComplete;
      if (CompletionStages.isCompletedSuccessfully(stage)) {
         E result = CompletionStages.join(stage);
         try {
            if (handlerWhenComplete != null) {
               return CompletableFuture.completedFuture(handlerWhenComplete.apply(result));
            }
            triConsumer.accept(result, ctx, null);
         } catch (Throwable t) {
            return CompletableFutures.completedExceptionFuture(t);
         }
         return myStage;
      }
      if (triConsumer != null) {
         // Note that this method is only ever invoked in the event loop, so this whenCompleteAsync can never complete
         // until this request completes, meaning the thenApply will always be invoked in the event loop as well
         return CompletionStages.handleAndComposeAsync(stage, (e, t) -> {
            try {
               triConsumer.accept(e, ctx, t);
               return myStage;
            } catch (Throwable innerT) {
               if (t != null) {
                  innerT.addSuppressed(t);
               }
               return CompletableFutures.completedExceptionFuture(innerT);
            }
         }, ctx.channel().eventLoop());
      }
      return stage.handle((value, t) -> {
         if (t != null) {
            // If exception, never use handler
            return this;
         }
         return handlerWhenComplete.apply(value);
      });
   }

   static ByteBuf bytesToResult(byte[] result, ByteBufAllocator allocator) {
      int length = result.length;
      int stringLength = stringSize(length);

      // Need 5 extra for $ and 2 sets of /r/n
      int exactSize = stringLength + length + 5;
      ByteBuf buffer = bufferToUse(allocator, exactSize);
      buffer.writeByte('$');
      // This method is anywhere from 10-100% faster than ByteBufUtil.writeAscii by avoiding String allocation
      setIntChars(length, stringLength, buffer);
      buffer.writeByte('\r').writeByte('\n');
      buffer.writeBytes(result);
      buffer.writeByte('\r').writeByte('\n');

      return buffer;
   }

   static int stringSize(int x) {
      int d = 1;
      if (x >= 0) {
         d = 0;
         x = -x;
      }
      int p = -10;
      for (int i = 1; i < 10; i++) {
         if (x > p)
            return i + d;
         p = 10 * p;
      }
      return 10 + d;
   }

   static int setIntChars(int i, int index, ByteBuf buf) {
      int writeIndex = buf.writerIndex();
      int q, r;
      int charPos = index;

      boolean negative = i < 0;
      if (!negative) {
         i = -i;
      }

      // Generate two digits per iteration
      while (i <= -100) {
         q = i / 100;
         r = (q * 100) - i;
         i = q;
         buf.setByte(writeIndex + --charPos, DigitOnes[r]);
         buf.setByte(writeIndex + --charPos, DigitTens[r]);
      }

      // We know there are at most two digits left at this point.
      q = i / 10;
      r = (q * 10) - i;
      buf.setByte(writeIndex + --charPos, (byte) ('0' + r));

      // Whatever left is the remaining digit.
      if (q < 0) {
         buf.setByte(writeIndex + --charPos, (byte) ('0' - q));
      }

      if (negative) {
         buf.setByte(writeIndex + --charPos, (byte) '-');
      }
      buf.writerIndex(writeIndex + index);
      return charPos;
   }

   static final byte[] DigitTens = {
         '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
         '1', '1', '1', '1', '1', '1', '1', '1', '1', '1',
         '2', '2', '2', '2', '2', '2', '2', '2', '2', '2',
         '3', '3', '3', '3', '3', '3', '3', '3', '3', '3',
         '4', '4', '4', '4', '4', '4', '4', '4', '4', '4',
         '5', '5', '5', '5', '5', '5', '5', '5', '5', '5',
         '6', '6', '6', '6', '6', '6', '6', '6', '6', '6',
         '7', '7', '7', '7', '7', '7', '7', '7', '7', '7',
         '8', '8', '8', '8', '8', '8', '8', '8', '8', '8',
         '9', '9', '9', '9', '9', '9', '9', '9', '9', '9',
   };

   static final byte[] DigitOnes = {
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
   };

   static ByteBuf bufferToUse(ByteBufAllocator allocator, int exactSize) {
      if (exactSize <= 1024) {
         ByteBuf buf = bufferThreadLocal.get();
         // Only use the cached buffer if we are the first one to request it. If refCnt is not 1 that means a previous
         // request is still using the buffer. This is most likely caused by the client socket not reading fast enough
         if (buf.refCnt() == 1) {
            buf.clear();
            buf.retain();
            return buf;
         }
      }
      return allocator.buffer(exactSize, exactSize);
   }

   private static final FastThreadLocal<ByteBuf> bufferThreadLocal = new FastThreadLocal<>() {
      @Override
      protected ByteBuf initialValue() throws Exception {
         // Using the correct type will prevent additional casting and we don't want to use pool
         if (NativeTransport.USE_NATIVE_EPOLL || NativeTransport.USE_NATIVE_IOURING) {
            return Unpooled.directBuffer(1024, 1024);
         }
         return Unpooled.buffer(1024, 1024);
      }

      @Override
      protected void onRemoval(ByteBuf value) throws Exception {
         value.release();
      }
   };

   static ByteBuf stringToByteBufWithExtra(CharSequence string, ByteBufAllocator allocator, int extraBytes) {
      boolean release = true;
      // This is allocating 3x the string size. Unfortunately, no way to have netty write UTF-8 without checking this that I could find
      int sizeToUse = ByteBufUtil.utf8MaxBytes(string) + extraBytes;

      ByteBuf buffer = bufferToUse(allocator, sizeToUse);

      try {
         ByteBufUtil.writeUtf8(buffer, string);
         release = false;
      } finally {
         if (release) {
            buffer.release();
         }
      }

      return buffer;
   }

   static ByteBuf stringToByteBuf(CharSequence string, ByteBufAllocator allocator) {
      return stringToByteBufWithExtra(string, allocator, 0);
   }
}
