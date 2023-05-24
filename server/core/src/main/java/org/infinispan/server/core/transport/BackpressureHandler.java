package org.infinispan.server.core.transport;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.core.logging.Log;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;

/**
 *
 */
public class BackpressureHandler extends ChannelDuplexHandler {
   protected final Log log = LogFactory.getLog(this.getClass(), Log.class);

   protected final boolean treatFlushAsCompleted;

   // All the following variables should only be read while on the event loop

   // This may be -1 in the case where an operation flushes before finishing the read call
   protected int pendingOperations = 0;
   protected int lowWatermark;
   protected int highWatermark;
   boolean writeable = true;
   boolean reachedHighWatermark;

   // This variable can be referenced by multiple threads
   protected final AtomicInteger concurrentCompletions = new AtomicInteger();

   protected BackpressureHandler(boolean treatFlushAsCompleted, int lowWatermark, int highWatermark) {
      this.treatFlushAsCompleted = treatFlushAsCompleted;
      this.lowWatermark = lowWatermark;
      this.highWatermark = highWatermark;
   }

   @Override
   public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
      super.channelWritabilityChanged(ctx);
      // When a channel is no longer writeable we shouldn't read anything from it to prevent overwhelming the
      // client, give it some time to consume our output and when it is available again we resume reading
      writeable = ctx.channel().isWritable();
      if (reachedHighWatermark) {
         log.tracef("Channel %s writeability has changed to %s, however concurrent operations is still disabling auto read", ctx.channel(), writeable);
         return;
      }
      log.tracef("Channel %s writeability has changed to %s, setting auto read to same", ctx.channel(), writeable);
      ctx.channel().config().setAutoRead(writeable);
   }

   @Override
   public void flush(ChannelHandlerContext ctx) throws Exception {
      if (treatFlushAsCompleted) {
         decrementPendingOperations(ctx.channel());
      }
      super.flush(ctx);
   }

   @Override
   public final void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (!actualRead(ctx, msg)) {
         if (++pendingOperations == highWatermark && !reachedHighWatermark) {
            log.tracef("Channel %s has met high watermark %d for concurrent operations, disabling auto read", ctx.channel(), highWatermark);
            reachedHighWatermark = true;
            ctx.channel().config().setAutoRead(false);
         }
      }
   }

   protected void decrementPendingOperations(Channel ch) {
      if (pendingOperations-- == lowWatermark && reachedHighWatermark) {
         lowWatermarkMet(ch);
      }
   }

   /**
    * This method is to invoked by extensions of this class to notify us that a msg has now completed that wasn't
    * completed when {@link #actualRead(ChannelHandlerContext, Object)} was invoked.
    * @param ch The channel tied to this message
    */
   public void completedPriorMessage(Channel ch) {
      if (ch.eventLoop().inEventLoop()) {
         decrementPendingOperations(ch);
      } else if (concurrentCompletions.getAndIncrement() == 0) {
         // Submit to event loop
         ch.eventLoop().submit(() -> updateCompletions(ch));
      }
   }

   private void lowWatermarkMet(Channel ch) {
      reachedHighWatermark = false;
      if (!writeable) {
         log.tracef("Channel %s has reduced concurrent operations to minimum watermark, however channel is still not readable, so not re-enabling auto read yet", ch);
         return;
      }
      log.tracef("Channel %s has reduced concurrent operations to minimum watermark, re-enabling auto read", ch);
      ch.config().setAutoRead(true);
   }

   void updateCompletions(Channel ch) {
      assert ch.eventLoop().inEventLoop();
      int decrements = concurrentCompletions.getAndSet(0);
      pendingOperations -= decrements;
      if (pendingOperations < lowWatermark && reachedHighWatermark) {
         lowWatermarkMet(ch);
      }
   }

   /**
    * This method is here to be extended by implementations to provide some additional processing after a read operation
    * is done passing the context and message object as a normal {@link io.netty.channel.ChannelInboundHandler#channelRead(ChannelHandlerContext, Object)}
    * invocation. The method should return if the operation was completed or not. If it wasn't completed then the pending
    * operation will be incremented and the implementation should eventually invoke {@link BackpressureHandler#decrementPendingOperations(Channel)}
    * to signal that the operation is complete. Optinally if {@link BackpressureHandler#treatFlushAsCompleted} is <b>TRUE</b>
    * then flushing a single result will be sufficient to decrement the count.
    * <p>
    * By default this method returns the negation of {@link BackpressureHandler#treatFlushAsCompleted} since the
    * command in that case is completed when the flush is done.
    * @param ctx The handler context for this handler
    * @param msg The object message provided from downstream handlers
    * @return whether the operation was completed
    * @throws Exception if any error happens in processing the message
    */
   protected boolean actualRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      return !treatFlushAsCompleted;
   }
}
