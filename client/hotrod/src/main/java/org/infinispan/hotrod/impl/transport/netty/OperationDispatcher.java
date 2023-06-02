package org.infinispan.hotrod.impl.transport.netty;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.infinispan.hotrod.impl.operations.HotRodOperation;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.internal.shaded.org.jctools.queues.MessagePassingQueue;
import io.netty.util.internal.shaded.org.jctools.queues.MpscUnboundedArrayQueue;

public class OperationDispatcher implements BiConsumer<Channel, Throwable>,
      MessagePassingQueue.Consumer<ChannelOperation>, GenericFutureListener<Future<Void>> {
   final MessagePassingQueue<ChannelOperation> queue = new MpscUnboundedArrayQueue<>(128);
   final Consumer<HotRodOperation<?>> closedConsumer;

   volatile Channel channel;

   // These variables can only be referenced in the EventLoop for the given channel
   final Set<ChannelOperation> pendingOperations = new HashSet<>();
   ByteBuf buffer;

   private OperationDispatcher(Consumer<HotRodOperation<?>> closedConsumer) {
      this.closedConsumer = closedConsumer;
   }

   public static OperationDispatcher newDispatcher(CompletionStage<Channel> channelStage,
                                                   Consumer<HotRodOperation<?>> closedConsumer) {
      OperationDispatcher dispatcher = new OperationDispatcher(closedConsumer);
      channelStage.whenComplete(dispatcher);
      return dispatcher;
   }

   public void dispatchOperation(ChannelOperation channelOperation) {
      queue.offer(channelOperation);
      if (channel != null) {
         if (channel.eventLoop().inEventLoop()) {
            sendOperations();
         } else {
            channel.eventLoop().submit(this::sendOperations);
         }
      }
   }

   private void sendOperations() {
      // TODO: can we get an estimate?
      buffer = channel.alloc().buffer();
      queue.drain(this);
      channel.writeAndFlush(buffer);
   }

   @Override
   public void accept(ChannelOperation e) {
      e.writeBytes(buffer);
      pendingOperations.add(e);
   }

   @Override
   public void accept(Channel channel, Throwable throwable) {
      if (throwable != null) {
         // TODO: need to reject all queued operations
         return;
      }
      this.channel = channel;
      channel.closeFuture().addListener(this);
   }

   @Override
   public void operationComplete(Future<Void> future) throws Exception {
      queue.drain(co -> closedConsumer.accept((HotRodOperation<?>) co));
      pendingOperations.forEach(co -> closedConsumer.accept((HotRodOperation<?>) co));
   }
}
