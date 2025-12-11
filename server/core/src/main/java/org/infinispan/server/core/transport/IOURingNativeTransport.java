package org.infinispan.server.core.transport;

import java.util.concurrent.ThreadFactory;

import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.uring.IoUring;
import io.netty.channel.uring.IoUringIoHandler;
import io.netty.channel.uring.IoUringServerSocketChannel;

/**
 * @since 14.0
 **/
public class IOURingNativeTransport {

   public static Class<? extends ServerSocketChannel> serverSocketChannelClass() {
      return IoUringServerSocketChannel.class;
   }

   public static MultithreadEventLoopGroup createEventLoopGroup(int maxExecutors, ThreadFactory threadFactory) {
      return new MultiThreadIoEventLoopGroup(maxExecutors, threadFactory, IoUringIoHandler.newFactory());
   }

   public static boolean isAvailable() {
      return IoUring.isAvailable();
   }

   public static String unavailableCause() {
      return IoUring.unavailabilityCause().toString();
   }
}
