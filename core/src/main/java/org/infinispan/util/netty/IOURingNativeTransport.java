package org.infinispan.util.netty;

import java.util.concurrent.ThreadFactory;

import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;
import io.netty.incubator.channel.uring.IOUringServerSocketChannel;
import io.netty.incubator.channel.uring.IOUringSocketChannel;

/**
 * @since 14.0
 **/
public class IOURingNativeTransport {

   public static Class<? extends ServerSocketChannel> serverSocketChannelClass() {
      return IOUringServerSocketChannel.class;
   }

   public static Class<? extends SocketChannel> clientSocketChannelClass() {
      return IOUringSocketChannel.class;
   }

   public static MultithreadEventLoopGroup createEventLoopGroup(int maxExecutors, ThreadFactory threadFactory) {
      return new IOUringEventLoopGroup(maxExecutors, threadFactory);
   }
}
