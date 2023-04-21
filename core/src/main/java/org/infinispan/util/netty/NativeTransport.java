package org.infinispan.util.netty;

import java.util.concurrent.ThreadFactory;

import org.infinispan.commons.logging.Log;

import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

// This is a separate class for easier replacement within Quarkus
public final class NativeTransport {
   private static final boolean IS_LINUX = System.getProperty("os.name").toLowerCase().startsWith("linux");
   private static final String USE_EPOLL_PROPERTY = "infinispan.server.channel.epoll";
   private static final String USE_IOURING_PROPERTY = "infinispan.server.channel.iouring";

   private static final boolean EPOLL_DISABLED = System.getProperty(USE_EPOLL_PROPERTY, "true").equalsIgnoreCase("false");
   private static final boolean IOURING_DISABLED = System.getProperty(USE_IOURING_PROPERTY, "true").equalsIgnoreCase("false");

   // Has to be after other static variables to ensure they are initialized
   public static final boolean USE_NATIVE_EPOLL = useNativeEpoll();
   public static final boolean USE_NATIVE_IOURING = useNativeIOUring();

   private static boolean useNativeEpoll() {
      try {
         Class.forName("io.netty.channel.epoll.Epoll", true, NativeTransport.class.getClassLoader());
         if (Epoll.isAvailable()) {
            return !EPOLL_DISABLED && IS_LINUX;
         } else {
            if (IS_LINUX) {
               Log.CONTAINER.epollNotAvailable(Epoll.unavailabilityCause().toString());
            }
         }
      } catch (ClassNotFoundException e) {
         if (IS_LINUX) {
            Log.CONTAINER.epollNotAvailable(e.getMessage());
         }
      }
      return false;
   }

   private static boolean useNativeIOUring() {
      try {
         Class.forName("io.netty.incubator.channel.uring.IOUring", true, NativeTransport.class.getClassLoader());
         if (io.netty.incubator.channel.uring.IOUring.isAvailable()) {
            return !IOURING_DISABLED && IS_LINUX;
         } else {
            if (IS_LINUX) {
               Log.CONTAINER.ioUringNotAvailable(io.netty.incubator.channel.uring.IOUring.unavailabilityCause().toString());
            }
         }
      } catch (ClassNotFoundException e) {
         if (IS_LINUX) {
            Log.CONTAINER.ioUringNotAvailable(e.getMessage());
         }
      }
      return false;
   }

   public static Class<? extends ServerSocketChannel> serverSocketChannelClass() {
      if (USE_NATIVE_EPOLL) {
         Log.CONTAINER.usingTransport("Epoll");
         return EpollServerSocketChannel.class;
      } else if (USE_NATIVE_IOURING) {
         Log.CONTAINER.usingTransport("IOUring");
         return IOURingNativeTransport.serverSocketChannelClass();
      } else {
         Log.CONTAINER.usingTransport("NIO");
         return NioServerSocketChannel.class;
      }
   }

   public static Class<? extends SocketChannel> clientSocketChannelClass() {
      if (USE_NATIVE_EPOLL) {
         return EpollSocketChannel.class;
      } else if (USE_NATIVE_IOURING) {
         return IOURingNativeTransport.clientSocketChannelClass();
      } else {
         return NioSocketChannel.class;
      }
   }

   public static MultithreadEventLoopGroup createEventLoopGroup(int maxExecutors, ThreadFactory threadFactory) {
      if (USE_NATIVE_EPOLL) {
         return new EpollEventLoopGroup(maxExecutors, threadFactory);
      } else if (USE_NATIVE_IOURING) {
         return IOURingNativeTransport.createEventLoopGroup(maxExecutors, threadFactory);
      } else {
         return new NioEventLoopGroup(maxExecutors, threadFactory);
      }
   }
}
