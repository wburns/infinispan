package org.infinispan.server.core.transport;

import static org.infinispan.server.core.logging.Log.SERVER;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import io.netty.channel.IoEventLoop;
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.ManualIoEventLoop;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

// This is a separate class for easier replacement within Quarkus
public final class NativeTransport {
   private static final boolean IS_LINUX = System.getProperty("os.name").toLowerCase().startsWith("linux");
   private static final String USE_EPOLL_PROPERTY = "infinispan.server.channel.epoll";
   private static final String USE_IOURING_PROPERTY = "infinispan.server.channel.iouring";

   private static final boolean EPOLL_DISABLED = System.getProperty(USE_EPOLL_PROPERTY, "true").equalsIgnoreCase("false");
   private static final boolean IOURING_DISABLED = System.getProperty(USE_IOURING_PROPERTY, "true").equalsIgnoreCase("false");

   private static final long RUNNING_YIELD_NS = TimeUnit.MICROSECONDS
         .toNanos(Integer.getInteger("infinispan.virtualthread.running.yield.us", 1));

   // Has to be after other static variables to ensure they are initialized
   private static final Type TYPE = transportType();

   enum Type {
      EPOLL,
      IOURING,
      NIO
   }

   private static Type transportType() {
      if (useNativeEpoll()) {
         return Type.EPOLL;
      } else if (useNativeIoUring()) {
         return Type.IOURING;
      } else {
         return Type.NIO;
      }
   }

   private static boolean useNativeEpoll() {
      try {
         Class.forName("io.netty.channel.epoll.Epoll", true, NativeTransport.class.getClassLoader());
         if (Epoll.isAvailable()) {
            return !EPOLL_DISABLED && IS_LINUX;
         } else {
            if (IS_LINUX) {
               SERVER.epollNotAvailable(Epoll.unavailabilityCause().toString());
            }
         }
      } catch (ClassNotFoundException e) {
         if (IS_LINUX) {
            SERVER.epollNotAvailable(e.getMessage());
         }
      }
      return false;
   }

   private static boolean useNativeIoUring() {
      try {
         Class.forName("io.netty.channel.uring.IoUring", true, NativeTransport.class.getClassLoader());
         if (IoURingNativeTransport.isAvailable()) {
            return !IOURING_DISABLED && IS_LINUX;
         } else {
            if (IS_LINUX) {
               SERVER.ioUringNotAvailable(IoURingNativeTransport.unavailableCause());
            }
         }
      } catch (ClassNotFoundException e) {
         if (IS_LINUX) {
            SERVER.ioUringNotAvailable(e.getMessage());
         }
      }
      return false;
   }

   public static Class<? extends ServerSocketChannel> serverSocketChannelClass() {
      switch (TYPE) {
//         case EPOLL -> {
//            SERVER.usingTransport("Epoll");
//            return EpollServerSocketChannel.class;
//         }
//         case IOURING ->  {
//            SERVER.usingTransport("IOURING");
//            return IoURingNativeTransport.serverSocketChannelClass();
//         }
         default ->  {
            SERVER.usingTransport("NIO");
            return NioServerSocketChannel.class;
         }
      }
   }

   public static MultithreadEventLoopGroup createEventLoopGroup(int maxExecutors, ThreadFactory threadFactory) {
      if (!(threadFactory instanceof DefaultThreadFactory)) {
         // TODO: assert that threadFactory supports vthreads
         return new MultiThreadIoEventLoopGroup(maxExecutors, (Executor) null, NioIoHandler.newFactory()) {
            @Override
            protected IoEventLoop newChild(Executor executor, IoHandlerFactory ioHandlerFactory, Object... args) {
               ManualIoEventLoop eventLoop = new ManualIoEventLoop(this, null, ioHandlerFactory);
               Thread vt = threadFactory.newThread(() -> {
                  while (!eventLoop.isShuttingDown()) {
                     eventLoop.run(0, RUNNING_YIELD_NS);
                     Thread.yield();
                     eventLoop.runNonBlockingTasks(RUNNING_YIELD_NS);
                     Thread.yield();
                  }
                  while (!eventLoop.isTerminated()) {
                     eventLoop.runNow();
                     Thread.yield();
                  }
               });
               eventLoop.setOwningThread(vt);
               vt.start();
               return eventLoop;
            }
         };
      }
      return switch (TYPE) {
//         case EPOLL -> new MultiThreadIoEventLoopGroup(maxExecutors, threadFactory, EpollIoHandler.newFactory());
//         case IOURING -> IoURingNativeTransport.createEventLoopGroup(maxExecutors, threadFactory);
         default -> new MultiThreadIoEventLoopGroup(maxExecutors, threadFactory, NioIoHandler.newFactory());
      };
   }
}
