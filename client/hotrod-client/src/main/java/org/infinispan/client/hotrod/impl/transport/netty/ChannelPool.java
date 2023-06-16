package org.infinispan.client.hotrod.impl.transport.netty;

import java.net.SocketAddress;

import io.netty.channel.Channel;

public interface ChannelPool {
   void acquire(ChannelOperation callback);

   void release(Channel channel, ChannelRecord record);

   void releaseClosedChannel(Channel channel, ChannelRecord channelRecord);

   SocketAddress getAddress();

   int getActive();

   int getIdle();

   int getConnected();

   void close();

   void inspectPool();
}
