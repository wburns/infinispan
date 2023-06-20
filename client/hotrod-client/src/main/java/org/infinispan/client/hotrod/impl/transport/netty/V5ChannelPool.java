package org.infinispan.client.hotrod.impl.transport.netty;

import java.net.SocketAddress;

import io.netty.channel.Channel;

public class V5ChannelPool implements ChannelPool {
   @Override
   public void acquire(ChannelOperation callback) {

   }

   @Override
   public void release(Channel channel, ChannelRecord record) {

   }

   @Override
   public void releaseClosedChannel(Channel channel, ChannelRecord channelRecord) {

   }

   @Override
   public SocketAddress getAddress() {
      return null;
   }

   @Override
   public int getActive() {
      return 0;
   }

   @Override
   public int getIdle() {
      return 0;
   }

   @Override
   public int getConnected() {
      return 0;
   }

   @Override
   public void close() {

   }

   @Override
   public void inspectPool() {

   }
}
