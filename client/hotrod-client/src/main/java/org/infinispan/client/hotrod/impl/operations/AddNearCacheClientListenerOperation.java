package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class AddNearCacheClientListenerOperation extends ClientListenerOperation {

   private final int nearCacheSize;

   protected AddNearCacheClientListenerOperation(InternalRemoteCache<?, ?> remoteCache, Object listener,
                                                 int nearCacheSize) {
      super(remoteCache, listener);
      this.nearCacheSize = nearCacheSize;
   }

   private AddNearCacheClientListenerOperation(InternalRemoteCache<?, ?> remoteCache, Object listener,
                                               byte[] listenerId, int nearCacheSize) {
      super(remoteCache, listener, listenerId);
      this.nearCacheSize = nearCacheSize;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      ByteBufUtil.writeArray(buf, listenerId);
      ByteBufUtil.writeVInt(buf, nearCacheSize);
   }

   @Override
   public Channel createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      return decoder.getChannel();
   }

   @Override
   public short requestOpCode() {
      return ADD_NEAR_CACHE_LISTENER_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return ADD_NEAR_CACHE_LISTENER_RESPONSE;
   }

   @Override
   public ClientListenerOperation copy() {
      return new AddNearCacheClientListenerOperation(internalRemoteCache, listener, listenerId, nearCacheSize);
   }
}
