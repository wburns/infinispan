package org.infinispan.client.hotrod.impl.operations;

import java.util.Set;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class RemoveNearCacheKeysOperation extends AbstractCacheOperation<Void> {

   private final Set<byte[]> keys;

   public RemoveNearCacheKeysOperation(InternalRemoteCache<?, ?> remoteCache, Set<byte[]> keys) {
      super(remoteCache);
      this.keys = keys;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      ByteBufUtil.writeVInt(buf, keys.size());
      for (byte[] key : keys) {
         ByteBufUtil.writeArray(buf, key);
      }
   }

   @Override
   public Void createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      return null;
   }

   @Override
   public short requestOpCode() {
      return REMOVE_NEAR_CACHE_KEYS_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return REMOVE_NEAR_CACHE_KEYS_RESPONSE;
   }

   @Override
   public DataFormat getDataFormat() {
      return null;
   }
}
