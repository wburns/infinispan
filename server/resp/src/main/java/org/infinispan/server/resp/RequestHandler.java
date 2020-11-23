package org.infinispan.server.resp;

import java.util.List;

import org.infinispan.Cache;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;

public interface RequestHandler {
   default RequestHandler handleRequest(ChannelHandlerContext ctx, Cache<byte[], byte[]> cache, String type, List<byte[]> arguments) {
      ctx.writeAndFlush(stringToByteBuf("-ERR unknown command" + "\r\n", ctx.alloc()));
      return this;
   }

   default ByteBuf stringToByteBufWithExtra(CharSequence string, ByteBufAllocator allocator, int extraBytes) {
      boolean release = true;
      ByteBuf buffer = allocator.buffer(ByteBufUtil.utf8Bytes(string) + extraBytes);

      ByteBuf var3;
      try {
         ByteBufUtil.writeUtf8(buffer, string);
         release = false;
         var3 = buffer;
      } finally {
         if (release) {
            buffer.release();
         }
      }

      return var3;
   }

   default ByteBuf stringToByteBuf(CharSequence string, ByteBufAllocator allocator) {
      return stringToByteBufWithExtra(string, allocator,0);
   }
}
