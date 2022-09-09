package org.infinispan.server.resp;

import java.util.List;

import io.netty.channel.ChannelHandlerContext;

public class RespAuthHandler implements RespRequestHandler {
   @Override
   public RespRequestHandler handleRequest(ChannelHandlerContext ctx, String type, List<byte[]> arguments) {

      return RespRequestHandler.super.handleRequest(ctx, type, arguments);
   }
}
