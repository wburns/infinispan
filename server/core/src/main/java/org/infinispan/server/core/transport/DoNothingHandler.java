package org.infinispan.server.core.transport;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * This handler does nothing and is here solely for the purpose of being in the pipeline so if we have a
 * ByteToMessageDecoder implementation that doesn't normally publish an out value but is required to satisfy
 * https://stackoverflow.com/questions/37535482/netty-disabling-auto-read-doesnt-work-for-bytetomessagedecoder
 * so that we can just ignore the value
 */
@ChannelHandler.Sharable
public class DoNothingHandler extends ChannelInboundHandlerAdapter {
   private DoNothingHandler() {

   }
   public static DoNothingHandler INSTANCE = new DoNothingHandler();
}
