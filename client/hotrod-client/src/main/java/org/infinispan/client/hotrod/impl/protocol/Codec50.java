package org.infinispan.client.hotrod.impl.protocol;

import java.util.concurrent.CompletionStage;

import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;

/**
 * @since 14.0
 */
public class Codec50 extends Codec40 {
   @Override
   public <V> CompletionStage<V> executeCommand(HotRodOperation<V> operation, ChannelFactory factory) {
      // TODO: call into channel factory
      return super.executeCommand(operation, factory);
   }
}
