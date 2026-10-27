package org.infinispan.client.hotrod.impl.transport.netty;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

/**
 * This class handles storage for unresolved address.
 */
public class ChannelRecord {
   static AttributeKey<SocketAddress> KEY = AttributeKey.newInstance("unresolved-address");

   public static SocketAddress of(Channel channel) {
      if (channel == null) {
         return null;
      }
      return channel.attr(KEY).get();
   }

   public static void set(Channel channel, SocketAddress unresolvedAddress) {
      assert !(unresolvedAddress instanceof InetSocketAddress) || ((InetSocketAddress) unresolvedAddress).isUnresolved();
      channel.attr(KEY).set(unresolvedAddress);
   }

   public static boolean sameAddress(SocketAddress a1, SocketAddress a2) {
      if (Objects.equals(a1, a2)) {
         return true;
      }
      if (a1 instanceof InetSocketAddress isa1 && a2 instanceof InetSocketAddress isa2) {
         if (isa1.getPort() != isa2.getPort()) {
            return false;
         }
         if (Objects.equals(isa1.getHostString(), isa2.getHostString())) {
            return true;
         }
         if (isa1.getAddress() != null && isa2.getAddress() != null) {
            return Objects.equals(isa1.getAddress(), isa2.getAddress());
         }
         if (isa1.getAddress() != null && Objects.equals(isa1.getAddress().getHostAddress(), isa2.getHostString())) {
            return true;
         }
         return isa2.getAddress() != null && Objects.equals(isa2.getAddress().getHostAddress(), isa1.getHostString());
      }
      return false;
   }
}
