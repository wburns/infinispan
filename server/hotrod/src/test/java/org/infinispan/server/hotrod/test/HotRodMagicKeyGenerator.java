package org.infinispan.server.hotrod.test;

import java.io.IOException;
import java.util.Random;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.marshall.core.JBossMarshaller;
import org.infinispan.remoting.transport.Address;

/**
 * {@link org.infinispan.distribution.MagicKey} equivalent for HotRod
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
public class HotRodMagicKeyGenerator {

   public static byte[] newKey(Cache<Object, Object> cache) {
      ConsistentHash ch = cache.getAdvancedCache().getDistributionManager().getReadConsistentHash();
      Address nodeAddress = cache.getAdvancedCache().getRpcManager().getAddress();
      Random r = new Random();

      JBossMarshaller sm = new JBossMarshaller();
      for (int i = 0; i < 1000; ++i) {
         String candidate = String.valueOf(r.nextLong());
         byte[] candidateBytes = new byte[0];
         try {
            candidateBytes = sm.objectToByteBuffer(candidate, 64);
         } catch (IOException | InterruptedException e) {
            throw new CacheException(e);
         }
         if (ch.isKeyLocalToNode(nodeAddress, candidateBytes)) {
            return candidateBytes;
         }
      }

      throw new RuntimeException("Unable to find a key local to node " + cache);
   }

   public static String getStringObject(byte[] bytes) {
      JBossMarshaller sm = new JBossMarshaller();
      try {
         return (String) sm.objectFromByteBuffer(bytes);
      } catch (IOException | ClassNotFoundException e) {
         throw new CacheException(e);
      }
   }

}
