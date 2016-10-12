package org.infinispan.commons.marshall;

/**
 * @author wburns
 * @since 9.0
 */
public interface WrappedBytes {
   byte[] getBytes();

   /**
    * Equality method provided to allow for various implementations to be equal to each other without having to know
    * about other implementations.
    * @param other
    * @return
    */
   boolean bytesEqual(WrappedBytes other);
}
