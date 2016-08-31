package org.infinispan.marshall.core;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

/**
 * @author wburns
 * @since 9.0
 */
public class WrappedByteArray implements WrappedBytes, Serializable {
   private final byte[] bytes;
   private transient int hashCode;

   public WrappedByteArray(byte[] bytes) {
      this.bytes = bytes;
      this.hashCode = Arrays.hashCode(bytes);
   }

   private void readObject(java.io.ObjectInputStream stream)
         throws IOException, ClassNotFoundException {
      stream.defaultReadObject();
      hashCode = stream.readInt();
   }

   private void writeObject(java.io.ObjectOutputStream stream)
         throws IOException {
      stream.defaultWriteObject();
      stream.writeInt(hashCode);
   }

   public byte[] getBytes() {
      return bytes;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      WrappedByteArray that = (WrappedByteArray) o;

      return Arrays.equals(bytes, that.bytes);

   }

   @Override
   public int hashCode() {
      return hashCode;
   }
}
