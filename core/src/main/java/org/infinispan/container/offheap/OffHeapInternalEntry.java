package org.infinispan.container.offheap;

import org.infinispan.commons.marshall.WrappedBytes;

/**
 * Hacky in that equality is only by the key.  This is because we don't ever do comparisons by value in off heap
 * @author wburns
 * @since 9.0
 */
public class OffHeapInternalEntry implements WrappedBytes {
   // hashCode of key
   private final int hashCode;

   // Normally this would be volatile, but it is only ever reset in a compute block which has a lock to ensure
   // volatile write
   private ByteBufWrapper buffer;

   public OffHeapInternalEntry(int hashCode, ByteBufWrapper buffer) {
      this.hashCode = hashCode;
      this.buffer = buffer;
   }

   @Override
   public boolean equals(Object obj) {
      if (obj == this) return true;
      if (obj.getClass().isAssignableFrom(WrappedBytes.class)) {
         return equalsWrappedBytes((WrappedBytes) obj);
      }
      return false;
   }

   @Override
   public int hashCode() {
      return hashCode;
   }

   @Override
   public byte[] getBytes() {
      return null;
   }

   @Override
   public int backArrayOffset() {
      return -1;
   }

   @Override
   public int getLength() {
      return buffer.buffer.readerIndex();
   }

   @Override
   public byte getByte(int offset) {
      return buffer.buffer.getByte(offset);
   }

   public ByteBufWrapper getBuffer() {
      return buffer;
   }

   public ByteBufWrapper changeByteBufWrapper(ByteBufWrapper newByteBufWrapper) {
      ByteBufWrapper prev = this.buffer;
      this.buffer = newByteBufWrapper;
      return prev;
   }
}
