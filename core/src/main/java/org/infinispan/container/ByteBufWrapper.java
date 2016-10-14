package org.infinispan.container;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.WrappedBytes;

import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.Recycler;
import io.netty.util.internal.PlatformDependent;

/**
 * @author wburns
 * @since 9.0
 */
abstract class ByteBufWrapper implements WrappedBytes {
   protected ByteBuf buffer;

   static boolean equals(byte[] bytes, ByteBuf buf, int offset, int length) {
      if (bytes.length != length) return false;
      for (int i = 0; i < length; i++)
         if (bytes[i] != buf.getByte(i + offset))
            return false;
      return true;
   }

   public ByteBuf getBuffer() {
      return buffer;
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
   public abstract int getLength();

   public abstract int getOffset();
}
