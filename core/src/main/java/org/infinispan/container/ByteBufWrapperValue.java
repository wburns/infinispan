package org.infinispan.container;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.infinispan.commons.marshall.WrappedBytes;

import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.internal.PlatformDependent;

/**
 * @author wburns
 * @since 9.0
 */
public class ByteBufWrapperValue extends ByteBufWrapper {
   private final Recycler.Handle<ByteBufWrapperValue> handler;

   private static final Recycler<ByteBufWrapperValue> RECYCLER = new Recycler<ByteBufWrapperValue>() {
      @Override
      protected ByteBufWrapperValue newObject(Handle<ByteBufWrapperValue> handle) {
         return new ByteBufWrapperValue(handle);
      }
   };

   ByteBufWrapperValue(Recycler.Handle<ByteBufWrapperValue> handler) {
      this.handler = handler;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj instanceof ByteBufWrapperValue) {
         ByteBufWrapperValue bbw = (ByteBufWrapperValue) obj;
         return buffer.equals(bbw.buffer);
      } else if (obj instanceof WrappedBytes) {
         return equalsWrappedBytes((WrappedBytes) obj);
      }
      return false;
   }

   public static ByteBufWrapperValue newInstance(ByteBuf buffer) {
      ByteBufWrapperValue obj = RECYCLER.get();
      obj.buffer = buffer;
      return obj;
   }

   @Override
   public int getLength() {
      return buffer.capacity() - buffer.readerIndex();
   }

   @Override
   public int getOffset() {
      return buffer.readerIndex();
   }

   @Override
   public byte getByte(int offset) {
      return buffer.getByte(buffer.readerIndex() + offset);
   }
}
