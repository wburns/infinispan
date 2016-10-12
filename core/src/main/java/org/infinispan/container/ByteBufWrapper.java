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
class ByteBufWrapper implements WrappedBytes {
   private ByteBuf buffer;
   private int hashCode;
   private volatile int refCnt;
   private final Recycler.Handle<ByteBufWrapper> handler;

   private static final AtomicIntegerFieldUpdater<ByteBufWrapper> refCntUpdater;

   static {
      AtomicIntegerFieldUpdater<ByteBufWrapper> updater =
            PlatformDependent.newAtomicIntegerFieldUpdater(ByteBufWrapper.class, "refCnt");
      if (updater == null) {
         updater = AtomicIntegerFieldUpdater.newUpdater(ByteBufWrapper.class, "refCnt");
      }
      refCntUpdater = updater;
   }

   private static final Recycler<ByteBufWrapper> RECYCLER = new Recycler<ByteBufWrapper>() {
      @Override
      protected ByteBufWrapper newObject(Handle<ByteBufWrapper> handle) {
         return new ByteBufWrapper(handle);
      }
   };

   ByteBufWrapper(Recycler.Handle<ByteBufWrapper> handler) {
      this.handler = handler;
   }

   public static ByteBufWrapper newInstance(ByteBuf buffer, int hashCode) {
      ByteBufWrapper obj = RECYCLER.get();
      obj.buffer = buffer;
      obj.hashCode = hashCode;
      refCntUpdater.set(obj, 1);
      return obj;
   }

   public ByteBufWrapper retain() {
      for (;;) {
         int refCnt = this.refCnt;
         final int nextCnt = refCnt + 1;

         // Ensure we not resurrect (which means the refCnt was 0) and also that we encountered an overflow.
         if (nextCnt <= 1) {
            return null;
         }
         if (refCntUpdater.compareAndSet(this, refCnt, nextCnt)) {
            break;
         }
      }
      return this;
   }

   public void deallocate() {
      buffer.release();
      buffer = null;
      hashCode = 0;
      handler.recycle(this);
   }

   public boolean release() {
      for (;;) {
         int refCnt = this.refCnt;
         if (refCnt == 0) {
            return true;
         }

         if (refCntUpdater.compareAndSet(this, refCnt, refCnt - 1)) {
            if (refCnt == 1) {
               deallocate();
               return true;
            }
            return false;
         }
      }
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj instanceof WrappedByteArray) {
         WrappedByteArray baw = (WrappedByteArray) obj;
         if (hashCode != baw.getHashCode()) return false;
         return equals(baw.getBytes(), buffer);
      } else if (obj instanceof ByteBufWrapper) {
         ByteBufWrapper bbw = (ByteBufWrapper) obj;
         if (hashCode != bbw.hashCode) return false;
         return buffer.equals(bbw.buffer);
      }
      return super.equals(obj);
   }

   static private boolean equals(byte[] bytes, ByteBuf buf) {
      if (bytes.length != buf.readableBytes()) return false;
      for (int i = 0; i < bytes.length; i++)
         if (bytes[i] != buf.getByte(i))
            return false;
      return true;
   }

   public ByteBuf getBuffer() {
      return buffer;
   }

   @Override
   public int hashCode() {
      return hashCode;
   }

   @Override
   public byte[] getBytes() {
      return new byte[0];
   }

   @Override
   public boolean bytesEqual(WrappedBytes other) {
      byte[] bytes = other.getBytes();
      return equals(bytes, buffer);
   }
}
