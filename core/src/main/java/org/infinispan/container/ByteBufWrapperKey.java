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
public class ByteBufWrapperKey extends ByteBufWrapper {
   private int hashCode;
   private volatile int refCnt;

   private final Recycler.Handle<ByteBufWrapperKey> handler;

   private static final AtomicIntegerFieldUpdater<ByteBufWrapperKey> refCntUpdater;

   static {
      AtomicIntegerFieldUpdater<ByteBufWrapperKey> updater =
            PlatformDependent.newAtomicIntegerFieldUpdater(ByteBufWrapperKey.class, "refCnt");
      if (updater == null) {
         updater = AtomicIntegerFieldUpdater.newUpdater(ByteBufWrapperKey.class, "refCnt");
      }
      refCntUpdater = updater;
   }

   private static final Recycler<ByteBufWrapperKey> RECYCLER = new Recycler<ByteBufWrapperKey>() {
      @Override
      protected ByteBufWrapperKey newObject(Handle<ByteBufWrapperKey> handle) {
         return new ByteBufWrapperKey(handle);
      }
   };

   ByteBufWrapperKey(Recycler.Handle<ByteBufWrapperKey> handler) {
      this.handler = handler;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) return true;
      // We need to retain this ByteBuf just in case if a concurrent thread removes us while we are checking equality
      // If retain returns null it means we were freed, so we can't be equal
      if (retain() == null) return false;
      try {
         if (obj instanceof ByteBufWrapperKey) {
            ByteBufWrapperKey bbw = (ByteBufWrapperKey) obj;
            if (hashCode != bbw.hashCode()) return false;
            return equalsWrappedBytes((WrappedBytes) obj);
         } else if (obj instanceof WrappedBytes) {
            return equalsWrappedBytes((WrappedBytes) obj);
         }
         return false;
      } finally {
         release();
      }
   }

   @Override
   public int hashCode() {
      return hashCode;
   }

   public static ByteBufWrapperKey newInstance(ByteBuf buffer, int hashCode) {
      ByteBufWrapperKey obj = RECYCLER.get();
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
      ByteBuf buf = buffer;
      buffer = null;
      buf.release();
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
   public int getLength() {
      // Key size is the reader index
      return buffer.readerIndex();
   }

   @Override
   public int getOffset() {
      return 0;
   }

   @Override
   public byte getByte(int offset) {
      return buffer.getByte(offset);
   }
}
