package org.infinispan.container.offheap;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import io.netty.buffer.ByteBuf;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.internal.PlatformDependent;

/**
 * @author wburns
 * @since 9.0
 */
class ByteBufWrapper {
   protected final ByteBuf buffer;

   private volatile int refCnt;

   private static final AtomicIntegerFieldUpdater<ByteBufWrapper> refCntUpdater;

   static {
      AtomicIntegerFieldUpdater<ByteBufWrapper> updater =
            PlatformDependent.newAtomicIntegerFieldUpdater(ByteBufWrapper.class, "refCnt");
      if (updater == null) {
         updater = AtomicIntegerFieldUpdater.newUpdater(ByteBufWrapper.class, "refCnt");
      }
      refCntUpdater = updater;
   }

   protected ByteBufWrapper(ByteBuf buffer) {
      this.buffer = buffer;
      refCnt = 1;
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

   public boolean release() {
      for (;;) {
         int refCnt = this.refCnt;
         if (refCnt == 0) {
            throw new IllegalReferenceCountException(0, -1);
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

   public void deallocate() {
      buffer.release();
   }

}
