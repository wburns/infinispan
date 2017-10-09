package org.infinispan.container.offheap;

import org.infinispan.commons.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import sun.misc.Unsafe;

/**
 * Simple wrapper around Unsafe to provide for trace messages for method calls.
 * @author wburns
 * @since 9.0
 */
public class UnsafeWrapper {
   protected static final Log log = LogFactory.getLog(UnsafeWrapper.class);
   protected static final boolean trace = log.isTraceEnabled();

   protected static final Unsafe UNSAFE = UnsafeHolder.UNSAFE;

   static final UnsafeWrapper INSTANCE = new UnsafeWrapper();

   private UnsafeWrapper() { }

   public void putLong(long address, long value) {
      if (trace) {
         log.tracef("Wrote long value %d to address %d", value, address);
      }
      UNSAFE.putLong(address, value);
   }

   public void putInt(long address, int value) {
      if (trace) {
         log.tracef("Wrote int value %d to address %d", value, address);
      }
      UNSAFE.putInt(address, value);
   }

   public long getLong(long address) {
      long value = UNSAFE.getLong(address);
      if (trace) {
         log.tracef("Retrieved long value %d from address %d", value, address);
      }
      return value;
   }

   public int getInt(long address) {
      int value = UNSAFE.getInt(address);
      if (trace) {
         log.tracef("Retrieved int value %d from address %d", value, address);
      }
      return value;
   }

   public int arrayBaseOffset(Class<?> arrayClass) {
      return UNSAFE.arrayBaseOffset(arrayClass);
   }

   public void copyMemory(Object srcBase, long srcOffset, Object destBase, long destOffset, long bytes) {
      if (trace) {
         log.tracef("Copying memory of object %s offset by %d to %s offset by %d with a total of %d bytes",
               printable(srcBase), srcOffset, printable(destBase), destOffset, bytes);
      }
      UNSAFE.copyMemory(srcBase, srcOffset, destBase, destOffset, bytes);
   }

   static Object printable(Object obj) {
      if (obj instanceof byte[]) {
         return Util.printArray((byte[]) obj);
      }

      return obj;
   }
}
