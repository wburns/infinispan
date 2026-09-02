package org.infinispan.util;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * In-memory {@link SoftBPlusTree.NodeStore} used by tests. Counts the number of {@link #write} calls so
 * that tests can make deterministic assertions about how many node writes a sequence of mutations produces.
 */
public class InMemoryNodeStore implements SoftBPlusTree.NodeStore {
   private byte[] data = new byte[4096];
   public int writeCount = 0;

   @Override
   public void write(ByteBuffer buf, long offset) {
      writeCount++;
      int length = buf.remaining();
      ensureCapacity(offset + length);
      buf.get(data, (int) offset, length);
   }

   @Override
   public ByteBuffer read(long offset, int length) {
      byte[] result = new byte[length];
      System.arraycopy(data, (int) offset, result, 0, length);
      return ByteBuffer.wrap(result);
   }

   @Override
   public void truncate(long size) {
      // A real file drops everything at/after size; space that is later regrown reads back as zeros
      // until rewritten. Model that so truncating past a still-referenced block surfaces as corruption
      // rather than returning stale-but-valid bytes.
      if (size < data.length) {
         Arrays.fill(data, (int) size, data.length, (byte) 0);
      }
   }

   private void ensureCapacity(long required) {
      if (required > data.length) {
         int newLen = Math.max((int) required, data.length * 2);
         byte[] newData = new byte[newLen];
         System.arraycopy(data, 0, newData, 0, data.length);
         data = newData;
      }
   }

   public InMemoryNodeStore snapshot() {
      InMemoryNodeStore copy = new InMemoryNodeStore();
      copy.data = this.data.clone();
      return copy;
   }
}
