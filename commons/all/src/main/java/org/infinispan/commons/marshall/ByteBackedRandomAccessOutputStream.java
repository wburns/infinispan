package org.infinispan.commons.marshall;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.infinispan.protostream.impl.RandomAccessOutputStreamImpl;

public class ByteBackedRandomAccessOutputStream extends RandomAccessOutputStreamImpl {
   protected int offset;

   public ByteBackedRandomAccessOutputStream(byte[] buf, int offset) {
      this.buf = buf;
      this.offset = offset;
      this.pos = offset;
   }

   @Override
   public void ensureCapacity(int capacity) {
      // We don't do anything as we only have the byte[] given and its size should be guaranteed to be enough
   }

   @Override
   public ByteBuffer getByteBuffer() {
      return ByteBuffer.wrap(buf, offset, pos - offset);
   }

   @Override
   public byte[] toByteArray() {
      return Arrays.copyOfRange(buf, offset, pos);
   }

   @Override
   public void copyTo(DataOutput output) throws IOException {
      output.write(buf, offset, pos - offset);
   }

   @Override
   public void close() {
      // No-op
   }
}
