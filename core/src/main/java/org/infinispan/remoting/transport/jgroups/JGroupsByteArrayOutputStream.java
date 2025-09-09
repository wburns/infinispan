package org.infinispan.remoting.transport.jgroups;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.infinispan.protostream.RandomAccessOutputStream;
import org.jgroups.util.ByteArrayDataOutputStream;

/**
 * @author wburns
 * @since 15.0
 */
public class JGroupsByteArrayOutputStream implements RandomAccessOutputStream {
   private final ByteArrayDataOutputStream os;
   private final int offset;

   public JGroupsByteArrayOutputStream(ByteArrayDataOutputStream os) {
      this.os = os;
      this.offset = os.position();
   }

   @Override
   public int getPosition() {
      return os.position() - offset;
   }

   @Override
   public void setPosition(int position) {
      os.position(position + offset);
   }

   @Override
   public void ensureCapacity(int capacity) {
      // TODO: unfortunately is hidden..
//      os.ensureCapacity(capacity);
   }

   @Override
   public ByteBuffer getByteBuffer() {
      return ByteBuffer.wrap(os.buffer(), offset, os.position() - offset);
   }

   @Override
   public byte[] toByteArray() {
      if (os.position() == os.capacity() && offset == 0) {
         return os.buffer();
      }
      return Arrays.copyOfRange(os.buffer(), offset, os.position());
   }

   @Override
   public byte get(int position) {
      return os.buffer()[position + offset];
   }

   @Override
   public void write(int b) throws IOException {
      os.write(b);
   }

   @Override
   public void write(byte[] b) throws IOException {
      os.write(b);
   }

   @Override
   public void write(byte[] b, int off, int len) throws IOException {
      os.write(b, off, len);
   }

   @Override
   public void write(int position, int b) throws IOException {
      int originalPosition = os.position();
      os.position(position + offset);
      os.write(b);
      os.position(originalPosition);
   }

   @Override
   public void write(int position, byte[] b, int off, int len) throws IOException {
      int originalPosition = os.position();
      os.position(position + offset);
      os.write(b, off, len);
      os.position(originalPosition);
   }

   @Override
   public void move(int startPos, int length, int newPos) {
      // TODO: technically this is an issue...
//      os.ensureCapacity(newPos - startPos);
      System.arraycopy(os.buffer(), startPos + offset, os.buffer(), newPos + offset, length);
   }

   @Override
   public void copyTo(DataOutput output) throws IOException {
      output.write(os.buffer(), offset, os.position() - offset);
   }

   @Override
   public void close() throws IOException {
      // No-op
   }
}
