package org.infinispan.remoting.transport.jgroups;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.infinispan.protostream.RandomAccessOutputStream;
import org.jgroups.util.BaseDataOutputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.PartialOutputStream;

/**
 * @author wburns
 * @since 15.0
 */
public class JGroupsByteArrayOutputStream implements RandomAccessOutputStream {
   private final BaseDataOutputStream os;
   private final ByteArrayDataOutputStream actual;
   private final int offset;

   public JGroupsByteArrayOutputStream(PartialOutputStream os, ByteArrayDataOutputStream actual) {
      this.os = os;
      this.actual = actual;
      this.offset = actual.position();
   }

   public JGroupsByteArrayOutputStream(ByteArrayDataOutputStream noNested) {
      this.os = noNested;
      this.actual = noNested;
      // Position is the offset when the same
      this.offset = 0;
   }

   @Override
   public int getPosition() {
      return os.position();
   }

   @Override
   public void setPosition(int position) {
      os.position(position);
   }

   @Override
   public void ensureCapacity(int capacity) {
      os.ensureCapacity(capacity);
   }

   @Override
   public ByteBuffer getByteBuffer() {
      return ByteBuffer.wrap(actual.buffer(), offset, actual.position() - offset);
   }

   @Override
   public byte[] toByteArray() {
      if (actual.position() == actual.capacity() && offset == 0) {
         return actual.buffer();
      }
      return Arrays.copyOfRange(actual.buffer(), offset, actual.position());
   }

   @Override
   public byte get(int position) {
      return actual.buffer()[position + offset];
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
      os.position(position);
      os.write(b);
      os.position(originalPosition);
   }

   @Override
   public void write(int position, byte[] b, int off, int len) throws IOException {
      int originalPosition = os.position();
      os.position(position);
      os.write(b, off, len);
      os.position(originalPosition);
   }

   @Override
   public void move(int startPos, int length, int newPos) {
      int originalPosition = os.position();
      int moveAmount = newPos - startPos;
      // This can't work with Partial...
      ensureCapacity(moveAmount);
      os.position(newPos);
      os.write(actual.buffer(), startPos + offset, length);
      os.position(originalPosition);
   }

   @Override
   public void copyTo(DataOutput output) throws IOException {
      // TODO: is this correct?
      output.write(actual.buffer(), offset, os.position() - offset);
   }

   @Override
   public void close() throws IOException {
      // No-op
   }
}
