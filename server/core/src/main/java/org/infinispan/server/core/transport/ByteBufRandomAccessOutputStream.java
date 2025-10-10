package org.infinispan.server.core.transport;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.infinispan.protostream.RandomAccessOutputStream;

import io.netty.buffer.ByteBuf;

public class ByteBufRandomAccessOutputStream implements RandomAccessOutputStream {
   private final ByteBuf buf;

   public ByteBufRandomAccessOutputStream(ByteBuf buf) {
      this.buf = buf;
   }

   @Override
   public int getPosition() {
      return buf.writerIndex();
   }

   @Override
   public void setPosition(int position) {
      buf.writerIndex(position);
   }

   @Override
   public void ensureCapacity(int capacity) {
      buf.ensureWritable(capacity);
   }

   @Override
   public ByteBuffer getByteBuffer() {
      return buf.nioBuffer();
   }

   @Override
   public byte[] toByteArray() {
      byte[] bytes = new byte[buf.readableBytes()];
      buf.getBytes(buf.readerIndex(), bytes);
      return bytes;
   }

   @Override
   public byte get(int position) {
      return buf.getByte(position);
   }

   @Override
   public void write(int position, int b) {
      buf.setByte(position, b);
   }

   @Override
   public void write(int position, byte[] b, int off, int len) {
      buf.setBytes(position, b, off, len);
   }

   @Override
   public void move(int startPos, int length, int newPos) {
      buf.setBytes(newPos, buf, startPos, length);
   }

   @Override
   public void copyTo(DataOutput output) throws IOException {
      if (buf.hasArray()) {
         output.write(buf.array(), buf.readerIndex(), buf.readableBytes());
      } else {
         buf.forEachByte(b -> {
            output.write(b);
            return true;
         });
      }
   }

   @Override
   public void close() throws IOException {
      buf.release();
   }
}
