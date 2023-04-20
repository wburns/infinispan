package org.infinispan.marshall.core;

import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;

public class ByteBufObjectInput implements InMemoryObjectInput {
   private final ByteBuf buf;
   private final GlobalMarshaller marshaller;

   public ByteBufObjectInput(ByteBuf buf, GlobalMarshaller marshaller) {
      this.buf = buf;
      this.marshaller = marshaller;
   }

   @Override
   public Object readObject() throws ClassNotFoundException, IOException {
      return marshaller.readNullableObject(this);
   }

   @Override
   public int read() {
      int available = available();
      if (available == 0) {
         return -1;
      }
      return buf.readByte() & 0xff;
   }

   @Override
   public int read(byte[] b, int off, int len) {
      int available = available();
      if (available == 0) {
         return -1;
      }

      len = Math.min(available, len);
      buf.readBytes(b, off, len);
      return len;
   }

   @Override
   public long skip(long n) {
      if (n > Integer.MAX_VALUE) {
         return skipBytes(Integer.MAX_VALUE);
      } else {
         return skipBytes((int) n);
      }
   }

   @Override
   public int skipBytes(int n) {
      int nBytes = Math.min(available(), n);
      buf.skipBytes(nBytes);
      return nBytes;
   }

   @Override
   public int available() {
      return buf.readableBytes();
   }

   @Override
   public void readFully(byte[] b, int off, int len) throws IOException {
      checkAvailable(len);
      buf.readBytes(b, off, len);
   }

   @Override
   public byte readByte() throws EOFException {
      int available = available();
      if (available == 0) {
         throw new EOFException();
      }
      return buf.readByte();
   }

   @Override
   public short readShort() throws EOFException {
      checkAvailable(2);
      return buf.readShort();
   }

   @Override
   public char readChar() throws EOFException {
      return (char) readShort();
   }

   @Override
   public int readInt() throws EOFException {
      checkAvailable(4);
      return buf.readInt();
   }

   @Override
   public long readLong() throws EOFException {
      checkAvailable(8);
      return buf.readLong();
   }

   @Override
   public String readUTF() throws EOFException {
      int utflen = readInt();

      String strToReturn = buf.toString(buf.readerIndex(), utflen, StandardCharsets.UTF_8);
      buf.skipBytes(utflen);
      return strToReturn;
   }

   @Override
   public String readString() throws EOFException {
      int mark = readByte();

      switch(mark) {
         case 0:
            return ""; // empty string
         case 1:
            // small ascii
            int size = readByte();
            String strToReturn = buf.toString(buf.readerIndex(), size, StandardCharsets.US_ASCII);
            buf.skipBytes(size);
            return strToReturn;
         case 2:
            // large string
            return readUTF();
         default:
            throw new RuntimeException("Unknown marker(String). mark=" + mark);
      }
   }

   private void checkAvailable(int fieldSize) throws EOFException {
      if (fieldSize < 0) {
         throw new IndexOutOfBoundsException("fieldSize cannot be a negative number");
      }
      if (fieldSize > available()) {
         throw new EOFException("fieldSize is too long! Length is " + fieldSize
               + ", but maximum is " + available());
      }
   }
}
