package org.infinispan.marshall.core;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInput;

public interface InMemoryObjectInput extends ObjectInput {
   @Override
   int read();

   @Override
   default int read(byte[] b) {
      return read(b, 0, b.length);
   }

   @Override
   int read(byte[] b, int off, int len);

   @Override
   long skip(long n);

   @Override
   int available();

   @Override
   default void readFully(byte[] b) throws IOException {
      readFully(b, 0, b.length);
   }

   @Override
   void readFully(byte[] b, int off, int len) throws IOException;

   @Override
   int skipBytes(int n);

   @Override
   byte readByte() throws EOFException;

   @Override
   default int readUnsignedByte() throws EOFException {
      return readByte() & 0xff;
   }

   @Override
   short readShort() throws EOFException;

   @Override
   default int readUnsignedShort() throws EOFException {
      return readShort() & 0xffff;
   }

   @Override
   char readChar() throws EOFException;

   @Override
   int readInt() throws EOFException;

   @Override
   long readLong() throws EOFException;

   @Override
   default String readLine() {
      throw new UnsupportedOperationException("readLine not supported!");
   }

   @Override
   String readUTF() throws EOFException;

   @Override
   default void close() {
      // No-op
   }

   @Override
   default boolean readBoolean() throws EOFException {
      return readByte() != 0;
   }

   @Override
   default float readFloat() throws EOFException {
      return Float.intBitsToFloat(readInt());
   }

   @Override
   default double readDouble() throws EOFException {
      return Double.longBitsToDouble(readLong());
   }

   String readString() throws EOFException;
}
