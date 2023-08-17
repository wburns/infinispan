package org.infinispan.marshall.core;

import java.io.IOException;
import java.io.ObjectOutput;

public interface InMemoryObjectOutput extends ObjectOutput {
   @Override
   void writeObject(Object obj) throws IOException;

   @Override
   void write(byte[] b, int off, int len);

   @Override
   void writeByte(int v);

   @Override
   void writeShort(int v);

   @Override
   void writeChar(int v);

   @Override
   void writeInt(int v);

   @Override
   void writeLong(long v);

   @Override
   void writeUTF(String s);

   @Override
   default void write(int b) {
      writeByte(b);
   }

   @Override
   default void write(byte[] b) {
      write(b, 0, b.length);
   }

   @Override
   default void writeBoolean(boolean v) {
      writeByte((byte) (v ? 1 : 0));
   }

   @Override
   default void writeFloat(float v) {
      writeInt(Float.floatToIntBits(v));
   }

   @Override
   default void writeDouble(double v) {
      writeLong(Double.doubleToLongBits(v));
   }

   @Override
   default void writeChars(String s) {
      throw new UnsupportedOperationException("Use writeUTF instead!");
   }

   @Override
   default void writeBytes(String s) {
      throw new UnsupportedOperationException("Use writeUTF instead!");
   }

   @Override
   default void flush() {
      // No-op
   }

   @Override
   default void close() {
      // No-op
   }

   void writeString(String s);
}
