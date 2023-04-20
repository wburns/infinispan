package org.infinispan.marshall.core;

import java.io.IOException;
import java.io.ObjectOutput;
import java.io.OutputStream;

import org.infinispan.commons.marshall.Marshaller;

public class OutputStreamToObjectOutput implements ObjectOutput {
   private final OutputStream outputStream;
   private final Marshaller marshaller;

   public OutputStreamToObjectOutput(OutputStream outputStream, Marshaller marshaller) {
      this.outputStream = outputStream;
      this.marshaller = marshaller;
   }

   @Override
   public void writeObject(Object obj) throws IOException {
   }

   @Override
   public void write(int b) throws IOException {

   }

   @Override
   public void write(byte[] b) throws IOException {

   }

   @Override
   public void write(byte[] b, int off, int len) throws IOException {

   }

   @Override
   public void writeBoolean(boolean v) throws IOException {

   }

   @Override
   public void writeByte(int v) throws IOException {

   }

   @Override
   public void writeShort(int v) throws IOException {

   }

   @Override
   public void writeChar(int v) throws IOException {

   }

   @Override
   public void writeInt(int v) throws IOException {

   }

   @Override
   public void writeLong(long v) throws IOException {

   }

   @Override
   public void writeFloat(float v) throws IOException {

   }

   @Override
   public void writeDouble(double v) throws IOException {

   }

   @Override
   public void writeBytes(String s) throws IOException {

   }

   @Override
   public void writeChars(String s) throws IOException {

   }

   @Override
   public void writeUTF(String s) throws IOException {

   }

   @Override
   public void flush() throws IOException {

   }

   @Override
   public void close() throws IOException {

   }
}
