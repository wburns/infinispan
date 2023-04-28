package org.infinispan.marshall.core;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

public class ByteBufObjectOutput implements InMemoryObjectOutput {
   private final ByteBuf buf;
   private final GlobalMarshaller marshaller;

   public ByteBufObjectOutput(ByteBuf buf, GlobalMarshaller marshaller) {
      this.buf = buf;
      this.marshaller = marshaller;
   }

   public ByteBuf getBuf() {
      return buf;
   }

   @Override
   public void writeObject(Object obj) throws IOException {
      marshaller.writeNullableObject(obj, this);
   }

   @Override
   public void writeByte(int v) {
      buf.writeByte(v);
   }

   @Override
   public void write(byte[] b, int off, int len) {
      buf.writeBytes(b, off, len);
   }

   @Override
   public void writeShort(int v) {
      buf.writeShort(v);
   }

   @Override
   public void writeChar(int v) {
      buf.writeChar(v);
   }

   @Override
   public void writeInt(int v) {
      buf.writeInt(v);
   }

   @Override
   public void writeLong(long v) {
      buf.writeLong(v);
   }

   @Override
   public void writeUTF(String s) {
      ByteBufUtil.writeUtf8(buf, s);
   }

   @Override
   public void writeString(String s) {
      int len;
      if ((len = s.length()) == 0){
         writeByte(0); // empty string
      } else if (isAscii(s, len)) {
         writeByte(1); // small ascii
         writeByte(len);
         ByteBufUtil.writeAscii(buf, s);
      } else {
         writeByte(2);  // large string
         writeInt(len);
         writeUTF(s);
      }
   }

   private boolean isAscii(String s, int len) {
      boolean ascii = false;
      if(len < 64) {
         ascii = true;
         for (int i = 0; i < len; i++) {
            if (s.charAt(i) > 127) {
               ascii = false;
               break;
            }
         }
      }
      return ascii;
   }
}
