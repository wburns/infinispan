package org.infinispan.server.resp;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import io.netty.util.ByteProcessor;

public class Intrinsics {
   private static final int TERMINATOR_LENGTH = 2;

   public static byte singleByte(ByteBuf buffer) {
      if (buffer.isReadable()) {
         return buffer.readByte();
      } else return 0;
   }

   public static String simpleString(ByteBuf buf) {
      // Find the end LF (would be nice to do this without iterating twice)
      int offset = buf.forEachByte(ByteProcessor.FIND_LF);
      if (offset <= 0 || buf.getByte(offset - 1) != '\r') {
         return null;
      }
      String simpleString = buf.toString(buf.readerIndex(), offset - 1, StandardCharsets.US_ASCII);
      // toString doesn't move the reader index forward
      buf.skipBytes(offset + TERMINATOR_LENGTH);
      return simpleString;
   }

   public static long readNumber(ByteBuf buf, Resp2LongProcessor longProcessor) {
      long value = longProcessor.getValue(buf);
      if (longProcessor.complete) {
         buf.skipBytes(longProcessor.bytesRead + TERMINATOR_LENGTH);
      }
      return value;
   }

   public static String bulkString(ByteBuf buf, Resp2LongProcessor longProcessor) {
      buf.markReaderIndex();
      long longSize = readNumber(buf, longProcessor);
      if (longSize > Integer.MAX_VALUE) {
         throw new IllegalArgumentException("Bytes cannot be longer than " + Integer.MAX_VALUE);
      }
      if (longSize == Long.MIN_VALUE) {
         return null;
      }
      int size = (int) longSize;
      if (buf.readableBytes() < size + TERMINATOR_LENGTH) {
         buf.resetReaderIndex();
         return null;
      }
      String stringValue = buf.toString(buf.readerIndex(), size, StandardCharsets.US_ASCII);
      buf.skipBytes(size + TERMINATOR_LENGTH);
      return stringValue;
   }

   public static byte[] bulkArray(ByteBuf buf, Resp2LongProcessor longProcessor) {
      buf.markReaderIndex();
      long longSize = readNumber(buf, longProcessor);
      if (longSize > Integer.MAX_VALUE) {
         throw new IllegalArgumentException("Bytes cannot be longer than " + Integer.MAX_VALUE);
      }
      if (longSize == Long.MIN_VALUE) {
         return null;
      }
      int size = (int) longSize;
      if (buf.readableBytes() < size + TERMINATOR_LENGTH) {
         buf.resetReaderIndex();
         return null;
      }
      byte[] array = new byte[size];
      buf.readBytes(array);
      buf.skipBytes(TERMINATOR_LENGTH);
      return array;
   }

   static class Resp2LongProcessor implements ByteProcessor {
      long result;
      int bytesRead;
      boolean complete;
      boolean negative;
      boolean first;

      public long getValue(ByteBuf buffer) {

         this.result = 0;
         this.bytesRead = 0;
         this.complete = false;
         this.first = true;

         // We didn't have enough to read the number
         if (buffer.forEachByte(this) == -1) {
            complete = false;
            return -1;
         }
         complete = true;

         if (!this.negative) {
            this.result = -this.result;
         }

         return this.result;
      }

      @Override
      public boolean process(byte value) {

         if (value == '\r') {
            return false;
         }

         bytesRead++;

         if (first) {
            first = false;

            if (value == '-') {
               negative = true;
            } else {
               negative = false;
               int digit = value - '0';
               result = result * 10 - digit;
            }
            return true;
         }

         int digit = value - '0';
         result = result * 10 - digit;

         return true;
      }
   }
}
