package org.infinispan.server.core;

import io.netty.buffer.ByteBuf;

import java.util.OptionalInt;
import java.util.OptionalLong;

/**
 * Static helper class to provide unsigned numeric methods when using a {@link ByteBuf}.
 *
 * @author wburns
 * @since 9.0
 */
public class UnsignedNumeric {
   /**
    * Reads an int stored in variable-length format.  Reads between one and five bytes.  Smaller values take fewer
    * bytes.  Negative numbers are not supported.
    */
   public static int readUnsignedInt(ByteBuf in) {
      byte b = in.readByte();
      int i = b & 0x7F;
      for (int shift = 7; (b & 0x80) != 0; shift += 7) {
         b = in.readByte();
         i |= (b & 0x7FL) << shift;
      }
      return i;
   }

   /**
    * Same as {@link #readUnsignedInt(ByteBuf)} except that it returns an int that can be used
    * when the byte buf is not used in blocking mode.  If a -1 is returned the reader index will be reset.
    * @param in the byte buf to read from
    * @return anb int value showing whether or not it could be read - if the value is -1 there were not enough bytes
    */
   public static int readOptionalUnsignedInt(ByteBuf in) {
      if (in.readableBytes() < 1) {
         in.resetReaderIndex();
         return -1;
      }
      byte b = in.readByte();
      int i = b & 0x7F;
      for (int shift = 7; (b & 0x80) != 0; shift += 7) {
         if (in.readableBytes() < 1) {
            in.resetReaderIndex();
            return -1;
         }
         b = in.readByte();
         i |= (b & 0x7FL) << shift;
      }
      return i;
   }

   /**
    * Reads a long stored in variable-length format.  Reads between one and nine bytes.  Smaller values take fewer
    * bytes.  Negative numbers are not supported.
    */
   public static long readUnsignedLong(ByteBuf in) {
      byte b = in.readByte();
      long i = b & 0x7F;
      for (int shift = 7; (b & 0x80) != 0; shift += 7) {
         b = in.readByte();
         i |= (b & 0x7FL) << shift;
      }
      return i;
   }

   /**
    * Same as {@link #readUnsignedLong(ByteBuf)} except that it returns a long that
    * can be used when the byte buf is not used in blocking mode.  If a -1 is returned the reader index will
    * be reset.
    * @param in the byte buf to read from
    * @return a long value showing whether or not it could be read - if the value is -1 there were not enough bytes
    */
   public static long readOptionalUnsignedLong(ByteBuf in) {
      if (in.readableBytes() < 1) {
         in.resetReaderIndex();
         return -1;
      }
      byte b = in.readByte();
      long i = b & 0x7F;
      for (int shift = 7; (b & 0x80) != 0; shift += 7) {
         if (in.readableBytes() < 1) {
            in.resetReaderIndex();
            return -1;
         }
         b = in.readByte();
         i |= (b & 0x7FL) << shift;
      }
      return i;
   }
}
