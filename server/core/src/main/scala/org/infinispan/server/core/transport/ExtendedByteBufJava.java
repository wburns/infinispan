package org.infinispan.server.core.transport;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import org.infinispan.commons.io.SignedNumeric;

/**
 * // TODO: Document this
 *
 * @author wburns
 * @since 4.0
 */
public class ExtendedByteBufJava {

   public static int readUnsignedMaybeInt(ByteBuf buf) {
      if (buf.readableBytes() < 4) {
         buf.resetReaderIndex();
         return Integer.MIN_VALUE;
      }
      return buf.readInt();
   }

   public static long readUnsignedMaybeLong(ByteBuf buf) {
      if (buf.readableBytes() < 8) {
         buf.resetReaderIndex();
         return Long.MIN_VALUE;
      }
      return buf.readLong();
   }

   public static long readMaybeVLong(ByteBuf buf) {
      if (buf.readableBytes() > 0) {
         byte b = buf.readByte();
         long i = b & 0x7F;
         for (int shift = 7; (b & 0x80) != 0; shift += 7) {
            if (buf.readableBytes() == 0) {
               buf.resetReaderIndex();
               return Long.MIN_VALUE;
            }
            b = buf.readByte();
            i |= (b & 0x7FL) << shift;
         }
         return i;
      } else {
         buf.resetReaderIndex();
         return Long.MIN_VALUE;
      }
   }

   public static int readMaybeVInt(ByteBuf buf) {
      if (buf.readableBytes() > 0) {
         byte b = buf.readByte();
         int i = b & 0x7F;
         for (int shift = 7; (b & 0x80) != 0; shift += 7) {
            if (buf.readableBytes() == 0) {
               buf.resetReaderIndex();
               return Integer.MIN_VALUE;
            }
            b = buf.readByte();
            i |= (b & 0x7FL) << shift;
         }
         return i;
      } else {
         buf.resetReaderIndex();
         return Integer.MIN_VALUE;
      }
   }

   public static int readMaybeSignedInt(ByteBuf buf) {
      int maybeVInt = readMaybeVInt(buf);
      if (maybeVInt == Integer.MIN_VALUE) {
         return Integer.MIN_VALUE;
      } else {
         return SignedNumeric.decode(maybeVInt);
      }
   }

   public static byte[] readMaybeRangedBytes(ByteBuf bf) {
      int length = readMaybeVInt(bf);
      if (length == Integer.MIN_VALUE) {
         return null;
      }
      return readMaybeRangedBytes(bf, length);
   }

   public static byte[] readMaybeRangedBytes(ByteBuf bf, int length) {
      if (bf.readableBytes() < length) {
         bf.resetReaderIndex();
         return null;
      } else {
         byte[] bytes = new byte[length];
         bf.readBytes(bytes);
         return bytes;
      }
   }

   public static String readMaybeString(ByteBuf bf) {
      byte[] bytes = readMaybeRangedBytes(bf);
      if (bytes != null) {
         if (bytes.length == 0) {
            return "";
         } else {
            return new String(bytes, CharsetUtil.UTF_8);
         }
      } else {
         return null;
      }
   }
}
