package org.infinispan.server.hotrod;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import org.infinispan.server.core.transport.ExtendedByteBufJava;

/**
 * // TODO: Document this
 *
 * @author wburns
 * @since 4.0
 */
public class Decoder2xJava {
   static boolean readHeader(ByteBuf buffer, byte version, long messageId, HotRodHeader header) throws HotRodUnknownOperationException {
      if (header.op() == null) {
         int readableBytes = buffer.readableBytes();
         // We require at least 2 bytes at minimum
         if (readableBytes < 2) {
            return false;
         }
         byte streamOp = buffer.readByte();
         int length = ExtendedByteBufJava.readMaybeVInt(buffer);
         // Didn't have enough bytes for VInt or the length is too long for remaining
         if (length == Integer.MIN_VALUE || length > buffer.readableBytes()) {
            return false;
         } else if (length == 0) {
            header.cacheName_$eq("");
         } else {
            byte[] bytes = new byte[length];
            buffer.readBytes(bytes);
            header.cacheName_$eq(new String(bytes, CharsetUtil.UTF_8));
         }
         switch (streamOp) {
            case 0x01:
               header.op_$eq(HotRodOperation.PutRequest);
               break;
            case 0x03:
               header.op_$eq(HotRodOperation.GetRequest);
               break;
            case 0x05:
               header.op_$eq(HotRodOperation.PutIfAbsentRequest);
               break;
            case 0x07:
               header.op_$eq(HotRodOperation.ReplaceRequest);
               break;
            case 0x09:
               header.op_$eq(HotRodOperation.ReplaceIfUnmodifiedRequest);
               break;
            case 0x0B:
               header.op_$eq(HotRodOperation.RemoveRequest);
               break;
            case 0x0D:
               header.op_$eq(HotRodOperation.RemoveIfUnmodifiedRequest);
               break;
            case 0x0F:
               header.op_$eq(HotRodOperation.ContainsKeyRequest);
               break;
            case 0x11:
               header.op_$eq(HotRodOperation.GetWithVersionRequest);
               break;
            case 0x13:
               header.op_$eq(HotRodOperation.ClearRequest);
               break;
            case 0x15:
               header.op_$eq(HotRodOperation.StatsRequest);
               break;
            case 0x17:
               header.op_$eq(HotRodOperation.PingRequest);
               break;
            case 0x19:
               header.op_$eq(HotRodOperation.BulkGetRequest);
               break;
            case 0x1B:
               header.op_$eq(HotRodOperation.GetWithMetadataRequest);
               break;
            case 0x1D:
               header.op_$eq(HotRodOperation.BulkGetKeysRequest);
               break;
            case 0x1F:
               header.op_$eq(HotRodOperation.QueryRequest);
               break;
            case 0x21:
               header.op_$eq(HotRodOperation.AuthMechListRequest);
               break;
            case 0x23:
               header.op_$eq(HotRodOperation.AuthRequest);
               break;
            case 0x25:
               header.op_$eq(HotRodOperation.AddClientListenerRequest);
               break;
            case 0x27:
               header.op_$eq(HotRodOperation.RemoveClientListenerRequest);
               break;
            case 0x29:
               header.op_$eq(HotRodOperation.SizeRequest);
               break;
            case 0x2B:
               header.op_$eq(HotRodOperation.ExecRequest);
               break;
            case 0x2D:
               header.op_$eq(HotRodOperation.PutAllRequest);
               break;
            case 0x2F:
               header.op_$eq(HotRodOperation.GetAllRequest);
               break;
            case 0x31:
               header.op_$eq(HotRodOperation.IterationStartRequest);
               break;
            case 0x33:
               header.op_$eq(HotRodOperation.IterationNextRequest);
               break;
            case 0x35:
               header.op_$eq(HotRodOperation.IterationEndRequest);
               break;
            default:
               throw new HotRodUnknownOperationException(
                    "Unknown operation: " + streamOp, version, messageId);
         }
         buffer.markReaderIndex();
      }
      int flag = ExtendedByteBufJava.readMaybeVInt(buffer);
      if (flag == Integer.MIN_VALUE) {
         return false;
      }
      if (buffer.readableBytes() < 2) {
         return false;
      }
      byte clientIntelligence = buffer.readByte();
      int topologyId = ExtendedByteBufJava.readMaybeVInt(buffer);
      if (topologyId == Integer.MIN_VALUE) {
         return false;
      }
      header.flag_$eq(flag);
      header.clientIntel_$eq(clientIntelligence);
      header.topologyId_$eq(topologyId);

      buffer.markReaderIndex();
      return true;
   }
}
