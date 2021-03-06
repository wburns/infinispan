package org.infinispan.marshall.protostream.impl;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A wrapper message used by ProtoStream Marshallers to wrap bytes generated by a non-protostream user marshaller.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
@ProtoTypeId(ProtoStreamTypeIds.USER_MARSHALLER_BYTES)
public class UserMarshallerBytes {

   @ProtoField(number = 1)
   final byte[] bytes;

   @ProtoFactory
   public UserMarshallerBytes(byte[] bytes) {
      this.bytes = bytes;
   }

   public byte[] getBytes() {
      return bytes;
   }

   public static int size(int objectBytes) {
      int typeId = ProtoStreamTypeIds.USER_MARSHALLER_BYTES;
      int typeIdSize = tagSize(19, 1) + computeUInt32SizeNoTag(typeId);
      int userBytesFieldSize = tagSize(1, 2) + computeUInt32SizeNoTag(objectBytes) + objectBytes;
      int wrappedMessageSize = tagSize(17, 2) + computeUInt32SizeNoTag(objectBytes);

      return typeIdSize + userBytesFieldSize + wrappedMessageSize;
   }

   private static int tagSize(int fieldNumber, int wireType) {
      return computeUInt32SizeNoTag(fieldNumber << 3 | wireType);
   }

   // Protobuf logic included to avoid requiring a dependency on com.google.protobuf.CodedOutputStream
   private static int computeUInt32SizeNoTag(int value) {
      if ((value & -128) == 0) {
         return 1;
      } else if ((value & -16384) == 0) {
         return 2;
      } else if ((value & -2097152) == 0) {
         return 3;
      } else {
         return (value & -268435456) == 0 ? 4 : 5;
      }
   }
}
