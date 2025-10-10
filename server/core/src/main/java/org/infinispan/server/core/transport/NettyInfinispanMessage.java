package org.infinispan.server.core.transport;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.StreamAwareMarshaller;
import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.remoting.transport.jgroups.InfinispanMessage;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.jgroups.Address;

import io.netty.buffer.ByteBuf;

public class NettyInfinispanMessage extends InfinispanMessage implements JGroupsTransport.MessageCreator<NettyInfinispanMessage>  {
   public NettyInfinispanMessage() { }

   public NettyInfinispanMessage(Address target, WrappedBytes key, Object object, StreamAwareMarshaller marshaller) {
      super(target, key, object, marshaller);
   }

   @Override
   protected void writeObjectToDataOutput(DataOutput out) throws IOException {
      try {
         byte[] bytes = ((Marshaller) marshaller).objectToByteBuffer(deserializedObject);
         // Make sure size is updated just in case
         size = bytes.length;
         out.writeInt(bytes.length);
         out.write(bytes);
      } catch (InterruptedException e) {
         throw new IOException(e);
      }
//      // TODO: optimize based on if primitive?
//      ByteBuf buf = size > 0 ? ByteBufAllocator.DEFAULT.heapBuffer(size, size) : ByteBufAllocator.DEFAULT.heapBuffer();
//      try {
//         marshaller.writeObject(deserializedObject, new ByteBufRandomAccessOutputStream(buf));
//         int length = buf.readableBytes();
//         size = length;
//         out.writeInt(length);
//         out.write(buf.array(), buf.arrayOffset() + buf.readerIndex(), length);
//      } finally {
//         buf.release();
//      }
   }

   @Override
   protected Object deserializeObject(DataInput in, int length) throws IOException, ClassNotFoundException {
      ByteBuf buf = null;
      try {
         byte[] array;
         int offset;
//         if (length > 256) {
//            buf = ByteBufAllocator.DEFAULT.heapBuffer(length, length);
//            array = buf.array();
//            offset = buf.arrayOffset() + buf.readerIndex();
//         } else {
            array = new byte[length];
            offset = 0;
//         }
         in.readFully(array, offset, length);
         return ((Marshaller) marshaller).objectFromByteBuffer(array, offset, length);
      } finally {
         if (buf != null) {
            buf.release();
         }
      }
   }

   @Override
   public Supplier<NettyInfinispanMessage> create() {
      return NettyInfinispanMessage::new;
   }

   @Override
   public NettyInfinispanMessage create(Address target, WrappedBytes key, Object object, StreamAwareMarshaller marshaller) {
      return new NettyInfinispanMessage(target, key, object, marshaller);
   }
}
