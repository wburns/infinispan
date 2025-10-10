package org.infinispan.remoting.transport.jgroups;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Callable;

import org.infinispan.commons.marshall.ByteBackedRandomAccessOutputStream;
import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.commons.marshall.StreamAwareMarshaller;
import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.protostream.RandomAccessOutputStream;
import org.jgroups.BaseMessage;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.util.ByteArray;
import org.jgroups.util.ByteArrayDataOutputStream;

public abstract class InfinispanMessage extends BaseMessage implements Callable<Object> {
   public static final short TYPE = 2321;

   protected WrappedBytes key;
   protected StreamAwareMarshaller marshaller;

   protected Object deserializedObject;
   protected int size;
   protected Throwable receivedThrowable;

   public InfinispanMessage() {
   }

   public InfinispanMessage(org.jgroups.Address target, WrappedBytes key, Object object,
                            StreamAwareMarshaller marshaller) {
      this(target, key, object, marshaller, 0);
   }

   public InfinispanMessage(org.jgroups.Address target, WrappedBytes key, Object object,
                            StreamAwareMarshaller marshaller, int sizeEstimate) {
      super(target);
      this.key = Objects.requireNonNull(key);
      if (key.getLength() > Byte.MAX_VALUE) {
         throw new IllegalArgumentException("");
      }
      this.deserializedObject = Objects.requireNonNull(object);
      this.marshaller = Objects.requireNonNull(marshaller);
      this.size = sizeEstimate;
   }

   @Override
   protected int payloadSize() {
      return size + 1 + key.getLength();
   }

   @Override
   public short getType() {
      return TYPE;
   }

   @Override
   public boolean hasPayload() {
      return true;
   }

   @Override
   public boolean hasArray() {
      return false;
   }

   @Override
   public void writePayload(DataOutput out) throws IOException {
//      out.writeByte(key.getLength());
//      out.write(key.getBytes());

      if (out instanceof ByteArrayDataOutputStream bdos) {
         if (size == 0) {
            size = marshaller.sizeEstimate(deserializedObject);
         }
         // This might over allocate, but will always be larger than our value
         int originalPos = bdos.position();
         bdos.ensureCapacity(size + 4);

         try (RandomAccessOutputStream ros = new ByteBackedRandomAccessOutputStream(bdos.buffer(), originalPos + 4)) {
            marshaller.writeObject(deserializedObject, ros);
            bdos.position(originalPos);
            int actualSize = ros.getPosition() - originalPos - 4;
            size = actualSize;
            bdos.writeInt(actualSize);
            bdos.position(ros.getPosition());
         }

      } else {
         writeObjectToDataOutput(out);
      }
   }

   protected abstract void writeObjectToDataOutput(DataOutput out) throws IOException;

   @Override
   public void readPayload(DataInput in) {
      try {
//         if (marshaller == null) {
//            int keyLength = in.readByte();
//            byte[] keyBytes = new byte[keyLength];
//            in.readFully(keyBytes);
//            key = new WrappedByteArray(keyBytes);
//            marshaller = JGroupsTransport.MARSHALLER.get(key);
//         } else {
//            in.skipBytes(key.getLength() + 1);
//         }
         marshaller = JGroupsTransport.SINGLE_MARSHALLER;
         if (deserializedObject == null) {
            int length = in.readInt();
            size = length;
            deserializedObject = deserializeObject(in, length);
         } else {
            in.skipBytes(size + 4);
         }
      } catch (Throwable t) {
         receivedThrowable = t;
      }
   }

   protected abstract Object deserializeObject(DataInput in, int length) throws IOException, ClassNotFoundException;

   @Override
   protected Message copyPayload(Message copy) {
      if (copy instanceof InfinispanMessage ibm) {
         ibm.key = key;
         ibm.size = size;
         ibm.marshaller = marshaller;
         ibm.deserializedObject = deserializedObject;
      }
      return copy;
   }

   @Override
   public Object call() throws MarshallingException {
      if (receivedThrowable != null) {
         throw new MarshallingException(receivedThrowable);
      }
      return deserializedObject;
   }

   public byte[] getArray() {
      throw new UnsupportedOperationException();
   }

   @Override
   public int getOffset() {
      return -1;
   }

   @Override
   public int getLength() {
      return -1;
   }

   public ObjectMessage setArray(byte[] b, int off, int len) {
      throw new UnsupportedOperationException();
   }

   public ObjectMessage setArray(ByteArray buf) {
      throw new UnsupportedOperationException();
   }

   @Override
   public <T> T getObject() {
      return null;
   }

   @Override
   public Message setObject(Object obj) {
      return null;
   }
}
