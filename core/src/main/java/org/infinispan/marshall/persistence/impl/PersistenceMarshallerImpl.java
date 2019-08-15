package org.infinispan.marshall.persistence.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.SerializationConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A Protostream based {@link PersistenceMarshaller} implementation that is responsible
 * for marshalling/unmarshalling objects which are to be persisted.
 * <p>
 * Known internal objects that are required by stores and loaders, such as {@link org.infinispan.metadata.EmbeddedMetadata},
 * are registered with this marshaller's {@link SerializationContext} so that they can be natively marshalled by the
 * underlying Protostream marshaller. If no entry exists in the {@link SerializationContext} for a given object, then
 * the marshalling of said object is delegated to the user marshaller
 * {@link org.infinispan.configuration.global.SerializationConfiguration#MARSHALLER} and the generated bytes are wrapped
 * in a {@link PersistenceMarshallerImpl.UserBytes} object and marshalled by ProtoStream.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
@Scope(Scopes.GLOBAL)
public class PersistenceMarshallerImpl implements PersistenceMarshaller {

   private static final Log log = LogFactory.getLog(PersistenceMarshallerImpl.class, Log.class);

   @Inject GlobalComponentRegistry gcr;
   private final SerializationContext serializationContext = ProtobufUtil.newSerializationContext();

   private Marshaller userMarshaller;

   public PersistenceMarshallerImpl() {
   }

   @Start
   @Override
   public void start() {
      userMarshaller = createUserMarshaller();
      log.startingUserMarshaller(userMarshaller.getClass().getName());
      userMarshaller.start();

      register(new PersistenceContextInitializerImpl());
   }

   private Marshaller createUserMarshaller() {
      GlobalConfiguration globalConfig = gcr.getGlobalConfiguration();
      SerializationConfiguration serializationConfig = globalConfig.serialization();
      Marshaller marshaller = serializationConfig.marshaller();
      if (marshaller != null)
         return marshaller;

      // If no marshaller or SerializationContextInitializer specified, then we attempt to load `infinispan-jboss-marshalling`
      // and the JBossUserMarshaller, however if it does not exist then we default to the JavaSerializationMarshaller
      try {
         Class<Marshaller> clazz = Util.loadClassStrict("org.infinispan.jboss.marshalling.core.JBossUserMarshaller", globalConfig.classLoader());
         try {
            log.jbossMarshallingDetected();
            return clazz.getConstructor(GlobalComponentRegistry.class).newInstance(gcr);
         } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new CacheException("Unable to start PersistenceMarshaller with JBossUserMarshaller", e);
         }
      } catch (ClassNotFoundException e) {
         ClassWhiteList whiteList = gcr.getCacheManager().getClassWhiteList();
         UserMarshallerWhiteList.addInternalClassesToWhiteList(whiteList);
         return new JavaSerializationMarshaller(whiteList);
      }
   }

   @Override
   public void register(SerializationContextInitializer initializer) {
      register(serializationContext, initializer);
   }

   private void register(SerializationContext ctx, SerializationContextInitializer initializer) {
      if (initializer == null) return;
      try {
         initializer.registerSchema(ctx);
         initializer.registerMarshallers(ctx);
      } catch (IOException e) {
         throw new CacheException("Exception encountered when initialising SerializationContext", e);
      }
   }

   @Override
   public void stop() {
      userMarshaller.stop();
   }

   @Override
   public MediaType mediaType() {
      return MediaType.APPLICATION_PROTOSTREAM;
   }

   @Override
   public ByteBuffer objectToBuffer(Object o) {
      return objectToBuffer(o, -1);
   }

   @Override
   public byte[] objectToByteBuffer(Object obj, int estimatedSize) throws IOException, InterruptedException {
      ByteBuffer b = objectToBuffer(obj, estimatedSize);
      byte[] bytes = new byte[b.getLength()];
      System.arraycopy(b.getBuf(), b.getOffset(), bytes, 0, b.getLength());
      return bytes;
   }

   @Override
   public byte[] objectToByteBuffer(Object obj) throws IOException, InterruptedException {
      return objectToByteBuffer(obj, sizeEstimate(obj));
   }

   private ByteBuffer objectToBuffer(Object o, int estimatedSize) {
      if (o == null)
         return null;

      try {
         boolean requiresWrapping = !isPersistenceClass(o);
         if (requiresWrapping)
            o = wrapUserObject(o);
         int size = estimatedSize < 0 ? sizeEstimate(o, true) : estimatedSize;
         ByteArrayOutputStream baos = new ByteArrayOutputStream(size);
         ProtobufUtil.toWrappedStream(serializationContext, baos, o, size);
         byte[] bytes = baos.toByteArray();
         return new ByteBufferImpl(bytes, 0, bytes.length);
      } catch (Throwable t) {
         log.warnf(t, "Cannot marshall %s", o.getClass().getName());
         if (t instanceof MarshallingException)
            throw (MarshallingException) t;
         throw new MarshallingException(t.getMessage(), t.getCause());
      }
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
      return objectFromByteBuffer(buf, 0, buf.length);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException {
      return unwrapAndInit(ProtobufUtil.fromWrappedByteArray(serializationContext, buf, offset, length));
   }

   @Override
   public BufferSizePredictor getBufferSizePredictor(Object o) {
      // TODO if persistenceClass, i.e. protobuf based, return estimate based upon schema
      return userMarshaller.getBufferSizePredictor(o);
   }

   @Override
   public void writeObject(Object o, OutputStream out) throws IOException {
      boolean requiresWrapping = !isPersistenceClass(o);
      if (requiresWrapping)
         o = wrapUserObject(o);
      int size = sizeEstimate(o, true);
      ProtobufUtil.toWrappedStream(serializationContext, out, o, size);
   }

   @Override
   public Object readObject(InputStream in) throws ClassNotFoundException, IOException {
      return unwrapAndInit(ProtobufUtil.fromWrappedStream(serializationContext, in));
   }

   private byte[] marshallUserObject(Object object) {
      try {
         return userMarshaller.objectToByteBuffer(object);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      } catch (IOException e) {
         throw new MarshallingException(e);
      }
   }

   private Object unmarshallUserBytes(byte[] bytes) {
      try {
         return userMarshaller.objectFromByteBuffer(bytes);
      } catch (Exception e) {
         throw new MarshallingException(e);
      }
   }

   private Object unwrapAndInit(Object o) {
      if (o instanceof UserBytes) {
         UserBytes userBytes = (UserBytes) o;
         return unmarshallUserBytes(userBytes.bytes);
      }
      return o;
   }

   @Override
   public boolean isMarshallable(Object o) {
      return isPersistenceClass(o) || isUserMarshallable(o);
   }

   @Override
   public int sizeEstimate(Object o) {
      return sizeEstimate(o, isPersistenceClass(o));
   }

   private Object wrapUserObject(Object o) {
      return new UserBytes(marshallUserObject(o));
   }

   private int sizeEstimate(Object o, boolean persistenceClass) {
      // hardcoded for now in order to avoid requiring a dependency on com.google.protobuf.CodedOutputStream
      // Dynamic estimates will be provided in future protostream version
      int wrapperEstimate = 40;
      if (persistenceClass) {
         if (o instanceof UserBytes) {
            byte[] user = ((UserBytes) o).bytes;
            return wrapperEstimate + user.length;
         }
         // Return the CodedOutputStream.DEFAULT_BUFFER_SIZE as this is equivalent to passing no estimate
         return 4096;
      }
      int userBytesEstimate = userMarshaller.getBufferSizePredictor(o.getClass()).nextSize(o);
      return wrapperEstimate + userBytesEstimate;
   }

   private boolean isPersistenceClass(Object o) {
      return o instanceof String ||
            o instanceof Long ||
            o instanceof Integer ||
            o instanceof Double ||
            o instanceof Float ||
            o instanceof Boolean ||
            o instanceof byte[] ||
            o instanceof Byte ||
            o instanceof Short ||
            o instanceof Character ||
            o instanceof java.util.Date ||
            o instanceof java.time.Instant ||
            serializationContext.canMarshall(o.getClass());
   }

   private boolean isUserMarshallable(Object o) {
      try {
         return userMarshaller.isMarshallable(o);
      } catch (Exception ignore) {
         return false;
      }
   }

   static class UserBytes {

      @ProtoField(number = 1)
      final byte[] bytes;

      @ProtoFactory
      UserBytes(byte[] bytes) {
         this.bytes = bytes;
      }
   }
}
