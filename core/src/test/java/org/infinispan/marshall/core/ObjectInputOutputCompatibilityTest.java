package org.infinispan.marshall.core;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.impl.ClassToExternalizerMap;
import org.testng.annotations.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

@Test(groups = "functional", testName = "marshall.core.ObjectInputOutputCompatibilityTest")
public class ObjectInputOutputCompatibilityTest {
   public static class OuterTestClass {
      private final int id;
      private final InnerTestClass innerTestClass;

      public OuterTestClass(int id, InnerTestClass innerTestClass) {
         this.id = id;
         this.innerTestClass = innerTestClass;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         OuterTestClass that = (OuterTestClass) o;

         if (id != that.id) return false;
         return innerTestClass.equals(that.innerTestClass);
      }

      @Override
      public int hashCode() {
         int result = id;
         result = 31 * result + innerTestClass.hashCode();
         return result;
      }

      public static class Externalizer extends AbstractExternalizer<OuterTestClass> {
         @Override
         public void writeObject(ObjectOutput output, OuterTestClass object) throws IOException {
            output.writeInt(object.id);
            output.writeObject(object.innerTestClass);
         }

         @Override
         public OuterTestClass readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new OuterTestClass(input.readInt(), (InnerTestClass) input.readObject());
         }

         @Override
         public Integer getId() {
            return 104;
         }

         @Override
         public Set<Class<? extends OuterTestClass>> getTypeClasses() {
            return Util.asSet(OuterTestClass.class);
         }
      }
   }

   public static class InnerTestClass {
      private final String name;
      private final boolean isAdult;

      public InnerTestClass(String name, boolean isAdult) {
         this.name = name;
         this.isAdult = isAdult;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         InnerTestClass that = (InnerTestClass) o;

         if (isAdult != that.isAdult) return false;
         return name.equals(that.name);
      }

      @Override
      public int hashCode() {
         int result = name.hashCode();
         result = 31 * result + (isAdult ? 1 : 0);
         return result;
      }

      public static class Externalizer extends AbstractExternalizer<InnerTestClass> {
         @Override
         public void writeObject(ObjectOutput output, InnerTestClass object) throws IOException {
            output.writeUTF(object.name);
            output.writeBoolean(object.isAdult);
         }

         @Override
         public InnerTestClass readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new InnerTestClass(input.readUTF(), input.readBoolean());
         }

         @Override
         public Integer getId() {
            return 23;
         }

         @Override
         public Set<Class<? extends InnerTestClass>> getTypeClasses() {
            return Util.asSet(InnerTestClass.class);
         }
      }
   }
   @Test
   public void testByteBufWriteAndBytesRead() throws IOException, ClassNotFoundException {
      ByteBuf buf = Unpooled.buffer();
      GlobalMarshaller globalMarshaller = new GlobalMarshaller() {
         @Override
         public void start() {
            internalExts = new ClassToExternalizerMap(4, 0.6f);
            internalExts.put(OuterTestClass.class, new OuterTestClass.Externalizer());
            internalExts.put(InnerTestClass.class, new InnerTestClass.Externalizer());

            reverseInternalExts = internalExts.reverseMap(78124);
         }
      };
      globalMarshaller.start();

      try (ByteBufObjectOutput bboo = new ByteBufObjectOutput(buf, globalMarshaller)) {
         writeObjectOutput(bboo);
      }

      int total = buf.readableBytes();
      byte[] innerArray = buf.array();
      byte[] trimmedArray = new byte[total];
      System.arraycopy(innerArray, 0, trimmedArray, 0, total);

      try (BytesObjectInput input = BytesObjectInput.from(trimmedArray, globalMarshaller)) {
         assertObjectInput(input);
      }
   }

   private void writeObjectOutput(ObjectOutput output) throws IOException {
      output.write(32);
      MarshallUtil.marshallByteArray(new byte[] {122, 12, -31, 91}, output);
      output.writeInt(832178);
      output.write(new byte[] {65, -12, 31, 1, -12}, 1, 2);
      output.writeBoolean(false);
      output.writeChar('>');
      output.writeDouble(1.231d);
      output.writeShort(1231);
      output.writeUTF("ƽǒ1jk21 >;!*");
      output.writeLong(3213123128928776L);
      output.writeFloat(3.14f);

      InnerTestClass innerTestClass = new InnerTestClass("Tristan", true);
      OuterTestClass outerTestClass = new OuterTestClass(1, innerTestClass);
      output.writeObject(outerTestClass);
   }

   private void assertObjectInput(ObjectInput input) throws IOException, ClassNotFoundException {
      assertEquals(32, input.read());
      assertEquals(new byte[] {122, 12, -31, 91}, MarshallUtil.unmarshallByteArray(input));
      assertEquals(832178, input.readInt());
      assertEquals(-12, input.readByte());
      assertEquals(31, input.readByte());
      assertFalse(input.readBoolean());
      assertEquals('>', input.readChar());
      assertEquals(1.231d, input.readDouble());
      assertEquals(1231, input.readShort());
      assertEquals("ƽǒ1jk21 >;!*", input.readUTF());
      assertEquals(3213123128928776L, input.readLong());
      assertEquals(3.14f, input.readFloat());

      InnerTestClass innerTestClass = new InnerTestClass("Tristan", true);
      OuterTestClass outerTestClass = new OuterTestClass(1, innerTestClass);

      assertEquals(outerTestClass, input.readObject());
   }
}
