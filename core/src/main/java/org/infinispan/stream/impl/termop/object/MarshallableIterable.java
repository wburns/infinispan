package org.infinispan.stream.impl.termop.object;

import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.stream.Stream;

/**
 * This is an {@link Iterable} that can be marshalled.  This iterable can only create 1 iterator however.
 */
@SerializeWith(value = MarshallableIterable.MarshallableIterableExternalizer.class)
class MarshallableIterable<T> implements Iterable<T> {
   private final Iterable<T> iterable;

   MarshallableIterable(Iterable<T> iterable) {
      this.iterable = iterable;
   }

   @Override
   public Iterator<T> iterator() {
      return iterable.iterator();
   }

   public static final class MarshallableIterableExternalizer implements Externalizer<MarshallableIterable> {
      @Override
      public void writeObject(ObjectOutput output, MarshallableIterable object) throws IOException {
         for(Object i : object.iterable) {
            output.writeBoolean(true);
            output.writeObject(i);
         }
         output.writeBoolean(false);
      }

      @Override
      public MarshallableIterable readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Stream.Builder builder = Stream.builder();
         while (input.readBoolean()) {
            builder.accept(input.readObject());
         }
         return new MarshallableIterable(builder.build()::iterator);
      }
   }
}
