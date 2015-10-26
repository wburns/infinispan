package org.infinispan.stream;

import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Comparator;
import java.util.function.Supplier;

/**
 * Helper class designed to be used to create a serializable Comparator for use with
 * {@link org.infinispan.CacheStream#sorted(Comparator)} from a supplier of a comparator.  The problem is that the
 * standard {@link java.util.Comparator} class doesn't provide Serializable Comparators and no way to extend
 * their functionality, so this class is used instead.
 */
public class CacheComparators {
   private CacheComparators() { }

   /**
    * Creates a comparator that is serializable and will upon usage create a comparator using the serializable supplier
    * provided by the user.
    * @param supplier The supplier to crate the comparator that is specifically serializable
    * @param <T> The input type of the comparator
    * @return the comparator which is serializable
    * @see SerializableSupplier
    */
   public static <T> Comparator<T> serializableComparator(SerializableSupplier<Comparator<T>> supplier) {
      return new ComparatorSupplier<>(supplier);
   }

   /**
    * Similar to {@link CacheComparators#serializableComparator(SerializableSupplier)} except that the supplier provided
    * must be marshable through ISPN marshalling techniques.  Note this is not detected until runtime.
    * @param supplier The marshallable supplier of comparators
    * @param <T> The input type of the comparator
    * @return the comparator which is serializable
    * @see Externalizer
    * @see org.infinispan.commons.marshall.AdvancedExternalizer
    */
   public static <T> Comparator<T> comparator(Supplier<Comparator<T>> supplier) {
      return new ComparatorSupplier<>(supplier);
   }

   @SerializeWith(value = ComparatorSupplier.ComparatorSupplierExternalizer.class)
   private static final class ComparatorSupplier<T> implements Comparator<T> {
      private final Supplier<Comparator<T>> supplier;
      private transient Comparator<T> comparator;

      private Comparator<T> getComparator() {
         if (comparator == null) {
            comparator = supplier.get();
         }
         return comparator;
      }

      ComparatorSupplier(Supplier<Comparator<T>> supplier) {
         this.supplier = supplier;
      }

      @Override
      public int compare(T o1, T o2) {
         return getComparator().compare(o1, o2);
      }

      public static final class ComparatorSupplierExternalizer implements Externalizer<ComparatorSupplier<?>> {

         @Override
         public void writeObject(ObjectOutput output, ComparatorSupplier object) throws IOException {
            output.writeObject(object.supplier);
         }

         @Override
         public ComparatorSupplier readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new ComparatorSupplier((Supplier<Comparator>) input.readObject());
         }
      }
   }
}
