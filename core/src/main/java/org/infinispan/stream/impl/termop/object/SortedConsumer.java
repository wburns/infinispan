package org.infinispan.stream.impl.termop.object;

import java.util.stream.Stream;

/**
 * Allows for generation
 */
public interface SortedConsumer<E> {
   /**
    * Adds the element to the sorted stream
    * @param element
    */
   void addElement(E element);

   /**
    *
    * @return
    */
   Stream<E> toStream();
}
