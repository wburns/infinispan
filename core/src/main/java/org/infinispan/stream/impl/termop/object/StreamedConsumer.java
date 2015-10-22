package org.infinispan.stream.impl.termop.object;

import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Allows for generation
 */
public interface StreamedConsumer<E> extends Consumer<E> {

   /**
    *
    * @param e
    * @throws BatchOverlapException
    */
   @Override
   void accept(E e) throws BatchOverlapException;

   /**
    *
    * @return
    * @throws BatchOverlapException
    */
   E compact() throws BatchOverlapException;

   /**
    *
    * @return
    */
   Stream<E> stream();

   /**
    *
    * @return
    */
   long estimatedSize();
}
