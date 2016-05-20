package org.infinispan.stream.impl.intops;

import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Created by wburns on 5/19/16.
 */
public interface SortLimiterStreamBuilder<E> extends Consumer<E> {
   /**
    * Adds a new element to the builder
    * @param e
    */
   @Override
   void accept(E e);

   /**
    *
    * @return
    */
   Stream<E> stream();

   /**
    *
    * @return
    */
   Stream<E> parallelStream();
}
