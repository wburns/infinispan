package org.infinispan.stream.impl;

import org.infinispan.factories.ComponentRegistry;

import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Created by wburns on 10/22/15.
 */
public interface IterableTerminalOperation<R> {
   /**
    * Invoked when a key aware operation is desired without rehash being enabled.
    * @param response the consumer that will be called back for any intermediate results
    * @return the final response from the remote node
    */
   Iterable<R> performOperation(Consumer<Iterable<R>> response);

   /**
    * This method is to be invoked only locally after a key tracking operation has been serialized to a new node
    * @param supplier the supplier to use
    */
   void setSupplier(Supplier<? extends Stream<?>> supplier);

   /**
    * Handles injection of components for various intermediate and this operation.
    * @param registry component registry to use
    */
   void handleInjection(ComponentRegistry registry);
}
