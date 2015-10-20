package org.infinispan.stream.impl;

import org.infinispan.factories.ComponentRegistry;

import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * TODO:
 * A terminal operation for use with distributed stream
 * @param <Sorted> the type of the sorted result.  In this case it is also resulting values
 */
public interface SortedMapTerminalOperation<Sorted, R> extends SegmentAwareOperation {
   /**
    * Invoked when a key aware operation is desired without rehash being enabled.
    * @param response the consumer that will be called back for any intermediate results
    * @return the final response from the remote node
    */
   Iterable<R> performOperation(Consumer<Iterable<Sorted>> response);

   /**
    * Invoked when a key and rehash aware operation is desired.
    * @param response the consumer that will be called back for any intermediate results
    * @return the final response from the remote node
    */
   Iterable<R> performOperationRehashAware(Consumer<Iterable<Sorted>> response);

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
