package org.infinispan.stream.impl;

import org.infinispan.factories.ComponentRegistry;

import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * TODO:
 * A terminal operation for use with distributed stream
 * @param <R> resulting values
 */
public interface SortedIterableTerminalOperation<Sorted, R> extends SegmentAwareOperation {
   interface SortedConsumer<Sorted, R> {
      void accept(Iterable<R> response, Sorted highestSort);
      void completed(Iterable<R> response, Sorted highestSort);
   }

   /**
    * Invoked when a key aware operation is desired without rehash being enabled.
    * @param response the consumer that will be called back for any intermediate results
    * @return the final response from the remote node
    */
   Iterable<R> performOperation(Consumer<Iterable<R>> response);

   /**
    * Invoked when a key and rehash aware operation is desired.
    * @param response the consumer that will be called back for any intermediate results
    * @return the final response from the remote node
    */
   void performOperationRehashAware(SortedConsumer<Sorted, R> response);

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
