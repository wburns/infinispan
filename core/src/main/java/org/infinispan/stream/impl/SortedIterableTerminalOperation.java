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
public interface SortedIterableTerminalOperation<Sorted, R> extends SegmentAwareOperation, IterableTerminalOperation<R> {
   interface SortedConsumer<Sorted, R> {
      void accept(Iterable<R> response, Sorted highestSort);
      void completed(Iterable<R> response, Sorted highestSort);
   }

   /**
    * Invoked when a key and rehash aware operation is desired.
    * @param response the consumer that will be called back for any intermediate results
    * @return the final response from the remote node
    */
   void performOperationRehashAware(SortedConsumer<Sorted, R> response);
}
