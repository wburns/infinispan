package org.infinispan.stream.impl;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.factories.ComponentRegistry;

import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * A terminal operation for a {@link org.infinispan.CacheStream} that allows tracking keys during a rehash event.
 * @param <K> key type for the entry returned
 * @param <R> return type when not utilizing rehash aware
 * @param <R2> value type for the entry returned
 */
public interface KeyTrackingTerminalOperation<K, R, R2> extends SegmentAwareOperation {
   /**
    * Invoked when a key aware operation is desired without rehash being enabled.
    * @param response the collector that will be called back for any intermediate results
    * @return the final response from the remote node
    */
   Iterable<R> performOperation(Consumer<Iterable<R>> response);

   /**
    * Invoked when a key and rehash aware operation is desired.
    * @param response the collector that will be called back for any intermediate results
    * @return the final response from the remote node
    */
   Iterable<CacheEntry<K, R2>> performOperationRehashAware(Consumer<Iterable<CacheEntry<K, R2>>> response);

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
