package org.infinispan.stream.impl;

import org.infinispan.commons.CacheException;
import org.infinispan.util.CloseableSupplier;

import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Created by wburns on 10/23/15.
 */
public interface StreamCloseableSupplier<R> extends CloseableSupplier<R> {
   void close(CacheException e);

   /**
    * Allows a consumer to be fired before the entry is returned via the {@link Supplier#get()} method.
    * @param consumer the consumer to listen
    */
   void addConsumerOnSupply(Consumer<? super R> consumer);

   /**
    * This sets the identifier for the supplier.  This can  be useful for the supplier to trigger some additional
    * service when this supplier is closed usually.  Such as stopping additional processing.
    *
    * @param identifier the identifier to use
    */
   void setIdentifier(UUID identifier);
}
