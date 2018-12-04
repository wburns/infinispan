package org.infinispan;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import org.infinispan.util.function.SerializableBiConsumer;

/**
 * @author wburns
 * @since 10.0
 */
public interface UnsafeInfinispanPublisher<T> extends InfinispanPublisher<T> {

   /**
    * Returns a "safe" InfinispanPublisher that guarantees exactly-once delivery semantics.
    * @return
    */
   InfinispanPublisher<T> exactlyOnceDelivery();

   <K, V> CompletableFuture<Void> forEach(BiConsumer<? super Cache<K, V>, ? super T> consumer);

   <K, V> CompletableFuture<Void> forEach(SerializableBiConsumer<? super Cache<K, V>, ? super T> consumer);
}
