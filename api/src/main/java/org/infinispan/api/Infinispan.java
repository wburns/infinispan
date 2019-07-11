package org.infinispan.api;

import java.util.concurrent.CompletionStage;

import org.infinispan.api.reactive.KeyValueStore;
import org.infinispan.api.reactive.KeyValueStoreConfig;

/**
 * Infinispan instance, embedded or client, depending on the access point. {@link
 * InfinispanClient} or {@link InfinispanEmbedded}
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 10.0
 */
@Experimental
public interface Infinispan {

   /**
    * Gets the {@link KeyValueStore} by name.
    * <p>
    * If the store does not exist, creates a {@link KeyValueStore} with the given config
    *
    * @param name, name of the store
    * @return {@link KeyValueStore}
    */
   <K, V> CompletionStage<KeyValueStore<K, V>> getKeyValueStore(String name, KeyValueStoreConfig config);

   /**
    * Stops Infinispan
    *
    * @return {@link CompletionStage}
    */
   CompletionStage<Void> stop();
}