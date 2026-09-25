package org.infinispan.client.hotrod.configuration;

/**
 * Defines the strategy for synchronizing client-side near cache evictions with the server-side filter.
 *
 * @since 16.3
 */
public enum NearCacheEvictionStrategy {
   /**
    * Collects evicted keys and sends them in asynchronous batches to the server to be removed from the server-side filter.
    */
   BATCH_DELETE,

   /**
    * Resets the entire near cache and server filter when the number of evictions reaches a threshold.
    */
   CLEAR_ON_THRESHOLD
}
