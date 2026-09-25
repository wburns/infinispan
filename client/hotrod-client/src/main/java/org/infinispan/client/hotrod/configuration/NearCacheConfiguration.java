package org.infinispan.client.hotrod.configuration;

import org.infinispan.client.hotrod.near.DefaultNearCacheFactory;
import org.infinispan.client.hotrod.near.NearCacheFactory;

public class NearCacheConfiguration {
   public static final int DEFAULT_EVICTION_BATCH_SIZE = 32;

   // TODO: Consider an option to configure key equivalence function for near cache (e.g. for byte arrays)
   private final NearCacheMode mode;
   private final int maxEntries;
   private final boolean bloomFilter;
   private final NearCacheEvictionStrategy evictionStrategy;
   private final int evictionBatchSize;
   private final int evictionThreshold;
   private final NearCacheFactory nearCacheFactory;

   public NearCacheConfiguration(NearCacheMode mode, int maxEntries, boolean bloomFilterOptimization) {
      this(mode, maxEntries, bloomFilterOptimization, DefaultNearCacheFactory.INSTANCE);
   }

   public NearCacheConfiguration(NearCacheMode mode, int maxEntries, boolean bloomFilter, NearCacheFactory nearCacheFactory) {
      this(mode, maxEntries, bloomFilter, NearCacheEvictionStrategy.BATCH_DELETE, DEFAULT_EVICTION_BATCH_SIZE, maxEntries, nearCacheFactory);
   }

   public NearCacheConfiguration(NearCacheMode mode, int maxEntries, boolean bloomFilter,
                                 NearCacheEvictionStrategy evictionStrategy, int evictionBatchSize, int evictionThreshold,
                                 NearCacheFactory nearCacheFactory) {
      this.mode = mode;
      this.maxEntries = maxEntries;
      this.bloomFilter = bloomFilter;
      this.evictionStrategy = evictionStrategy != null ? evictionStrategy : NearCacheEvictionStrategy.BATCH_DELETE;
      this.evictionBatchSize = evictionBatchSize > 0 ? evictionBatchSize : DEFAULT_EVICTION_BATCH_SIZE;
      this.evictionThreshold = evictionThreshold > 0 ? evictionThreshold : (maxEntries > 0 ? maxEntries : 100);
      this.nearCacheFactory = nearCacheFactory;
   }

   public int maxEntries() {
      return maxEntries;
   }

   public NearCacheMode mode() {
      return mode;
   }

   public boolean bloomFilter() {
      return bloomFilter;
   }

   public NearCacheEvictionStrategy evictionStrategy() {
      return evictionStrategy;
   }

   public int evictionBatchSize() {
      return evictionBatchSize;
   }

   public int evictionThreshold() {
      return evictionThreshold;
   }

   public NearCacheFactory nearCacheFactory() {
      return nearCacheFactory;
   }

   @Override
   public String toString() {
      return "NearCacheConfiguration{" +
            "mode=" + mode +
            ", maxEntries=" + maxEntries +
            ", bloomFilter=" + bloomFilter +
            ", evictionStrategy=" + evictionStrategy +
            ", evictionBatchSize=" + evictionBatchSize +
            ", evictionThreshold=" + evictionThreshold +
            ", nearCacheFactory=" + nearCacheFactory +
            '}';
   }
}
