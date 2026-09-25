package org.infinispan.client.hotrod.configuration;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import org.infinispan.client.hotrod.near.DefaultNearCacheFactory;
import org.infinispan.client.hotrod.near.NearCacheFactory;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class NearCacheConfigurationBuilder extends AbstractConfigurationChildBuilder
      implements Builder<NearCacheConfiguration> {

   private NearCacheMode mode = NearCacheMode.DISABLED;
   private Integer maxEntries = null; // undefined
   private boolean bloomFilter = false;
   private NearCacheEvictionStrategy evictionStrategy = NearCacheEvictionStrategy.BATCH_DELETE;
   private int evictionBatchSize = NearCacheConfiguration.DEFAULT_EVICTION_BATCH_SIZE;
   private int evictionThreshold = -1;
   private NearCacheFactory nearCacheFactory = DefaultNearCacheFactory.INSTANCE;

   protected NearCacheConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public AttributeSet attributes() {
      return AttributeSet.EMPTY;
   }

   /**
    * Specifies the maximum number of entries that will be held in the near cache.
    *
    * @param maxEntries maximum entries in the near cache.
    * @return an instance of the builder
    */
   public NearCacheConfigurationBuilder maxEntries(int maxEntries) {
      this.maxEntries = maxEntries;
      return this;
   }

   /**
    * Specifies whether bloom filter should be used for near cache to limit the number of write
    * notifications for unrelated keys.
    * @param enable whether to enable bloom filter
    * @return an instance of this builder
    */
   public NearCacheConfigurationBuilder bloomFilter(boolean enable) {
      this.bloomFilter = enable;
      return this;
   }

   /**
    * Specifies the eviction strategy to use for synchronizing client-side near cache evictions
    * with the server-side filter.
    *
    * @param strategy the {@link NearCacheEvictionStrategy}
    * @return an instance of this builder
    */
   public NearCacheConfigurationBuilder evictionStrategy(NearCacheEvictionStrategy strategy) {
      this.evictionStrategy = strategy;
      return this;
   }

   /**
    * Specifies the batch size of evicted keys sent to the server when using {@link NearCacheEvictionStrategy#BATCH_DELETE}.
    *
    * @param evictionBatchSize batch size of evicted keys
    * @return an instance of this builder
    */
   public NearCacheConfigurationBuilder evictionBatchSize(int evictionBatchSize) {
      this.evictionBatchSize = evictionBatchSize;
      return this;
   }

   /**
    * Specifies the eviction count threshold before resetting the near cache and server filter
    * when using {@link NearCacheEvictionStrategy#CLEAR_ON_THRESHOLD}.
    *
    * @param evictionThreshold the number of evictions before clearing
    * @return an instance of this builder
    */
   public NearCacheConfigurationBuilder evictionThreshold(int evictionThreshold) {
      this.evictionThreshold = evictionThreshold;
      return this;
   }

   /**
    * Specifies the near caching mode. See {@link NearCacheMode} for details on the available modes.
    *
    * @param mode one of {@link NearCacheMode}
    * @return an instance of the builder
    */
   public NearCacheConfigurationBuilder mode(NearCacheMode mode) {
      this.mode = mode;
      return this;
   }

   /**
    * Specifies a {@link NearCacheFactory} which is responsible for creating {@link org.infinispan.client.hotrod.near.NearCache} instances.
    *
    * @param factory a {@link NearCacheFactory}
    * @return an instance of the builder
    */
   public NearCacheConfigurationBuilder nearCacheFactory(NearCacheFactory factory) {
      this.nearCacheFactory = factory;
      return this;
   }

   @Override
   public void validate() {
      if (mode.enabled()) {
         if (maxEntries == null) {
            throw HOTROD.nearCacheMaxEntriesUndefined();
         } else if (maxEntries < 0 && bloomFilter) {
            throw HOTROD.nearCacheMaxEntriesPositiveWithBloom(maxEntries);
         }
      }
   }

   @Override
   public NearCacheConfiguration create() {
      int threshold = evictionThreshold > 0 ? evictionThreshold : (maxEntries == null ? 100 : maxEntries);
      return new NearCacheConfiguration(mode, maxEntries == null ? -1 : maxEntries, bloomFilter, evictionStrategy, evictionBatchSize, threshold, nearCacheFactory);
   }

   @Override
   public Builder<?> read(NearCacheConfiguration template, Combine combine) {
      mode = template.mode();
      maxEntries = template.maxEntries();
      bloomFilter = template.bloomFilter();
      evictionStrategy = template.evictionStrategy();
      evictionBatchSize = template.evictionBatchSize();
      evictionThreshold = template.evictionThreshold();
      nearCacheFactory = template.nearCacheFactory();
      return this;
   }
}
