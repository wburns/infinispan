package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @author wburns
 * @since 9.4
 */
public abstract class AbstractNonSharedSegmentedConfiguration<T extends AbstractStoreConfiguration> extends AbstractStoreConfiguration {
   public AbstractNonSharedSegmentedConfiguration(AttributeSet attributes, AsyncStoreConfiguration async,
         SingletonStoreConfiguration singletonStore) {
      super(attributes, async, singletonStore);
   }

   public abstract T newConfigurationFrom(int segment);
}
