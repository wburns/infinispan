package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.AbstractStoreConfiguration.SHARED;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.persistence.Store;

/**
 * Configuration builder to be extended only by non shared stores
 * @author wburns
 * @since 9.0
 */
public abstract class AbstractSegmentedStoreConfigurationBuilder<T extends StoreConfiguration, S extends AbstractStoreConfigurationBuilder<T, S>>
      extends AbstractStoreConfigurationBuilder<T, S> {
   public AbstractSegmentedStoreConfigurationBuilder(PersistenceConfigurationBuilder builder, AttributeSet attributes) {
      super(builder, attributes);
   }

   public abstract T newConfigurationFor(int segment);

   @Override
   protected void validate(boolean skipClassChecks) {
      super.validate(skipClassChecks);
      boolean shared = attributes.attribute(SHARED).get();
      if (shared) {
         // TODO: store can't be configured as shared
         throw new CacheConfigurationException("TODO: store can't be configured as shared");
      }
      if (!skipClassChecks) {
         Class configKlass = attributes.getKlass();
         if (configKlass != null && configKlass.isAnnotationPresent(ConfigurationFor.class)) {
            Class storeKlass = ((ConfigurationFor) configKlass.getAnnotation(ConfigurationFor.class)).value();
            if (storeKlass.isAnnotationPresent(Store.class)) {
               Store storeProps = (Store) storeKlass.getAnnotation(Store.class);
               if (storeProps.shared()) {
                  // TODO: need to throw exception as this is used on a shareable store
                  throw new CacheConfigurationException("TODO: store is ");
               }
            }
         }
      }
   }
}
