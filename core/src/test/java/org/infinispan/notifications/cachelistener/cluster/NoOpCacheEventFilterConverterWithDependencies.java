package org.infinispan.notifications.cachelistener.cluster;

import java.io.Serializable;

import org.infinispan.Cache;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.AbstractCacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.EventType;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
@Scope(Scopes.NONE)
public class NoOpCacheEventFilterConverterWithDependencies<K, V>
      extends AbstractCacheEventFilterConverter<K, V, V> implements Serializable {

   private transient Cache cache;

   @Inject
   protected void injectDependencies(Cache cache) {
      this.cache = cache;
   }

   @Override
   public V filterAndConvert(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata, EventType eventType) {
      if (cache == null) {
         throw new IllegalStateException("Dependencies were not injected");
      }
      return newValue;
   }
}
