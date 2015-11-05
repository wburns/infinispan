package org.infinispan.container;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.metadata.Metadata;

/**
 * // TODO: Document this
 *
 * @author wburns
 * @since 4.0
 */
public class OffHeapInternalEntryFactoryImpl extends InternalEntryFactoryImpl {
   @Override
   public InternalCacheEntry update(InternalCacheEntry cacheEntry, Object value, Metadata metadata) {
      return create(cacheEntry.getKey(), value, metadata);
   }

   @Override
   public InternalCacheEntry update(InternalCacheEntry ice, Metadata metadata) {
      return create(ice.getKey(), ice.getValue(), metadata);
   }
}
