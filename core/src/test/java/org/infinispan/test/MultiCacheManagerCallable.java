package org.infinispan.test;

import org.infinispan.manager.EmbeddedCacheManager;

/**
 * A task that executes operations against a group of cache managers.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public abstract class MultiCacheManagerCallable {

   protected final EmbeddedCacheManager[] cms;

   public MultiCacheManagerCallable(EmbeddedCacheManager... cms) {
      this.cms = cms;
   }

   public abstract void call();
}
