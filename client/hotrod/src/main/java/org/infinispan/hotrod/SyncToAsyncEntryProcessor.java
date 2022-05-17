package org.infinispan.hotrod;

import java.util.concurrent.Flow;

import org.infinispan.api.async.AsyncCacheEntryProcessor;
import org.infinispan.api.common.MutableCacheEntry;
import org.infinispan.api.common.process.CacheEntryProcessorContext;
import org.infinispan.api.common.process.CacheEntryProcessorResult;
import org.infinispan.api.sync.SyncCacheEntryProcessor;

public class SyncToAsyncEntryProcessor<K, V, T> implements AsyncCacheEntryProcessor<K, V, T> {
   private final SyncCacheEntryProcessor<K, V, T> syncCacheEntryProcessor;

   public SyncToAsyncEntryProcessor(SyncCacheEntryProcessor<K, V, T> syncCacheEntryProcessor) {
      this.syncCacheEntryProcessor = syncCacheEntryProcessor;
   }

   @Override
   public Flow.Publisher<CacheEntryProcessorResult<K, T>> process(Flow.Publisher<MutableCacheEntry<K, V>> entries, CacheEntryProcessorContext context) {
      
      return null;
   }
}
