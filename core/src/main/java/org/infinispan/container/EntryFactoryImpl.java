package org.infinispan.container;

import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.container.entries.ContextEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.metadata.Metadata;
import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.DeltaAwareCacheEntry;
import org.infinispan.container.entries.RepeatableReadEntry;
import org.infinispan.container.entries.StateChangingEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.metadata.Metadatas;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * {@link EntryFactory} implementation to be used for optimistic locking scheme.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public class EntryFactoryImpl implements EntryFactory {

   private static final Log log = LogFactory.getLog(EntryFactoryImpl.class);
   private final boolean trace = log.isTraceEnabled();
   
   private DataContainer container;
   protected boolean clusterModeWriteSkewCheck;
   private Configuration configuration;
   private CacheNotifier notifier;

   @Inject
   public void injectDependencies(DataContainer dataContainer, Configuration configuration, CacheNotifier notifier) {
      this.container = dataContainer;
      this.configuration = configuration;
      this.notifier = notifier;
   }

   @Start (priority = 8)
   public void init() {
      clusterModeWriteSkewCheck = configuration.locking().writeSkewCheck() &&
            configuration.clustering().cacheMode().isClustered() && configuration.versioning().scheme() == VersioningScheme.SIMPLE &&
            configuration.versioning().enabled();
   }

   @Override
   public final ContextEntry wrapEntryForReading(InvocationContext ctx, Object key) throws InterruptedException {
      ContextEntry contextEntry = getFromContext(ctx, key);
      if (contextEntry == null) {
         CacheEntry cacheEntry = getFromContainer(key);

         // do not bother wrapping though if this is not in a tx.  repeatable read etc are all meaningless unless there is a tx.
         if (cacheEntry == null) {
            contextEntry = createWrappedEntry(key, null, ctx, null, false, false, false);
         } else {
            contextEntry = createWrappedEntry(key, cacheEntry, ctx, null, false, false, false);
            // If the original entry has changeable state, copy state flags to the new MVCC entry.
            if (cacheEntry instanceof StateChangingEntry && contextEntry != null)
               contextEntry.copyStateFlagsFrom((StateChangingEntry) cacheEntry);
         }

         if (contextEntry != null) ctx.putLookedUpEntry(key, contextEntry);
         if (trace) {
            log.tracef("Wrap %s for read. Entry=%s", key, contextEntry);
         }
         return contextEntry;
      }
      if (trace) {
         log.tracef("Wrap %s for read. Entry=%s", key, contextEntry);
      }
      return contextEntry;
   }

   @Override
   public ContextEntry wrapPreviouslyReadEntry(InvocationContext ctx, CacheEntry entry) throws InterruptedException {
      return wrapCacheEntryForPut(ctx, entry.getKey(), entry, null, false);
   }

   @Override
   public final ContextEntry wrapEntryForClear(InvocationContext ctx, Object key) throws InterruptedException {
      //skipRead == true because the keys values are not read during the ClearOperation (neither by application)
      ContextEntry contextEntry = wrapEntry(ctx, key, null, true);
      if (trace) {
         log.tracef("Wrap %s for clear. Entry=%s", key, contextEntry);
      }
      return contextEntry;
   }

   @Override
   public final ContextEntry wrapEntryForReplace(InvocationContext ctx, ReplaceCommand cmd) throws InterruptedException {
      Object key = cmd.getKey();
      ContextEntry contextEntry = wrapEntry(ctx, key, cmd.getMetadata(), false);
      if (contextEntry == null) {
         // make sure we record this! Null value since this is a forced lock on the key
         ctx.putLookedUpEntry(key, null);
      }
      if (trace) {
         log.tracef("Wrap %s for replace. Entry=%s", key, contextEntry);
      }
      return contextEntry;
   }

   @Override
   public final ContextEntry wrapEntryForRemove(InvocationContext ctx, Object key, boolean skipRead) throws InterruptedException {
      CacheEntry cacheEntry = getFromContext(ctx, key);
      ContextEntry contextEntry = null;
      if (cacheEntry != null) {
         if (cacheEntry instanceof ContextEntry) {
            contextEntry = (ContextEntry) cacheEntry;
         } else {
            //skipRead == true because the key already exists in the context that means the key was previous accessed.
            contextEntry = wrapMvccEntryForRemove(ctx, key, cacheEntry, true);
         }
      } else {
         CacheEntry ice = getFromContainer(key);
         if (ice != null || clusterModeWriteSkewCheck) {
            contextEntry = wrapCacheEntryForPut(ctx, key, ice, null, skipRead);
         }
      }
      if (contextEntry == null) {
         // make sure we record this! Null value since this is a forced lock on the key
         ctx.putLookedUpEntry(key, null);
      } else {
         contextEntry.copyForUpdate(container);
      }
      if (trace) {
         log.tracef("Wrap %s for remove. Entry=%s", key, contextEntry);
      }
      return contextEntry;
   }

   @Override
   public final ContextEntry wrapEntryForPut(InvocationContext ctx, Object key, CacheEntry icEntry,
         boolean undeleteIfNeeded, FlagAffectedCommand cmd, boolean skipRead) throws InterruptedException {
      CacheEntry cacheEntry = getFromContext(ctx, key);
      ContextEntry contextEntry;
      Metadata providedMetadata = cmd.getMetadata();
      if (cacheEntry != null) {
         //sanity check. In repeatable read, we only deal with RepeatableReadEntry and ClusteredRepeatableReadEntry
         if (cacheEntry instanceof RepeatableReadEntry) {
            contextEntry = (ContextEntry) cacheEntry;
         } else {
            throw new IllegalStateException("Cache entry stored in context should be a RepeatableReadEntry instance " +
                                                  "but it is " + cacheEntry.getClass().getCanonicalName());
         }
         //if the icEntry is not null, then this is a remote get. We need to update the value and the metadata.
         if (!contextEntry.isRemoved() && !contextEntry.skipRemoteGet() && icEntry != null) {
            contextEntry.setValue(icEntry.getValue());
            updateVersion(contextEntry, icEntry.getMetadata());
         }
         if (!contextEntry.isRemoved() && contextEntry.isNull()) {
            //new entry
            contextEntry.setCreated(true);
         }
         //always update the metadata if needed.
         updateMetadata(contextEntry, providedMetadata);
         contextEntry.undelete(undeleteIfNeeded);
      } else {
         CacheEntry ice = (icEntry == null ? getFromContainer(key) : icEntry);
         // A putForExternalRead is putIfAbsent, so if key present, do nothing
         if (ice != null && cmd.hasFlag(Flag.PUT_FOR_EXTERNAL_READ)) {
            // make sure we record this! Null value since this is a forced lock on the key
            ctx.putLookedUpEntry(key, null);
            if (trace) {
               log.tracef("Wrap %s for put. Entry=null", key);
            }
            return null;
         }

         contextEntry = ice != null ?
             wrapCacheEntryForPut(ctx, key, ice, providedMetadata, skipRead) :
             newMvccEntryForPut(ctx, key, cmd, providedMetadata, skipRead);
      }
      contextEntry.copyForUpdate(container);
      if (trace) {
         log.tracef("Wrap %s for put. Entry=%s", key, contextEntry);
      }
      return contextEntry;
   }
   
   @Override
   public ContextEntry wrapEntryForDelta(InvocationContext ctx, Object deltaKey, Delta delta ) throws InterruptedException {
      CacheEntry cacheEntry = getFromContext(ctx, deltaKey);
      DeltaAwareCacheEntry deltaAwareEntry = null;
      if (cacheEntry != null) {        
         deltaAwareEntry = wrapEntryForDelta(ctx, deltaKey, cacheEntry);
      } else {
         CacheEntry ice = getFromContainer(deltaKey);
         if (ice != null){
            deltaAwareEntry = newDeltaAwareCacheEntry(ctx, deltaKey, (DeltaAware)ice.getValue());
         }
      }
      if (deltaAwareEntry != null)
         deltaAwareEntry.appendDelta(delta);
      if (trace) {
         log.tracef("Wrap %s for delta. Entry=%s", deltaKey, deltaAwareEntry);
      }
      return deltaAwareEntry;
   }
   
   private DeltaAwareCacheEntry wrapEntryForDelta(InvocationContext ctx, Object key, CacheEntry cacheEntry) {
      if (cacheEntry instanceof DeltaAwareCacheEntry) return (DeltaAwareCacheEntry) cacheEntry;
      return wrapCacheEntryForDelta(ctx, key, cacheEntry);
   }
   
   private DeltaAwareCacheEntry wrapCacheEntryForDelta(InvocationContext ctx, Object key, CacheEntry cacheEntry) {
      DeltaAwareCacheEntry e;
      if (cacheEntry instanceof ContextEntry) {
         e = createWrappedDeltaEntry(key, (DeltaAware) cacheEntry.getValue(), cacheEntry);
      }
      else if (cacheEntry instanceof InternalCacheEntry) {
         cacheEntry = wrapCacheEntryForPut(ctx, key, cacheEntry, null, false);
         e = createWrappedDeltaEntry(key, (DeltaAware) cacheEntry.getValue(), cacheEntry);
      }
      else {
         e = createWrappedDeltaEntry(key, (DeltaAware) cacheEntry.getValue(), null);
      }
      ctx.putLookedUpEntry(key, e);
      return e;

   }

   private ContextEntry getFromContext(InvocationContext ctx, Object key) {
      final ContextEntry cacheEntry = ctx.lookupEntry(key);
      if (trace) log.tracef("Exists in context? %s ", cacheEntry);
      return cacheEntry;
   }

   private CacheEntry getFromContainer(Object key) {
      final CacheEntry ice = container.get(key);
      if (trace) log.tracef("Retrieved from container %s", ice);
      return ice;
   }

   private ContextEntry newMvccEntryForPut(
         InvocationContext ctx, Object key, FlagAffectedCommand cmd, Metadata providedMetadata, boolean skipRead) {
      ContextEntry contextEntry;
      if (trace) log.trace("Creating new entry.");
      notifier.notifyCacheEntryCreated(key, null, true, ctx, cmd);
      contextEntry = createWrappedEntry(key, null, ctx, providedMetadata, true, false, skipRead);
      contextEntry.setCreated(true);
      ctx.putLookedUpEntry(key, contextEntry);
      return contextEntry;
   }

   private ContextEntry wrapMvccEntryForPut(InvocationContext ctx, Object key, CacheEntry cacheEntry, Metadata providedMetadata, boolean skipRead) {
      if (cacheEntry instanceof ContextEntry) {
         ContextEntry contextEntry = (ContextEntry) cacheEntry;
         updateMetadata(contextEntry, providedMetadata);
         return contextEntry;
      }
      return wrapCacheEntryForPut(ctx, key, cacheEntry, providedMetadata, skipRead);
   }

   private ContextEntry wrapCacheEntryForPut(InvocationContext ctx, Object key, CacheEntry cacheEntry, Metadata providedMetadata, boolean skipRead) {
      ContextEntry contextEntry = createWrappedEntry(key, cacheEntry, ctx, providedMetadata, false, false, skipRead);
      ctx.putLookedUpEntry(key, contextEntry);
      return contextEntry;
   }

   private ContextEntry wrapMvccEntryForRemove(InvocationContext ctx, Object key, CacheEntry cacheEntry, boolean skipRead) {
      ContextEntry contextEntry = createWrappedEntry(key, cacheEntry, ctx, null, false, true, skipRead);
      // If the original entry has changeable state, copy state flags to the new MVCC entry.
      if (cacheEntry instanceof StateChangingEntry)
         contextEntry.copyStateFlagsFrom((StateChangingEntry) cacheEntry);

      ctx.putLookedUpEntry(key, contextEntry);
      return contextEntry;
   }

   private ContextEntry wrapEntry(InvocationContext ctx, Object key, Metadata providedMetadata, boolean skipRead) {
      CacheEntry cacheEntry = getFromContext(ctx, key);
      ContextEntry contextEntry = null;
      if (cacheEntry != null) {
         //already wrapped. set skip read to true to avoid replace the current version.
         contextEntry = wrapMvccEntryForPut(ctx, key, cacheEntry, providedMetadata, true);
      } else {
         CacheEntry ice = getFromContainer(key);
         if (ice != null || clusterModeWriteSkewCheck) {
            contextEntry = wrapCacheEntryForPut(ctx, key, ice, providedMetadata, skipRead);
         }
      }
      if (contextEntry != null)
         contextEntry.copyForUpdate(container);
      return contextEntry;
   }

   protected ContextEntry createWrappedEntry(Object key, CacheEntry cacheEntry, InvocationContext context,
                                          Metadata providedMetadata, boolean isForInsert, boolean forRemoval, boolean skipRead) {
      Object value = cacheEntry != null ? cacheEntry.getValue() : null;
      Metadata metadata = providedMetadata != null
            ? providedMetadata
            : cacheEntry != null ? cacheEntry.getMetadata() : null;

      return new RepeatableReadEntry(key, value, metadata);
   }
   
   private DeltaAwareCacheEntry newDeltaAwareCacheEntry(InvocationContext ctx, Object key, DeltaAware deltaAware){
      DeltaAwareCacheEntry deltaEntry = createWrappedDeltaEntry(key, deltaAware, null);
      ctx.putLookedUpEntry(key, deltaEntry);
      return deltaEntry;
   }
   
   private DeltaAwareCacheEntry createWrappedDeltaEntry(Object key, DeltaAware deltaAware, CacheEntry entry) {
      return new DeltaAwareCacheEntry(key,deltaAware, entry);
   }

   private void updateMetadata(ContextEntry entry, Metadata providedMetadata) {
      if (trace) {
         log.tracef("Update metadata for %s. Provided metadata is %s", entry, providedMetadata);
      }
      if (providedMetadata == null || entry == null || entry.getMetadata() != null) {
         return;
      }
      entry.setMetadata(providedMetadata);
   }

   private void updateVersion(ContextEntry entry, Metadata providedMetadata) {
      if (trace) {
         log.tracef("Update metadata for %s. Provided metadata is %s", entry, providedMetadata);
      }
      if (providedMetadata == null || entry == null) {
         return;
      } else if (entry.getMetadata() == null) {
         entry.setMetadata(providedMetadata);
         return;
      }

      entry.setMetadata(Metadatas.applyVersion(entry.getMetadata(), providedMetadata));
   }

}
