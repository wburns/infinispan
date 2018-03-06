package org.infinispan.commands.read;

import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.ToIntFunction;

import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.container.DataContainer;
import org.infinispan.container.SegmentedDataContainer;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.stream.impl.local.KeyStreamSupplier;
import org.infinispan.stream.impl.local.LocalCacheStream;
import org.infinispan.stream.impl.local.SegmentedKeyStreamSupplier;
import org.infinispan.util.DataContainerRemoveIterator;

/**
 * Command implementation for {@link java.util.Map#keySet()} functionality.
 *
 * @author Galder Zamarreño
 * @author Mircea.Markus@jboss.com
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @author William Burns
 * @since 4.0
 */
public class KeySetCommand<K, V> extends AbstractLocalCommand implements VisitableCommand {
   private final Cache<K, V> cache;

   public KeySetCommand(Cache<K, V> cache, long flagsBitSet) {
      setFlagsBitSet(flagsBitSet);
      cache = AbstractDelegatingCache.unwrapCache(cache);
      if (flagsBitSet != EnumUtil.EMPTY_BIT_SET) {
         this.cache = cache.getAdvancedCache().withFlags(EnumUtil.enumArrayOf(flagsBitSet, Flag.class));
      } else {
         this.cache = cache;
      }
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitKeySetCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Set<K> perform(InvocationContext ctx) throws Throwable {
      return new BackingKeySet<>(cache);
   }

   @Override
   public String toString() {
      return "KeySetCommand{" +
            "cache=" + cache.getName() +
            ", flags=" + printFlags() +
            '}';
   }

   private static class BackingKeySet<K, V> extends AbstractCloseableIteratorCollection<K, K, V> implements CacheSet<K> {

      public BackingKeySet(Cache<K, V> cache) {
         super(cache);
      }

      @Override
      public CloseableIterator<K> iterator() {
         return new IteratorMapper<>(new DataContainerRemoveIterator<>(cache), Map.Entry::getKey);
      }

      @Override
      public CloseableSpliterator<K> spliterator() {
         return Closeables.spliterator(iterator(), cache.getAdvancedCache().getDataContainer().sizeIncludingExpired(),
                 Spliterator.CONCURRENT | Spliterator.DISTINCT | Spliterator.NONNULL);
      }

      @Override
      public int size() {
         return cache.getAdvancedCache().getDataContainer().size();
      }

      @Override
      public boolean contains(Object o) {
         return cache.containsKey(o);
      }

      @Override
      public boolean remove(Object o) {
         return cache.remove(o) != null;
      }

      private ToIntFunction<Object> getSegmentMapper(Cache<K, V> cache) {
         DistributionManager dm = cache.getAdvancedCache().getDistributionManager();
         if (dm != null) {
            return dm.getCacheTopology()::getSegment;
         }
         return null;
      }

      private CacheStream<K> stream(boolean parallel) {
         DataContainer<K, V> dc = cache.getAdvancedCache().getDataContainer();
         if (dc instanceof SegmentedDataContainer) {
            SegmentedDataContainer<K, V> segmentedDataContainer = (SegmentedDataContainer) dc;
            return new LocalCacheStream<>(new SegmentedKeyStreamSupplier<>(cache, getSegmentMapper(cache),
                  segmentedDataContainer), parallel, cache.getAdvancedCache().getComponentRegistry());
         } else {
            return new LocalCacheStream<>(new KeyStreamSupplier<>(cache, getSegmentMapper(cache),
                  super::stream), parallel, cache.getAdvancedCache().getComponentRegistry());
         }
      }

      @Override
      public CacheStream<K> stream() {
         return stream(false);
      }

      @Override
      public CacheStream<K> parallelStream() {
         return stream(true);
      }
   }
}
