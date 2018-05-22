package org.infinispan.container;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.infinispan.commons.util.ComposeArraySpliterator;
import org.infinispan.commons.util.ConcatIterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.InternalCacheEntry;

/**
 * @author wburns
 * @since 9.0
 */
public class L1DefaultSegmentedDataContainer<K, V> extends DefaultSegmentedDataContainer<K, V> {
   private final ConcurrentMap<K, InternalCacheEntry<K, V>> nonOwnedEntries;

   public L1DefaultSegmentedDataContainer(int numSegments) {
      super(numSegments);
      this.nonOwnedEntries = new ConcurrentHashMap<>();
   }

   @Override
   protected ConcurrentMap<K, InternalCacheEntry<K, V>> getMapForSegment(int segment) {
      ConcurrentMap<K, InternalCacheEntry<K, V>> map = super.getMapForSegment(segment);
      if (map == null) {
         map = nonOwnedEntries;
      }
      return map;
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iteratorIncludingExpired(IntSet segments) {
      // TODO: explore creating streaming approach to not create this list?
      List<Collection<InternalCacheEntry<K, V>>> valueIterables = new ArrayList<>(segments.size() + 1);
      PrimitiveIterator.OfInt iter = segments.iterator();
      boolean includeOthers = false;
      while (iter.hasNext()) {
         int segment = iter.nextInt();
         ConcurrentMap<K, InternalCacheEntry<K, V>> map = maps.get(segment);
         if (map != null) {
            valueIterables.add(map.values());
         } else {
            includeOthers = true;
         }
      }
      if (includeOthers) {
         // TODO: this needs to be fixed
         valueIterables.add(nonOwnedEntries.values().stream()
               .filter(e -> segments.contains(getSegmentForKey(e.getKey())))
               .collect(Collectors.toSet()));
      }
      return new ConcatIterator<>(valueIterables);
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iteratorIncludingExpired() {
      List<Collection<InternalCacheEntry<K, V>>> valueIterables = new ArrayList<>(maps.length() + 1);
      for (int i = 0; i < maps.length(); ++i) {
         ConcurrentMap<K, InternalCacheEntry<K, V>> map = maps.get(i);
         if (map != null) {
            valueIterables.add(map.values());
         }
      }
      valueIterables.add(nonOwnedEntries.values());
      return new ConcatIterator<>(valueIterables);
   }

   @Override
   public Spliterator<InternalCacheEntry<K, V>> spliteratorIncludingExpired(IntSet segments) {
      // Copy the ints into an array to parallelize them
      int[] segmentArray = segments.toIntArray();
      AtomicBoolean usedOthers = new AtomicBoolean(false);

      return new ComposeArraySpliterator<>(i -> {
         ConcurrentMap<K, InternalCacheEntry<K, V>> map = maps.get(segmentArray[i]);
         if (map == null) {
            if (!usedOthers.getAndSet(true)) {
               return nonOwnedEntries.values().stream()
                     .filter(e -> segments.contains(getSegmentForKey(e.getKey())))
                     .collect(Collectors.toSet());
            }
            return Collections.emptyList();
         }
         return map.values();
      }, segmentArray.length, Spliterator.CONCURRENT | Spliterator.NONNULL | Spliterator.DISTINCT);
   }

   @Override
   public Spliterator<InternalCacheEntry<K, V>> spliteratorIncludingExpired() {
      AtomicBoolean usedOthers = new AtomicBoolean(false);
      return new ComposeArraySpliterator<>(i -> {
         ConcurrentMap<K, InternalCacheEntry<K, V>> map = maps.get(i);
         if (map == null) {
            if (!usedOthers.getAndSet(true)) {
               return nonOwnedEntries.values();
            }
            return Collections.emptyList();
         }
         return map.values();
      }, maps.length(), Spliterator.CONCURRENT | Spliterator.NONNULL | Spliterator.DISTINCT);
   }

   @Override
   public void clear() {
      nonOwnedEntries.clear();
      super.clear();
   }

   /**
    * Clears entries out of caffeine map by invoking remove on iterator. This can either keep all keys that match the
    * provided segments when keepSegments is <code>true</code> or it will remove only the provided segments when
    * keepSegments is <code>false</code>.
    * @param segments the segments to either remove or keep
    */
   @Override
   public void clear(IntSet segments) {
      for (Iterator<?> iterator = iteratorIncludingExpired(segments); iterator.hasNext(); ) {
         iterator.next();
         iterator.remove();
      }
   }

   @Override
   public void removeSegments(IntSet segments) {
      if (!segments.isEmpty()) {
         nonOwnedEntries.clear();
      }
      super.removeSegments(segments);
   }
}
