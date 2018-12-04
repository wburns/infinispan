package org.infinispan;

import org.infinispan.commons.util.CloseableIteratorCollection;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;

/**
 * A collection type that returns special Cache based streams that have additional options to tweak behavior.
 * @param <E> The type of the collection
 * @since 8.0
 */
public interface CacheCollection<E> extends CloseableIteratorCollection<E> {
   @Override
   CacheStream<E> stream();

   @Override
   CacheStream<E> parallelStream();

   /**
    * Returns a local only (loader/store) based publisher that is ran only on the local node with no additional
    * threading, except possibly for the loader.
    */
   default Publisher<E> localPublisher(int segment) {
      return localPublisher(IntSets.immutableSet(segment));
   }

   default Publisher<E> localPublisher(IntSet segments) {
      return Flowable.fromIterable(() -> stream().filterKeySegments(segments).iterator());
   }
}
