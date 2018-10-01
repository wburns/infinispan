package org.infinispan;

import java.io.Serializable;
import java.util.Comparator;
import java.util.concurrent.CompletionStage;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.infinispan.util.function.SerializableComparator;
import org.reactivestreams.Publisher;

/**
 * @author wburns
 * @since 10.0
 */
public interface CachePublisher<T> {

   // Intermediate methods
   <R> CachePublisher<R> map(Function<? super T, ? extends R> function);

   CachePublisher<T> filter(Predicate<? super T> predicate);

   // Terminal Operations

   <R, S> Publisher<S> composeFlatten(CachePublisherTransformer<? super T, ? extends R> transformer,
         Function<R, Publisher<? extends S>> combiner);

   /**
    * Advanced method that can be used to perform a remote transformation via <b>transformer</b>. The result is then
    * sent back to the originator. The
    * @param transformer
    * @param combiner
    * @param <R>
    * @return
    */
   <R> Publisher<R> composeReduce(CachePublisherTransformer<? super T, ? extends R> transformer,
         BinaryOperator<R> combiner);

   void subscribe(Consumer<? super T> consumer);

   // Min and max are implemented via sorted(Comparator) with comparator correct direction and then request
   // only so many entries from publisher

   Publisher<T> sorted();

   Publisher<T> sorted(Comparator<? super T> comparator);

   Publisher<T> sorted(SerializableComparator<? super T> comparator);

   Publisher<T> asPublisher();

   @FunctionalInterface
   interface CachePublisherTransformer<T, R> {
      Publisher<R> transform(CachePublisher<T> publisher);
   }

   /**
    *
    * @param <T>
    * @param <R>
    */
   @FunctionalInterface
   interface SerializableCachePublisherTransformer<T, R> extends CachePublisherTransformer<T, R>, Serializable {
   }
}
