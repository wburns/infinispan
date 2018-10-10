package org.infinispan;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.infinispan.commons.util.IntSet;
import org.infinispan.util.function.SerializableComparator;
import org.infinispan.util.function.SerializablePredicate;
import org.reactivestreams.Publisher;

/**
 * TODO: need to mention issues with CompletionStage (ie. toCompletableFuture().join() and also async without Executor
 * @author wburns
 * @since 10.0
 */
public interface InfinispanPublisher<T> {

   // Intermediate methods
   <R> InfinispanPublisher<R> map(Function<? super T, ? extends R> function);

   <R> InfinispanPublisher<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> function);

   InfinispanPublisher<T> filter(Predicate<? super T> predicate);

   InfinispanPublisher<T> distributedBatchSize(int batchSize);

   InfinispanPublisher<T> filterKeySegments(IntSet segments);

   InfinispanPublisher<T> filterKeys(Set<?> keys);

   InfinispanPublisher<T> parallel();

   InfinispanPublisher<T> sequential();

   boolean isParallel();

   /**
    *
    * @return
    */
   UnsafeInfinispanPublisher<T> atMostOnceDelivery();

   /**
    *
    * @return
    */
   UnsafeInfinispanPublisher<T> atLeastOnceDelivery();

   // Terminal Operations

   default Publisher<T> min(Comparator<? super T> comparator, int amountToReturn) {
      return CachePublisherOperators.min(this, comparator, amountToReturn);
   }

   default Publisher<T> min(SerializableComparator<? super T> comparator, int amountToReturn) {
      return min((Comparator<? super T>) comparator, amountToReturn);
   }

   default CompletionStage<Long> count() {
      return CachePublisherOperators.count(this);
   }

   default CompletionStage<Boolean> allMatch(Predicate<? super T> predicate) {
      return CachePublisherOperators.allMatch(this, predicate);
   }

   default CompletionStage<Boolean> allMatch(SerializablePredicate<? super T> predicate) {
      return allMatch((Predicate<? super T>) predicate);
   }

   default CompletionStage<Boolean> anyMatch(Predicate<? super T> predicate) {
      return CachePublisherOperators.anyMatch(this, predicate);
   }

   default CompletionStage<Boolean> anyMatch(SerializablePredicate<? super T> predicate) {
      return anyMatch((Predicate<? super T>) predicate);
   }

   default CompletionStage<Boolean> noneMatch(Predicate<? super T> predicate) {
      return CachePublisherOperators.noneMatch(this, predicate);
   }

   default CompletionStage<Boolean> noneMatch(SerializablePredicate<? super T> predicate) {
      return noneMatch((Predicate<? super T>) predicate);
   }

   /**
    * Same as {@link #sorted(Comparator)} except it uses the natural ordering for the elements. This method set
    * the {@link org.reactivestreams.Subscriber#onError(Throwable)} with a {@link ClassCastException} if any element
    * does not implement {@link Comparable} or if the elements are not comparable with each other (ie. String and Integer).
    * @return
    */
   default Publisher<T> sorted() {
      // TODO: this is not serializable
      return sorted((Comparator<? super T>) Comparator.naturalOrder());
   }

   /**
    * Sorts the elements by using the provided comparator to order the elements. All
    * elements present in the InfinispanPublisher must implement the {@link Comparable} interface. If the publisher contains an
    * element that doesn't implement this interface or the two elements cannot be compared (ie. String and Integer)
    * the resulting Publisher will set the error status via {@link org.reactivestreams.Subscriber#onError(Throwable)}.
    * <p>
    * Note that it is not possible to perform this sort in a distributed fashion on an exactly-once InfinispanPublisher that
    * previously invoked the {@link #flatMap(Function)} operation. Doing so will cause it to sort all elements in
    * memory on the local node. If you wish to utilize sorting elements that have had a flatMap applied, it is
    * recommended to do this on an {@link UnsafeInfinispanPublisher} by invoking either {@link #atLeastOnceDelivery()} or
    * {@link #atMostOnceDelivery()}, but note that this can miss or
    * produce duplicates depending on which delivery mode is chosen (if a cache view change occurs)
    * @implSpec
    * The default implementation sorts all entries in local memory and is equivalent to, for this {@code cachePublisher}:
    * <pre> {@code
    * return CachePublisherOperators.sorted(this, comparator)}</pre>
    * @param comparator
    * @return
    */
   default Publisher<T> sorted(Comparator<? super T> comparator) {
      return CachePublisherOperators.sorted(this, comparator);
   }

   /**
    * Shortcut method that provides a way for the user to <b>subscribe</b> to the publisher where each element will
    * be presented to the consumer. Note that this is an asynchronous method and this consumer will <b>not</b> be
    * invoked in this thread. This method may be invoked from different threads for the same <b>consumer</b>, but
    * will not invoke this method concurrently and are published in a thread safe way.
    * <p>
    * Note that the consumer does not need to be marshallable and all invocations upon it will be done in this node.
    * @param consumer callback to be notified of all elements published
    */
   default AutoCloseable subscribe(Consumer<? super T> consumer) {
      return CachePublisherOperators.subscribe(this, consumer);
   }

   /**
    *
    * @param onNext
    * @param onError
    */
   default AutoCloseable subscribe(Consumer<? super T> onNext, Consumer<? super Throwable> onError) {
      return CachePublisherOperators.subscribe(this, onNext, onError);
   }

   /**
    *
    * @param onNext
    * @param onError
    * @param onComplete
    */
   default AutoCloseable subscribe(Consumer<? super T> onNext, Consumer<? super Throwable> onError, Runnable onComplete) {
      return CachePublisherOperators.subscribe(this, onNext, onError, onComplete);
   }

   /**
    *
    * @return
    */
   default Publisher<T> asRsPublisher() {
      return CachePublisherOperators.asPublisher(this);
   }

   // Compose methods that drive all of the above terminal methods

   /**
    * Advanced method that can be used to perform a remote transformation via <b>transformer</b>. Note this method
    * behaves differently and has different marshalling restrictions when this InfinispanPublisher is parallel or not. In
    * either case the <b>transformer</b> must be marshallable when using a clustered cache.
    * <p>
    * When the InfinispanPublisher is {@link #sequential()} the <b>transformer</b> is ran on each node and the resulting
    * CompletionStage value is sent as a response. All of those results are then fed into the
    * Publisher provided via the <b>finalizer</b> and condensed to create the resulting Publisher. Since the
    * <b>finalizer</b> is only ran on the originating node it doesn't need to be marshallable.
    * <p>
    * When the InfinispanPublisher is {@link #parallel()} the <b>transformer</b> is ran on each node but is ran for every
    * segment that node is a primary owner of. The results from the Publishers are then sent downstream to the
    * <b>finalizer</b> on each node to be processed further to create a single Publisher response. This result is fully
    * consume
    * @param transformer
    * @param finalizer
    * @param <R>
    * @return
    */
   <R> CompletionStage<R> compose(CachePublisherTransformer<? super T, ? extends R> transformer,
         CachePublisherTransformer<? super R, ? extends R> finalizer);

   /**
    *
    * @param transformer
    * @param intermediate
    * @param finalizer
    * @param <R>
    * @param <S>
    * @param <U>
    * @return
    */
   <R, S, U> Publisher<U> compose(CachePublisherTransformer<? super T, ? extends R> transformer,
         CachePublisherTransformer<? super R, ? extends S> intermediate,
         PublisherFunction<? super S, ? extends U> finalizer);

   /**
    * Advanced method that provides end to end streaming of data. The <b>publisherFunction</b> can be fired multiple
    * times from any node. Every published element from the returned Publisher will be returned back to the originator.
    * The number retrieved is controlled by how many elements are
    * requested via {@link org.reactivestreams.Subscription#request(long)}. However only
    * {@link #distributedBatchSize(int)} of elements will be returned at a time internally, to prevent
    * {@link OutOfMemoryError} when the subscription requests a large number such as {@link Long#MAX_VALUE}.
    * <p>
    * This method can be quite expensive when this is an exactly-once InfinispanPublisher as it must retain keys for the
    * elements to guarantee data consistency under cache view changes. It is therefore recommended to use
    * {@link #compose(CachePublisherTransformer, CachePublisherTransformer)} or
    * {@link #compose(CachePublisherTransformer, CachePublisherTransformer, PublisherFunction)} whenever possible as
    * they have a severely reduced cost of guaranteeing exactly-once semantics. The other option is to use an
    * {@link UnsafeInfinispanPublisher} which does not have these requirements to hold the keys for elements. Depending on
    * data requirements this can be acquired by invoking {@link #atLeastOnceDelivery()} or {@link #atMostOnceDelivery()}.
    * @param publisherFunction
    * @param <R>
    * @return
    */
   <R> Publisher<R> compose(PublisherFunction<? super T, ? extends R> publisherFunction);

   @FunctionalInterface
   interface CachePublisherTransformer<T, R> {
      CompletionStage<R> transform(Publisher<T> publisher);
   }

   /**
    *
    * @param <T>
    * @param <R>
    */
   @FunctionalInterface
   interface SerializableCachePublisherTransformer<T, R> extends CachePublisherTransformer<T, R>, Serializable {
   }

   interface PublisherFunction<T, R> extends Function<Publisher<T>, Publisher<R>> {

   }
}
