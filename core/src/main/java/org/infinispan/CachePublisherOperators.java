package org.infinispan;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;

import org.infinispan.util.Closeables;
import org.infinispan.util.rxjava.RxJavaInterop;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import io.reactivex.internal.functions.Functions;
import io.reactivex.internal.operators.parallel.ParallelSortedJoin;
import io.reactivex.parallel.ParallelFlowable;

/**
 * @author wburns
 * @since 10.0
 */
public class CachePublisherOperators {

   public static CompletionStage<Long> count(InfinispanPublisher<?> infinispanPublisher) {
      java.util.function.Function<Publisher<?>, CompletionStage<Long>> transformer = cp ->
            Flowable.fromPublisher(cp)
                  .count()
                  .to(RxJavaInterop.singleToCompletionStage());
      java.util.function.Function<Publisher<Long>, CompletionStage<Long>> finalizer = results ->
            Flowable.fromPublisher(results)
                  .reduce((long) 0, Long::sum)
                  .to(RxJavaInterop.singleToCompletionStage());
      return infinispanPublisher.compose2(transformer, finalizer);
   }

   public static <E> CompletionStage<Boolean> allMatch(InfinispanPublisher<E> infinispanPublisher, Predicate<? super E> predicate) {
      Function<Single<Boolean>, CompletionStage<Boolean>> interop = RxJavaInterop.singleToCompletionStage();
      InfinispanPublisher.CachePublisherTransformer<E, Boolean> transformer = cp ->
            Flowable.fromPublisher(cp)
                  .all(predicate::test)
                  .to(interop);

      InfinispanPublisher.CachePublisherTransformer<Boolean, Boolean> finalizer = results ->
            Flowable.fromPublisher(results)
                  .filter(b -> b == Boolean.FALSE)
                  .first(Boolean.TRUE)
                  .to(interop);

      return infinispanPublisher.compose(transformer, finalizer);
   }

   public static <E> CompletionStage<Boolean> noneMatch(InfinispanPublisher<E> infinispanPublisher, Predicate<? super E> predicate) {
      // TODO: make predicate serializable
      return allMatch(infinispanPublisher, predicate.negate());
   }

   public static <E> CompletionStage<Boolean> anyMatch(InfinispanPublisher<E> infinispanPublisher, Predicate<? super E> predicate) {
      Function<Single<Boolean>, CompletionStage<Boolean>> interop = RxJavaInterop.singleToCompletionStage();
      InfinispanPublisher.CachePublisherTransformer<E, Boolean> transformer = cp ->
            Flowable.fromPublisher(cp)
                  .any(predicate::test)
                  .to(interop);

      InfinispanPublisher.CachePublisherTransformer<Boolean, Boolean> finalizer = results ->
            Flowable.fromPublisher(results)
                  .filter(b -> b == Boolean.TRUE)
                  .first(Boolean.FALSE)
                  .to(interop);

      return infinispanPublisher.compose(transformer, finalizer);
   }

   public static <E> Publisher<E> min(InfinispanPublisher<E> infinispanPublisher, Comparator<? super E> comparator,
         int numberToReturn) {
      return max(infinispanPublisher, comparator.reversed(), numberToReturn);
   }

   public static <E> Publisher<E> max(InfinispanPublisher<E> infinispanPublisher, Comparator<? super E> comparator,
         int numberToReturn) {
      Publisher<E> publisher = infinispanPublisher.distributedBatchSize(numberToReturn).sorted(comparator);

      return Flowable.fromPublisher(publisher).take(numberToReturn);
   }

   public static <E, A> CompletionStage<A> collect(InfinispanPublisher<E> infinispanPublisher,
         Supplier<A> supplier, BiConsumer<A,? super E> accumulator, BiConsumer<A, A> combiner) {
      InfinispanPublisher.CachePublisherTransformer<E, A> transformer = cp ->
            Flowable.fromPublisher(cp)
                  .collect(supplier::get, accumulator::accept)
                  .to(RxJavaInterop.singleToCompletionStage());

      InfinispanPublisher.CachePublisherTransformer<A, A> finalizer = results ->
            Flowable.fromPublisher(results)
                  .reduce((first, second) -> {
                     combiner.accept(first, second);
                     return first;
                  })
                  // The provided publisher is always > 0, so this cannot return a null value
                  .to(RxJavaInterop.maybeToCompletionStage());

      return infinispanPublisher.compose2(transformer, finalizer);
   }

   public static <E, A, R> CompletionStage<R> collect(InfinispanPublisher<E> infinispanPublisher,
         Collector<? super E, A, R> collector) {
      // TODO: need to redo this
//      Callable<A> callable = () -> collector.supplier().get();
//      BiFunction<A, E, A> biFunction = (accumulated, element) -> {
//         collector.accumulator().accept(accumulated, element);
//         return accumulated;
//      };
//      InfinispanPublisher.CachePublisherTransformer<E, A> transformer =
//            cp -> Flowable.fromPublisher(cp).reduceWith(callable, biFunction).toFlowable();
//      BiFunction<A, A, A> combiner = (a, b) -> collector.combiner().apply(a, b);
//      InfinispanPublisher.PublisherFunction<A, A> finalizer = results ->
//            Flowable.fromPublisher(results).reduce(combiner).toFlowable();
//
//      Publisher<A> published = infinispanPublisher.compose(transformer, finalizer);
//      if (collector.characteristics().contains(Collector.Characteristics.IDENTITY_FINISH)) {
//         return (Publisher<R>) published;
//      } else {
//         return Flowable.fromPublisher(published).map(a -> collector.finisher().apply(a));
//      }
      return null;
   }

   /**
    * This method requires bringing all entries into memory at the same time, very bad
    * @param infinispanPublisher
    * @param comparator
    * @param <E>
    * @return
    */
   public static <E> Flowable<E> sorted(InfinispanPublisher<E> infinispanPublisher, Comparator<? super E> comparator) {
      java.util.function.Function<Publisher<E>, CompletionStage<List<E>>> transformer =
            publisher -> Flowable.fromPublisher(publisher)
                           .toSortedList(comparator)
                           .to(RxJavaInterop.singleToCompletionStage());

      java.util.function.Function<Publisher<List<E>>, CompletionStage<List<E>>> finalizer =
            // We use new ParallelSortedJoin directly, since the lists were presorted in the transformer
            publisherLists -> new ParallelSortedJoin<>(ParallelFlowable.from(publisherLists), comparator)
                  // Turn it back into a list for our consumption
                  .toList()
                  .to(RxJavaInterop.singleToCompletionStage());

      return RxJavaInterop.<List<E>>completionStageToPublisher().apply(
            infinispanPublisher.compose2(transformer, finalizer))
            .flatMapIterable(Functions.identity());
   }

   public static <E> AutoCloseable subscribe(InfinispanPublisher<E> infinispanPublisher, Consumer<? super E> onNext) {
      return Closeables.autoCloseable(Flowable.fromPublisher(infinispanPublisher.asRsPublisher())
            .subscribe(onNext::accept));
   }

   public static <E> AutoCloseable subscribe(InfinispanPublisher<E> infinispanPublisher, Consumer<? super E> onNext,
         Consumer<? super Throwable> onError) {
      return Closeables.autoCloseable(Flowable.fromPublisher(infinispanPublisher.asRsPublisher())
            .subscribe(onNext::accept, onError::accept));
   }

   public static <E> AutoCloseable subscribe(InfinispanPublisher<E> infinispanPublisher, Consumer<? super E> onNext,
         Consumer<? super Throwable> onError, Runnable onComplete) {
      return Closeables.autoCloseable(Flowable.fromPublisher(infinispanPublisher.asRsPublisher())
            .subscribe(onNext::accept, onError::accept, onComplete::run));
   }

   public static <E> Publisher<E> asPublisher(InfinispanPublisher<E> infinispanPublisher) {
      return infinispanPublisher.compose2(i -> i);
   }
}
