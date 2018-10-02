package org.infinispan;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;

import org.reactivestreams.Publisher;

import hu.akarnokd.rxjava2.interop.FlowableInterop;
import hu.akarnokd.rxjava2.interop.SingleInterop;
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
      Function<Single<Long>, CompletionStage<Long>> interop = SingleInterop.get();
      InfinispanPublisher.CachePublisherTransformer<Object, Long> transformer =
            cp -> Flowable.fromPublisher(cp).count().to(interop);
      InfinispanPublisher.CachePublisherTransformer<Long, Long> finalizer = results ->
            Flowable.fromPublisher(results).reduce((long) 0, Long::sum).to(interop);
      return infinispanPublisher.compose(transformer, finalizer);
   }

   public static <E> CompletionStage<Boolean> allMatch(InfinispanPublisher<E> infinispanPublisher, Predicate<? super E> predicate) {
      Function<Single<Boolean>, CompletionStage<Boolean>> interop = SingleInterop.get();
      InfinispanPublisher.CachePublisherTransformer<E, Boolean> transformer =
            cp -> Flowable.fromPublisher(cp).all(predicate::test).to(interop);

      InfinispanPublisher.CachePublisherTransformer<Boolean, Boolean> finalizer = results ->
         Flowable.fromPublisher(results)
               .filter(b -> b == Boolean.FALSE)
               .first(Boolean.TRUE).to(interop);

      return infinispanPublisher.compose(transformer, finalizer);
   }

   public static <E> CompletionStage<Boolean> noneMatch(InfinispanPublisher<E> infinispanPublisher, Predicate<? super E> predicate) {
      // TODO: make predicate serializable
      return allMatch(infinispanPublisher, predicate.negate());
   }

   public static <E> CompletionStage<Boolean> anyMatch(InfinispanPublisher<E> infinispanPublisher, Predicate<? super E> predicate) {
      Function<Single<Boolean>, CompletionStage<Boolean>> interop = SingleInterop.get();
      InfinispanPublisher.CachePublisherTransformer<E, Boolean> transformer =
            cp -> Flowable.fromPublisher(cp).any(predicate::test).to(interop);

      InfinispanPublisher.CachePublisherTransformer<Boolean, Boolean> finalizer = results ->
            Flowable.fromPublisher(results)
                  .filter(b -> b == Boolean.TRUE)
                  .first(Boolean.FALSE).to(interop);

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
      // TODO: do this
      return null;
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

   public static <E> Publisher<E> sorted(InfinispanPublisher<E> infinispanPublisher, Comparator<? super E> comparator) {
      InfinispanPublisher.CachePublisherTransformer<E, Publisher<E>> transformer =
            CompletableFuture::completedFuture;

      InfinispanPublisher.CachePublisherTransformer<Publisher<E>, List<E>> intermediate =
            ps -> ParallelFlowable.from(ps)
                  .flatMap(Functions.identity())
                  .toSortedList(comparator)
                  // We should always have 1 value
                  .to(FlowableInterop.single());

      // We already sorted the lists on each of the remote nodes so just merge them together
      InfinispanPublisher.PublisherFunction<List<E>, E> finalizer =
            sortedLists -> new ParallelSortedJoin<>(ParallelFlowable.from(sortedLists), comparator);

      return infinispanPublisher.compose(transformer, intermediate, finalizer);
   }

   public static <E> void subscribe(InfinispanPublisher<E> infinispanPublisher, Consumer<? super E> onNext) {
      Flowable.fromPublisher(infinispanPublisher.asPublisher()).subscribe(onNext::accept);
   }

   public static <E> void subscribe(InfinispanPublisher<E> infinispanPublisher, Consumer<? super E> onNext,
         Consumer<? super Throwable> onError) {
      Flowable.fromPublisher(infinispanPublisher.asPublisher()).subscribe(onNext::accept, onError::accept);
   }

   public static <E> void subscribe(InfinispanPublisher<E> infinispanPublisher, Consumer<? super E> onNext,
         Consumer<? super Throwable> onError, Runnable onComplete) {
      Flowable.fromPublisher(infinispanPublisher.asPublisher()).subscribe(onNext::accept, onError::accept, onComplete::run);
   }

   public static <E> Publisher<E> asPublisher(InfinispanPublisher<E> infinispanPublisher) {
      return infinispanPublisher.compose(i -> i);
   }
}
