package org.infinispan.util.rxjava;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import io.reactivex.processors.AsyncProcessor;

/**
 * TODO: this class should maybe be public?
 * @author wburns
 * @since 10.0
 */
public class RxJavaInterop {
   private RxJavaInterop() { }

   private static final Function<Single<Object>, CompletionStage<Object>> singleToCompletionStage = single -> {
      CompletableFuture<Object> cf = new CompletableFuture<>();
      single.subscribe(cf::complete, cf::completeExceptionally);
      return cf;
   };

   private static final Function<Maybe<Object>, CompletionStage<Object>> maybeToCompletionStage = maybe -> {
      CompletableFuture<Object> cf = new CompletableFuture<>();
      maybe.subscribe(cf::complete, cf::completeExceptionally, () -> cf.complete(null));
      return cf;
   };

   private static final java.util.function.Function<CompletionStage<Object>, Flowable<Object>> completionStageToPublisher = stage -> {
      AsyncProcessor<Object> asyncProcessor = AsyncProcessor.create();
      stage.whenComplete((value, t) -> {
         if (t != null) {
            asyncProcessor.onError(t);
         } else {
            asyncProcessor.onNext(value);
            asyncProcessor.onComplete();
         }
      });
      return asyncProcessor;
   };

   /**
    * Provides an interop function that can be used to convert a Single into a CompletionStage. Note that this function
    * is not from the standard java.util.function package, but rather {@link Function} to interop better with the
    * {@link Single#to(Function)} method.
    * @param <E> underlying type
    * @return rxjava function to convert Single to CompletionStage
    */
   public static <E> Function<Single<E>, CompletionStage<E>> singleToCompletionStage() {
      return (Function) singleToCompletionStage;
   }

   /**
    * Provides an interop function that can be used to convert a Maybe into a CompletionStage. Note that this function
    * is not from the standard java.util.function package, but rather {@link Function} to interop better with the
    * {@link Maybe#to(Function)} method.
    * @param <E> underlying type
    * @return rxjava function to convert Maybe to CompletionStage
    */
   public static <E> Function<Maybe<E>, CompletionStage<E>> maybeToCompletionStage() {
      return (Function) maybeToCompletionStage;
   }

   /**
    * Provides an interop function that can be used to convert a CompletionStage into a Flowable. Note that this function
    * is from the standard java.util.function package since we don't want the method to throw an exception
    * <p>
    * Remember that the CompletionStage when completing normally <b>MUST</b> have a non null value!
    * @param <E> underlying type
    * @return rxjava function to convert CompletionStage to Flowable
    */
   public static <E> java.util.function.Function<CompletionStage<E>, Flowable<E>> completionStageToPublisher() {
      return (java.util.function.Function) completionStageToPublisher;
   }
}
