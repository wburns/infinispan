package org.infinispan.util.rxjava;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.reactivex.Single;
import io.reactivex.functions.Function;

/**
 * @author wburns
 * @since 10.0
 */
public class RxJavaInterop {
   private RxJavaInterop() {
      // Static methods only
   }

   private static final Function<Single, CompletionStage> singleToStage = c -> {
      CompletableFuture cf = new CompletableFuture();
      c.subscribe(cf::complete, cf::completeExceptionally);
      return cf;
   };

   /**
    * Function that can be used with {@link Single#to(io.reactivex.functions.Function)} to convert a Single into
    * a CompletionStage.
    * @param <R> return value type
    * @return function
    */
   public static <R> Function<Single<R>, CompletionStage<R>> singleToStage() {
      return (Function) singleToStage;
   }

}
