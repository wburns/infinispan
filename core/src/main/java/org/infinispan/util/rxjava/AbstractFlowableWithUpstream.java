package org.infinispan.util.rxjava;

import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.reactivex.internal.functions.ObjectHelper;
import io.reactivex.internal.fuseable.HasUpstreamPublisher;

/**
 * @author wburns
 * @since 10.0
 */
// Copied from rxjava 2.2.2 since it is package protected for some reason
abstract class AbstractFlowableWithUpstream<T, R> extends Flowable<R> implements HasUpstreamPublisher<T> {

   /**
    * The upstream source Publisher.
    */
   protected final Flowable<T> source;

   /**
    * Constructs a FlowableSource wrapping the given non-null (verified)
    * source Publisher.
    * @param source the source (upstream) Publisher instance, not null (verified)
    */
   AbstractFlowableWithUpstream(Flowable<T> source) {
      this.source = ObjectHelper.requireNonNull(source, "source is null");
   }

   @Override
   public final Publisher<T> source() {
      return source;
   }
}
