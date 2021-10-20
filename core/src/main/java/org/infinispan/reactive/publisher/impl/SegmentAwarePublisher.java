package org.infinispan.reactive.publisher.impl;

import org.reactivestreams.Subscriber;

/**
 * This is the same as {@link SegmentCompletionPublisher} except that it also allows listening for when a segment is
 * lost. The lost segment provides the same notification guarantees as the segment completion of the parent interface.
 * <p>
 * This interface is normally just for internal Infinispan usage as users shouldn't normally have to care about retrying.
 * <p>
 * Implementors of this do not do retries and instead notify of lost segments instead of retrying, which implementors
 * of {@link SegmentCompletionPublisher} normally do.
 *
 * @param <R> value type
 */
public interface SegmentAwarePublisher<R> extends SegmentCompletionPublisher<R> {

   interface NotificationWithLost<R> extends SegmentCompletionPublisher.Notification<R> {
      boolean isLostSegment();

      int lostSegment();
   }

   /**
    * Same as {@link SegmentCompletionPublisher#subscribeWithSegments(Subscriber)} , except that we also can notify a
    * listener when a segment has been lost before publishing all its entries
    *
    * @param subscriber subscriber to be notified of values, segment completion and segment lost
    */
   void subscribeWithLostSegments(Subscriber<? super NotificationWithLost<R>> subscriber);

   @Override
   default void subscribeWithSegments(Subscriber<? super Notification<R>> subscriber) {

   }

   /**
    * When this method is used the {@link DeliveryGuarantee} is ignored as the user isn't listening to completion or
    * lost segments
    *
    * @param s
    */
   @Override
   void subscribe(Subscriber<? super R> s);
}
