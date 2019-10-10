package org.infinispan.reactive.publisher.impl;

import java.util.function.IntConsumer;

import org.reactivestreams.Subscriber;

@FunctionalInterface
public interface SegmentAwarePublisher<R> extends SegmentCompletionPublisher<R> {

   void subscribe(Subscriber<? super R> s, IntConsumer completedSegmentConsumer, IntConsumer lostSegmentConsumer);

   @Override
   default void subscribe(Subscriber<? super R> s, IntConsumer completedSegmentConsumer) {
      subscribe(s, completedSegmentConsumer, EMPTY_CONSUMER);
   }
}
