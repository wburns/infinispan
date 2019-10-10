package org.infinispan.reactive.publisher.impl;

import java.util.function.IntConsumer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

@FunctionalInterface
public interface SegmentCompletionPublisher<R> extends Publisher<R> {
   IntConsumer EMPTY_CONSUMER = v -> { };

   void subscribe(Subscriber<? super R> s, IntConsumer completedSegmentConsumer);

   @Override
   default void subscribe(Subscriber<? super R> s) {
      subscribe(s, EMPTY_CONSUMER);
   }
}
