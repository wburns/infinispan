package org.infinispan.reactive.publisher.impl;

import java.io.Serializable;

import org.infinispan.commons.util.IntSet;

/**
 * @author wburns
 * @since 10.0
 */
public class SimplePublisherResult<R> implements PublisherResult<R>, Serializable {
   private final IntSet suspectedSegments;

   private final R result;

   public SimplePublisherResult(IntSet suspectedSegments, R result) {
      this.suspectedSegments = suspectedSegments;
      this.result = result;
   }

   @Override
   public IntSet getSuspectedSegments() {
      return suspectedSegments;
   }

   @Override
   public R getResult() {
      return result;
   }

   @Override
   public String toString() {
      return "SimplePublisherResult{" +
            "result=" + result +
            ", suspectedSegments=" + suspectedSegments +
            '}';
   }

// TODO: have to implement externalizer
}
