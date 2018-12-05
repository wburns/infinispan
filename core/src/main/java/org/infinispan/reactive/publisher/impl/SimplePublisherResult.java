package org.infinispan.reactive.publisher.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.util.IntSet;

/**
 * @author wburns
 * @since 10.0
 */
public class SimplePublisherResult<R> implements PublisherResult<R> {
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

   public static class Externalizer implements AdvancedExternalizer<SimplePublisherResult> {

      @Override
      public Set<Class<? extends SimplePublisherResult>> getTypeClasses() {
         return Collections.singleton(SimplePublisherResult.class);
      }

      @Override
      public Integer getId() {
         return Ids.SIMPLE_PUBLISHER_RESULT;
      }

      @Override
      public void writeObject(ObjectOutput output, SimplePublisherResult object) throws IOException {
         output.writeObject(object.suspectedSegments);
         output.writeObject(object.result);
      }

      @Override
      public SimplePublisherResult readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new SimplePublisherResult((IntSet) input.readObject(), input.readObject());
      }
   }
}
