package org.infinispan.reactive.publisher.impl;

import java.util.function.Function;

public interface MaybeValueRetainedFunction<I, O> extends Function<I, O> {
   /**
    * This method should return true when this function doesn't change the actual values of the Publisher. This
    * can be useful for some optimizations that may need to track produced values from the original.
    * @return if the values in the publisher are unchanged
    */
   boolean retainsOriginalValue();
}
