package org.infinispan.stream.impl;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

/**
 * Coordinator that is used to handle operations that require retrying after a segment has been lost.
 */
public class SegmentRetryingCoordinator<E> {
   private static final Log log = LogFactory.getLog(SegmentRetryingCoordinator.class);
   private static final BaseStream<?, ?> EMPTY = Stream.empty();

   private final Function<BaseStream<?, ?>, E> function;
   private final Supplier<? extends BaseStream<?, ?>> supplier;

   private final AtomicReference<BaseStream<?, ?>> streamRef = new AtomicReference<>(EMPTY);
   private final AtomicBoolean continueTrying = new AtomicBoolean(true);

   public SegmentRetryingCoordinator(Function<BaseStream<?, ?>, E> function,
           Supplier<? extends BaseStream<?, ?>> supplier) {
      this.function = function;
      this.supplier = supplier;
   }

   public boolean lostSegment(boolean allSegmentsLost) {
      BaseStream<?, ?> oldStream = streamRef.get();
      continueTrying.set(!allSegmentsLost);
      boolean affected;
      if (oldStream != null) {
         // If the stream was non null and wasn't empty that means we were processing it at the time of the segment
         // being lost - so we tell that one to close
         if (oldStream != EMPTY) {
            // This can only fail if the operation completes concurrently
            if ((affected = streamRef.compareAndSet(oldStream, EMPTY))) {
               // This can short circuit some things like sending a response or waiting for retrieval from a
               // cache loader
               oldStream.close();
            }
         } else {
            affected = true;
         }
      } else {
         affected = false;
      }
      return affected;
   }

   public E runOperation() {
      boolean keepTrying = true;
      BaseStream<?, ?> stream;
      E value;
      do {
         stream = supplier.get();
         streamRef.set(stream);
         value = function.apply(stream);
         log.trace("Completed an operation, trying to see if we are done.");
      } while (!streamRef.compareAndSet(stream, null) && (keepTrying = continueTrying.get()));
      log.tracef("Operation now done, due to try denial: " + !keepTrying);
      return keepTrying ? value : null;
   }
}
