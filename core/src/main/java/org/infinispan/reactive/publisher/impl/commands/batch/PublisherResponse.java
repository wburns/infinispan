package org.infinispan.reactive.publisher.impl.commands.batch;

import java.util.function.ObjIntConsumer;

import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.Util;

public class PublisherResponse {
   final Object[] results;
   // The completed segments after this request - This may be null
   final IntSet completedSegments;
   // The segments that were lost mid processing - This may be null
   final IntSet lostSegments;
   // How many elements are in the results
   // Note that after being deserialized this is always equal to results.length - local this will be how many entries
   // are in the array
   final int size;
   final boolean complete;
   final int segmentOffset;

   public PublisherResponse(Object[] results, IntSet completedSegments, IntSet lostSegments, int size, boolean complete,
         int segmentOffset) {
      this.results = results;
      this.completedSegments = completedSegments;
      this.lostSegments = lostSegments;
      this.size = size;
      this.complete = complete;
      this.segmentOffset = segmentOffset;
   }

   public static PublisherResponse emptyResponse(IntSet completedSegments, IntSet lostSegments) {
      return new PublisherResponse(Util.EMPTY_OBJECT_ARRAY, completedSegments, lostSegments, 0, true, 0);
   }

   public Object[] getResults() {
      return results;
   }

   public IntSet getCompletedSegments() {
      return completedSegments;
   }

   public IntSet getLostSegments() {
      return lostSegments;
   }

   public int getSize() {
      return size;
   }

   public boolean isComplete() {
      return complete;
   }

   public int getSegmentOffset() {
      return segmentOffset;
   }

   public void forEachSegmentValue(ObjIntConsumer consumer, int segment) {
      for (int i = segmentOffset; i < results.length; ++i) {
         consumer.accept(results[i], segment);
      }
   }

   @Override
   public String toString() {
      return "PublisherResponse{" +
            "size=" + size +
            ", completedSegments=" + completedSegments +
            ", lostSegments=" + lostSegments +
            ", complete=" + complete +
            ", segmentOffset=" + segmentOffset +
            '}';
   }
}
