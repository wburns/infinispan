package org.infinispan.reactive.publisher.impl.commands.batch;

import java.util.function.ObjIntConsumer;

import org.infinispan.commons.util.IntSet;

/**
 * A Publisher Response that is used when key tracking is enabled. This is used in cases when EXACTLY_ONCE delivery
 * guarantee is needed and a map (that isn't encoder based) or flat map operation is required.
 */
public class KeyPublisherResponse extends PublisherResponse {
   final Object[] extraObjects;
   final Object[] keys;
   final int keySize;

   public KeyPublisherResponse(Object[] results, IntSet completedSegments, IntSet lostSegments, int size,
         boolean complete, Object[] extraObjects, int extraSize, Object[] keys, int keySize) {
      super(results, completedSegments, lostSegments, size, complete, extraSize);
      this.extraObjects = extraObjects;
      this.keys = keys;
      this.keySize = keySize;
   }

   // Do not remove this unused method - it is present more for documentation purposes
   // NOTE: extraSize is stored in the segmentOffset field since it isn't valid when using key tracking.
   // Normally segmentOffset is used to determine which key/entry(s) mapped to the current processing segment,
   // since we have the keys directly we don't need this field
   @SuppressWarnings("unused")
   private int getExtraSize() {
      return segmentOffset;
   }

   @Override
   public void forEachSegmentValue(ObjIntConsumer consumer, int segment) {
      for (int i = 0; i < keySize; ++i) {
         consumer.accept(keys[i], segment);
      }
   }
}
