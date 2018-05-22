package org.infinispan.commands;

/**
 * @author wburns
 * @since 9.0
 */
public interface SegmentSpecificCommand {
   int getSegment();

   void setSegment(int segment);

   Object getKey();
}
