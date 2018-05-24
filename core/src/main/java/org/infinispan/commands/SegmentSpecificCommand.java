package org.infinispan.commands;

/**
 * @author wburns
 * @since 9.0
 */
public interface SegmentSpecificCommand {
   int getSegment();

   void setSegment(int segment);

   Object getKey();

   static final int UNKOWN_SEGMENT = -1;

   /**
    * Utilit
    * @param command
    * @return
    */
   static int extractSegment(ReplicableCommand command) {
      if (command instanceof SegmentSpecificCommand) {
         return ((SegmentSpecificCommand) command).getSegment();
      }
      return UNKOWN_SEGMENT;
   }
}
