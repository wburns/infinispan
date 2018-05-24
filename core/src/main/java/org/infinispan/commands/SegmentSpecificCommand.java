package org.infinispan.commands;

/**
 * @author wburns
 * @since 9.0
 */
public interface SegmentSpecificCommand {
   /**
    * Returns the segment that this key maps to. This must always return a number 0 or larger.
    * @return the segment of the key
    */
   int getSegment();

   void setSegment(int segment);

   Object getKey();

   int UNKOWN_SEGMENT = -1;

   /**
    * Utility to extract the segment from a given command that may be a {@link SegmentSpecificCommand}. If it is
    * it will return the 0 or larger segment. If not it will return {@link #UNKOWN_SEGMENT} to signify this.
    * @param command the command to extract the segment from
    * @return the segment value from the command, being 0 or greater or -1 if this command doesn't implement
    * {@link SegmentSpecificCommand}
    */
   static int extractSegment(ReplicableCommand command) {
      if (command instanceof SegmentSpecificCommand) {
         return ((SegmentSpecificCommand) command).getSegment();
      }
      return UNKOWN_SEGMENT;
   }
}
