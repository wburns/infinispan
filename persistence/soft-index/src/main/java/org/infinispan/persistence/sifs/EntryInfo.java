package org.infinispan.persistence.sifs;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class EntryInfo extends EntryPosition {
   public final short numRecords;
   public final int segment;

   public EntryInfo(int file, int offset, short numRecords, int segment) {
      super(file, offset);
      this.numRecords = numRecords;
      this.segment = segment;
   }
}
