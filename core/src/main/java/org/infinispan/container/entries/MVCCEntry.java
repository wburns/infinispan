package org.infinispan.container.entries;

import org.infinispan.container.DataContainer;
import org.infinispan.metadata.Metadata;

/**
 * An entry that can be safely copied when updates are made, to provide MVCC semantics
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface MVCCEntry<K, V> extends CacheEntry<K, V> {

   void setChanged(boolean isChanged);

   /**
    * Marks this entry as being expired.  This is a special form of removal.
    * @param expired whether or not this entry should be expired
    */
   void setExpired(boolean expired);

   /**
    * Returns whether this entry was marked as being expired or not
    * @return whether expired has been set
    */
   boolean isExpired();

   /**
    * Reset the current value of the entry to the value before the command was executed the first time.
    * This is invoked before the command is retried.
    */
   void resetCurrentValue();

   /**
    * Update the previous value of the entry - set it to current value. This is invoked when the command
    * is successfully finished (there won't be any more retries) or when the value was updated from external
    * source.
    */
   void updatePreviousValue();

   /**
    * @return The previous value.
    */
   V getOldValue();

   /**
    * @return The previous metadata.
    */
   Metadata getOldMetadata();

   /**
    * Mark that this entry was loaded from the cache (as opposed to generated by the application
    * using write-only command), mostly for purposes of the write skew check.
    */
   default void setRead() {}

   /**
    * Check is this entry as loaded from the cache (as opposed to generated by the application
    * using write-only command), mostly for purposes of the write skew check.
    */
   default boolean isRead() {
      return false;
   }

   /**
    * Mark this context-entry as already committed to the {@link DataContainer}.
    */
   default void setCommitted() {}

   /**
    * @return True if this context entry has been committed to the {@link DataContainer}
    */
   default boolean isCommitted() { return false; }

   /**
    * @return True if we've checked persistence for presence of this entry.
    */
   default boolean isLoaded() {
      return false;
   }

   default void setLoaded(boolean loaded) {
   }

   /**
    * @return True if this entry should non be written to shared persistence
    */
   boolean isSkipSharedStore();

   void setSkipSharedStore();

   @Override
   MVCCEntry<K, V> clone();
}
