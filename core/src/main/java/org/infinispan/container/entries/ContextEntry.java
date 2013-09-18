package org.infinispan.container.entries;

import org.infinispan.container.DataContainer;

/**
 * An entry that is stored in a caller's invocation context
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface ContextEntry extends CacheEntry, StateChangingEntry {

   /**
    * Makes internal copies of the entry for updates
    *
    * @param container      data container
    */
   void copyForUpdate(DataContainer container);

   void setChanged(boolean isChanged);
}
