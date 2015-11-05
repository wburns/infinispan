package org.infinispan.container.entries.metadata;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * A {@link org.infinispan.container.entries.InternalCacheEntry} implementation to store a L1 entry.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public class L1MetadataInternalCacheEntry extends MetadataMortalCacheEntry {

   public L1MetadataInternalCacheEntry(Object key, Object value, Metadata metadata, long created) {
      super(key, value, metadata, created);
   }

   @Override
   public boolean isL1Entry() {
      return true;
   }

   public static class Externalizer extends AbstractExternalizer<L1MetadataInternalCacheEntry> {
      @Override
      public void writeObject(ObjectOutput output, L1MetadataInternalCacheEntry mce) throws IOException {
         output.writeObject(mce.key);
         output.writeObject(mce.value);
         UnsignedNumeric.writeUnsignedLong(output, mce.created);
         output.writeObject(mce.metadata);
      }

      @Override
      public L1MetadataInternalCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object k = input.readObject();
         Object v = input.readObject();
         long created = UnsignedNumeric.readUnsignedLong(input);
         Metadata metadata = (Metadata) input.readObject();
         return new L1MetadataInternalCacheEntry(k, v, metadata, created);
      }

      @Override
      public Integer getId() {
         return Ids.L1_ENTRY_METADATA;
      }

      @Override
      public Set<Class<? extends L1MetadataInternalCacheEntry>> getTypeClasses() {
         return Util.<Class<? extends L1MetadataInternalCacheEntry>>asSet(L1MetadataInternalCacheEntry.class);
      }
   }
}
