package org.infinispan.container.entries;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

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
public class L1InternalCacheEntry extends MortalCacheEntry {

   public L1InternalCacheEntry(Object key, Object value, long lifespan, long created) {
      super(key, value, lifespan, created);
   }

   @Override
   public boolean isL1Entry() {
      return true;
   }

   public static class Externalizer extends AbstractExternalizer<L1InternalCacheEntry> {
      @Override
      public void writeObject(ObjectOutput output, L1InternalCacheEntry mce) throws IOException {
         output.writeObject(mce.key);
         output.writeObject(mce.value);
         UnsignedNumeric.writeUnsignedLong(output, mce.created);
         output.writeLong(mce.lifespan); // could be negative so should not use unsigned longs
      }

      @Override
      public L1InternalCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object k = input.readObject();
         Object v = input.readObject();
         long created = UnsignedNumeric.readUnsignedLong(input);
         Long lifespan = input.readLong();
         return new L1InternalCacheEntry(k, v, lifespan, created);
      }

      @Override
      public Integer getId() {
         return Ids.L1_ENTRY;
      }

      @Override
      public Set<Class<? extends L1InternalCacheEntry>> getTypeClasses() {
         return Util.<Class<? extends L1InternalCacheEntry>>asSet(L1InternalCacheEntry.class);
      }
   }
}
