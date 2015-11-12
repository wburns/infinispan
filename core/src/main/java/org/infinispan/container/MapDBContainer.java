package org.infinispan.container;

import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.PeekableMap;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.L1Metadata;
import org.infinispan.util.AbstractDelegatingConcurrentMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentMap;

import static org.infinispan.factories.KnownComponentNames.CACHE_MARSHALLER;

/**
 * Created by wburns on 11/4/15.
 */
public class MapDBContainer<K, V> extends ConcurrentMapBackedDataContainer<K, V> {

   private static final Log log = LogFactory.getLog(MapDBContainer.class);
   private static final boolean trace = log.isTraceEnabled();

   private final Equivalence<? super K> keyEquivalence;

   DB db;
   HTreeMap<K, InternalCacheEntry<K, V>> realMap;
   PeakableHTreeMap<K, InternalCacheEntry<K, V>> peakableHTreeMap;
   private StreamingMarshaller marshaller;

   public MapDBContainer(boolean passivationEnabled, Equivalence<? super K> keyEquivalence) {
      super(passivationEnabled);
      this.keyEquivalence = keyEquivalence;
   }

   protected class DataOutputToOutputStream extends OutputStream {
      protected final DataOutput dataOutput;

      public DataOutputToOutputStream(DataOutput dataOutput) {
         this.dataOutput = dataOutput;
      }

      @Override
      public void write(int b) throws IOException {
         dataOutput.write(b);
      }

      @Override
      public void write(byte[] b) throws IOException {
         dataOutput.write(b);
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
         dataOutput.write(b, off, len);
      }
   }

   protected class DataInputToInputStream extends InputStream {
      protected final DataInput dataInput;

      public DataInputToInputStream(DataInput dataInput) {
         this.dataInput = dataInput;
      }

      @Override
      public int read() throws IOException {
         // The byte will be converted to an int so we have to make sure to only retain the byte information
         return dataInput.readByte() & 0xff;
      }

      @Override
      public int read(byte[] b) throws IOException {
         return super.read(b, 0, 1);
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
         return super.read(b, off, 1);
      }
   }

   protected class JBossSerializer<R> extends Serializer<R> {
      private final StreamingMarshaller marshaller;

      public JBossSerializer(StreamingMarshaller marshaller) {
         this.marshaller = marshaller;
      }

      @Override
      public void serialize(DataOutput out, R value) throws IOException {
         ObjectOutput objectOutput = marshaller.startObjectOutput(out instanceof OutputStream ? (OutputStream) out :
                 new DataOutputToOutputStream(out), false, 1024);
         try {
            objectOutput.writeObject(value);
         } finally {
            marshaller.finishObjectOutput(objectOutput);
         }
      }

      @Override
      public R deserialize(DataInput in, int available) throws IOException {
         ObjectInput objectInput = marshaller.startObjectInput(new DataInputToInputStream(in), false);
         Object obj;
         try {
            obj = objectInput.readObject();
         } catch (ClassNotFoundException e) {
            throw new IOException(e);
         } finally {
            marshaller.finishObjectInput(objectInput);
         }
         return (R) obj;
      }

      @Override
      public boolean isTrusted() {
         return true;
      }
   }

   protected class JBossSerializerWithEquivalence<R> extends JBossSerializer<R> {
      private final Equivalence<? super R> equivalence;

      public JBossSerializerWithEquivalence(StreamingMarshaller marshaller, Equivalence<? super R> equivalence) {
         super(marshaller);
         this.equivalence = equivalence;
      }

      @Override
      public boolean equals(R a1, R a2) {
         return equivalence.equals(a1, a2);
      }

      @Override
      public int hashCode(R r, int seed) {
         return equivalence.hashCode(r);
      }
   }

   // TODO: add in serializer with equivalence with ICE

   protected static class PeakableHTreeMap<K, V> extends AbstractDelegatingConcurrentMap<K, V> implements PeekableMap<K, V> {
      protected final HTreeMap<K, V> map;

      public PeakableHTreeMap(HTreeMap<K, V> map) {
         this.map = map;
      }

      @Override
      protected ConcurrentMap<K, V> delegate() {
         return map;
      }

      @Override
      public V peek(Object key) {
         return map.getPeek(key);
      }
   }

   @Inject
   public void inject(@ComponentName(CACHE_MARSHALLER) StreamingMarshaller marshaller) {
      this.marshaller = marshaller;
   }

   @Start
   public void start() {
      db = DBMaker.memoryDB()
//              .cacheHashTableEnable()
              .transactionDisable()
              // Note we cannot set jvm hook to clear data since then it isn't cleared on a stop
              .make();
      realMap = db.hashMap("cache", new JBossSerializerWithEquivalence<>(marshaller, keyEquivalence),
                      new JBossSerializer<>(marshaller));
      peakableHTreeMap = new PeakableHTreeMap<>(realMap);
   }

   /**
    * This has to be higher than stop priority
    */
   @Stop(priority = 9999)
   public void stop() {
      log.debug("Stopping MapDB container!");
      realMap.close();
      db.getCatalog().clear();
      db.close();
   }

   @Override
   public ConcurrentMap<K, InternalCacheEntry<K, V>> getMap() {
      return peakableHTreeMap;
   }

   // The following is optimizations when passivation is not enabled
   @Override
   public void put(K k, V v, Metadata metadata) {
      boolean l1Entry = false;
      if (metadata instanceof L1Metadata) {
         metadata = ((L1Metadata) metadata).metadata();
         l1Entry = true;
      }

      if (trace) {
         log.tracef("Creating new ICE for writing. Metadata=%s, new value=%s", metadata, v);
      }
      // We don't update the contained object with this map because we would have deserialize the value on a get, much
      // better to just overwrite it
      final InternalCacheEntry<K, V> copy;
      if (l1Entry) {
         copy = entryFactory.createL1(k, v, metadata);
      } else {
         // this is a brand-new entry
         copy = entryFactory.create(k, v, metadata);
      }

      if (trace)
         log.tracef("Store %s in container", copy);
      if (passivationEnabled) {
         realMap.compute(copy.getKey(), (key, entry) -> {
            activator.onUpdate(key, entry == null);
            return copy;
         });
      } else {
         realMap.put(copy.getKey(), copy);
      }
   }

   @Override
   public int size() {
      if (db.isClosed()) {
         return 0;
      }
      return super.size();
   }

   // We have to set this to 10 to stop it before the marshaller is stopped
   @Stop(priority = 10)
   @Override
   public void clear() {
      if (!db.isClosed()) {
         super.clear();
      }
   }
}
