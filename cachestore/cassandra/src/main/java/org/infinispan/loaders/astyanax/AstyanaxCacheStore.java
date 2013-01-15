/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.loaders.astyanax;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.config.ConfigurationException;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.loaders.AbstractCacheStore;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderMetadata;
import org.infinispan.loaders.astyanax.AstyanaxCacheStoreConfig;
import org.infinispan.loaders.cassandra.logging.Log;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.loaders.modifications.Remove;
import org.infinispan.loaders.modifications.Store;
import org.infinispan.marshall.Marshaller;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.util.logging.LogFactory;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.connectionpool.exceptions.SerializationException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.IndexQuery;
import com.netflix.astyanax.query.PreparedIndexExpression;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.RangeBuilder;

/**
 * A persistent <code>CacheLoader</code> based on Apache Cassandra project. See
 * http://cassandra.apache.org/
 *
 * @author wburns
 */
@CacheLoaderMetadata(configurationClass = AstyanaxCacheStoreConfig.class)
public class AstyanaxCacheStore extends AbstractCacheStore {

   private static final String ENTRY_KEY_PREFIX = "entry";
   private static final String ENTRY_COLUMN_NAME = "entry";
   private static final String CACHENAME_COLUMN_NAME = "cacheName";
   private static final String EXPIRATION_KEY = "expiration";
   private static final int SLICE_SIZE = 100;
   private static final Log log = LogFactory.getLog(AstyanaxCacheStore.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();
   private static Charset UTF8 = Charset.forName("UTF-8");

   private AstyanaxCacheStoreConfig config;

   private ConsistencyLevel readConsistencyLevel;
   private ConsistencyLevel writeConsistencyLevel;
   
   private Cluster _cluster;
   private Keyspace _keyspace;

   private String cacheName;
   private ColumnFamily<PrefixAndKey, String> _entryColumnFamily;
   private ColumnFamily<byte[], ExpirationAndKey> _expirationColumnFamily;
   private byte[] entryKeyPrefix;
   private byte[] expirationKey;
   
   private MarshallerSerializer _marshallerSerializer;
   private Collection<PreparedIndexExpression<PrefixAndKey, String>> _preparedIndices;

   private static byte END_OF_COMPONENT = 0;
   
   private static class ExpirationAndKeySerializer extends AbstractSerializer<ExpirationAndKey> {

      private final Marshaller _marshaller;

      public ExpirationAndKeySerializer(Marshaller marshaller) {
         _marshaller = marshaller;
      }

      @Override
      public ByteBuffer toByteBuffer(ExpirationAndKey obj) {
         ByteBuffer objBuffer;
         try {
            objBuffer = _marshaller.objectToBuffer(obj._key).toJDKByteBuffer();
            int remaining = objBuffer.remaining();
            // 6 is for the 2 shorts and 2 bytes
            // 8 is for the long and the rest is the byte bufer
            ByteBuffer buffer = ByteBuffer.allocate(6 + 8 + remaining);
            // Needs to be <size><data>EOC for each component
            buffer.putShort((short) 8);
            buffer.putLong(obj._expiry);
            buffer.put(END_OF_COMPONENT);
            // 
            if (remaining > Short.MAX_VALUE) {
               throw new IllegalArgumentException("Key serialized form cannot be larger than " + Short.MAX_VALUE
                     + " bytes");
            }
            buffer.putShort((short) remaining);
            buffer.put(objBuffer);
            buffer.put(END_OF_COMPONENT);

            buffer.flip();

            return buffer;
         } catch (Exception e) {
            throw new SerializationException(e);
         }
      }

      @Override
      public ExpirationAndKey fromByteBuffer(ByteBuffer byteBuffer) {
         try {
            byteBuffer.getShort();
            long expiry = byteBuffer.getLong();
            byteBuffer.get();
            short size = byteBuffer.getShort();
            Object key = _marshaller.objectFromByteBuffer(byteBuffer.array(),
                  byteBuffer.arrayOffset() + byteBuffer.position(), size);
            return new ExpirationAndKey(expiry, key);
         } catch (Exception e) {
            throw new SerializationException(e);
         }
      }
   }

   private static class PrefixAndKeySerializer extends AbstractSerializer<PrefixAndKey> {

      private final Marshaller _marshaller;

      public PrefixAndKeySerializer(Marshaller marshaller) {
         _marshaller = marshaller;
      }

      @Override
      public ByteBuffer toByteBuffer(PrefixAndKey obj) {
         try {
            ByteBuffer keyBuffer = _marshaller.objectToBuffer(obj._key).toJDKByteBuffer();
            int keyRemaining = keyBuffer.remaining();
            if (keyRemaining > Short.MAX_VALUE) {
               throw new IllegalArgumentException("Serialized key cannot be " + "larger than " + Short.MAX_VALUE
                     + " bytes, was " + keyRemaining);
            }
            byte[] prefix = obj._prefix;
            // 6 is for the 2 shorts and 2 bytes
            // rest is for 2 buffers
            ByteBuffer buffer = ByteBuffer.allocate(6 + prefix.length + keyRemaining);
            // Needs to be <size><data>EOC for each component
            buffer.putShort((short) prefix.length);
            buffer.put(prefix);
            buffer.put(END_OF_COMPONENT);
            // 
            buffer.putShort((short) keyRemaining);
            buffer.put(keyBuffer);
            buffer.put(END_OF_COMPONENT);

            buffer.flip();

            return buffer;
         } catch (Exception e) {
            throw new SerializationException(e);
         }
      }

      @Override
      public PrefixAndKey fromByteBuffer(ByteBuffer byteBuffer) {
         try {
            short size = byteBuffer.getShort();
            byte[] prefix = new byte[size];
            byteBuffer.get(prefix);
            byteBuffer.get();

            size = byteBuffer.getShort();
            Object key;
            key = _marshaller.objectFromByteBuffer(byteBuffer.array(), 
                  byteBuffer.arrayOffset() + byteBuffer.position(), size);

            return new PrefixAndKey(prefix, key);
         } catch (Exception e) {
            throw new SerializationException(e);
         }
      }
   }

   private static class ExpirationAndKey {
      public ExpirationAndKey(long expiry, Object key) {
         _expiry = expiry;
         _key = key;
      }

      private final long _expiry;
      private final Object _key;
   }

   private static class PrefixAndKey {
      public PrefixAndKey(byte[] prefix, Object key) {
         _prefix = prefix;
         _key = key;
      }

      private final byte[] _prefix;
      private final Object _key;
   }

   public static class MarshallerSerializer extends AbstractSerializer<InternalCacheValue> {
      public MarshallerSerializer(Marshaller marshaller) {
         _marshaller = marshaller;
      }

      @Override
      public ByteBuffer toByteBuffer(InternalCacheValue obj) {
         try {
            return _marshaller.objectToBuffer(obj).toJDKByteBuffer();
         } catch (Exception e) {
            throw new SerializationException(e);
         }
      }

      @Override
      public InternalCacheValue fromByteBuffer(ByteBuffer byteBuffer) {
         try {
            return (InternalCacheValue) _marshaller.objectFromByteBuffer(
                  byteBuffer.array(), byteBuffer.arrayOffset()
                  + byteBuffer.position(), byteBuffer.remaining());
         } catch (Exception e) {
            throw new SerializationException(e);
         }
      }

      private final Marshaller _marshaller;
   }

   @Override
   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return AstyanaxCacheStoreConfig.class;
   }

   @Override
   public void init(CacheLoaderConfig clc, Cache<?, ?> cache, StreamingMarshaller m)
            throws CacheLoaderException {
      super.init(clc, cache, m);
      this.cacheName = cache.getName();
      this.config = (AstyanaxCacheStoreConfig) clc;
   }

   @SuppressWarnings("unchecked")
   @Override
   public void start() throws CacheLoaderException {

      try {
         readConsistencyLevel = ConsistencyLevel.valueOf("CL_" + config.readConsistencyLevel);
         writeConsistencyLevel = ConsistencyLevel.valueOf("CL_" + config.writeConsistencyLevel);
         
         AstyanaxContext<Cluster> context = new AstyanaxContext.Builder()
         .forKeyspace(config.getKeySpace())
         .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
             .setDiscoveryType(NodeDiscoveryType.TOKEN_AWARE)
             .setConnectionPoolType(ConnectionPoolType.TOKEN_AWARE)
             .setDefaultReadConsistencyLevel(readConsistencyLevel)
             .setDefaultWriteConsistencyLevel(writeConsistencyLevel)
         )
         .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
             .setPort(config.getPort())
             .setSeeds(config.getHost())
         )
         .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
         .buildCluster(ThriftFamilyFactory.getInstance());
         
         context.start();
         _cluster = context.getEntity();
         
         _entryColumnFamily = ColumnFamily.newColumnFamily(
             config.entryColumnFamily, new PrefixAndKeySerializer(getMarshaller()), 
             StringSerializer.get());
         
         _expirationColumnFamily = ColumnFamily.newColumnFamily(
            config.expirationColumnFamily, BytesArraySerializer.get(), 
            new ExpirationAndKeySerializer(getMarshaller()));
         
         entryKeyPrefix = (ENTRY_KEY_PREFIX + (config.isSharedKeyspace() ? "_" + cacheName : "")).getBytes(UTF8);
         if (entryKeyPrefix.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Serialized prefix cannot be " + 
                  "larger than " + Short.MAX_VALUE + " bytes, was " + entryKeyPrefix.length);
         }
         expirationKey = (EXPIRATION_KEY + (config.isSharedKeyspace() ? "_" + cacheName : "")).getBytes();
         if (expirationKey.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Serialized expiration key cannot be " + 
                  "larger than " + Short.MAX_VALUE + " bytes, was " + expirationKey.length);
         }
         
         _preparedIndices = Arrays.asList(_entryColumnFamily
                .newIndexClause().whereColumn(CACHENAME_COLUMN_NAME).equals().value(cacheName));
         
         _marshallerSerializer = new MarshallerSerializer(getMarshaller());
      } catch (Exception e) {
         throw new ConfigurationException(e);
      }

      _keyspace = _cluster.getKeyspace(config.getKeySpace());
      
      if (config.autoCreateKeyspace) {
         log.debug("automatically create keyspace");
         createKeySpace();
      }
      
      log.debug("cleaning up expired entries...");
      purgeInternal();

      log.debug("started");
      super.start();
   }

   private void createKeySpace() throws CacheLoaderException {
      try {
         // Make sure to remove the previous keyspace if there was one
         KeyspaceDefinition def = _cluster.describeKeyspace(_keyspace.getKeyspaceName());
         if (def == null) {
            _cluster.addKeyspace(_cluster.makeKeyspaceDefinition()
                  .setName(_keyspace.getKeyspaceName())
                  .setStrategyClass("org.apache.cassandra.locator.SimpleStrategy")
                  .addStrategyOption("replication_factor", Integer.toString(config.getReplicationFactor()))
                  .addColumnFamily(_cluster.makeColumnFamilyDefinition()
                        .setName(_entryColumnFamily.getName())
                        .setKeyValidationClass("BytesType")
                        .setComparatorType("UTF8Type")
                        .addColumnDefinition(_cluster.makeColumnDefinition()
                              .setName(ENTRY_COLUMN_NAME)
                              .setValidationClass("BytesType")
                        )
                        // Make a secondary index on cacheName
                        .addColumnDefinition(_cluster.makeColumnDefinition()
                              .setName(CACHENAME_COLUMN_NAME)
                              .setValidationClass("UTF8Type")
                              .setKeysIndex("cacheName")
                        )
                  )
                  .addColumnFamily(_cluster.makeColumnFamilyDefinition()
                        .setName(_expirationColumnFamily.getName())
                        .setKeyValidationClass("UTF8Type")
                        .setComparatorType("CompositeType(LongType,BytesType)")
                  )
            );
         }
      } catch (ConnectionException e) {
         throw new CacheLoaderException("Could not create keyspace/column families", e);
      }
   }

    @Override
    public InternalCacheEntry load(Object key) throws CacheLoaderException {
        Column<String> column;
        try {
            column = _keyspace.prepareQuery(_entryColumnFamily)
                    .getKey(new PrefixAndKey(entryKeyPrefix, key))
                    .getColumn(ENTRY_COLUMN_NAME)
                    .execute()
                    .getResult();
        }
        catch (NotFoundException nfe) {
           log.debugf("Key '%s' not found", key);
           return null;
        }
        catch (Exception e) {
            throw new CacheLoaderException(e);
        }
        // If we have a column should be good, not tombstoned
        if (column == null) {
            log.debugf("Key '%s' not found", key);
            return null;
        }
        InternalCacheValue value = column.getValue(_marshallerSerializer);
        InternalCacheEntry ice = null;
        if (value != null) {
            if (value.isExpired(System.currentTimeMillis())) {
               MutationBatch mb = _keyspace.prepareMutationBatch()
                     .setConsistencyLevel(writeConsistencyLevel);
                remove0(key, mb);
                try {
                   mb.execute();
                }
                catch (NotFoundException nfe) {
                   // TODO: This could happen if it was removed by someone else, ignore usually
                   throw new CacheLoaderException(nfe);
                }
                catch (Exception e) {
                    throw new CacheLoaderException(e);
                }
                ice = null;
            }
            else {
               ice = value.toInternalCacheEntry(key);
            }
        }
        
        return ice;
    }

   @Override
   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      return load(Integer.MAX_VALUE);
   }

   @Override
   public Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException {
      Set<InternalCacheEntry> s = new HashSet<InternalCacheEntry>();
      try {
         IndexQuery<PrefixAndKey, String> query = 
               _keyspace.prepareQuery(_entryColumnFamily)
                  .searchWithIndex()
                  .setRowLimit(Math.min(SLICE_SIZE, numEntries))
                  .autoPaginateRows(true)
                  .addPreparedExpressions(_preparedIndices)
                  .withColumnSlice(ENTRY_COLUMN_NAME);
         
         boolean reachedLimit = false;
         Rows<PrefixAndKey, String> rows;
         while (!reachedLimit && !(rows = query.execute().getResult()).isEmpty()) {
            for (Row<PrefixAndKey, String> row : rows) {
               Object key = row.getKey()._key;
               if (key == null) {
                  continue;
               }
               // If we have a row columns should be good, not tombstoned
               if (!row.getColumns().isEmpty()) {
                  if (log.isDebugEnabled()) {
                     log.debugf("Loading %s", key);
                  }
                  ByteBuffer bytes = row.getColumns().getByteBufferValue(ENTRY_COLUMN_NAME, null);
                  if (bytes != null) {
                     InternalCacheValue ice = _marshallerSerializer.fromByteBuffer(bytes);
                     s.add(ice.toInternalCacheEntry(key));
                     if (s.size() >= numEntries) {
                        reachedLimit = true;
                        break;
                     }
                  }
                  else if (log.isDebugEnabled()) {
                     log.debugf("Entry %s not found, skipping", key);
                  }
               }
               else if (log.isDebugEnabled()){
                  log.debugf("Skipping empty key %s", key);
               }
            }
         }
         return s;
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }
   }

   @Override
   public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
      Set<Object> keys = new HashSet<Object>();
      try {
         IndexQuery<PrefixAndKey, String> query = 
               _keyspace.prepareQuery(_entryColumnFamily)
                  .searchWithIndex()
                  .setRowLimit(SLICE_SIZE)
                  .autoPaginateRows(true)
                  .addPreparedExpressions(_preparedIndices)
                  .withColumnSlice(ENTRY_COLUMN_NAME);
         
         Rows<PrefixAndKey, String> rows;
         while (!(rows = query.execute().getResult()).isEmpty()) {
            for (Row<PrefixAndKey, String> row : rows) {
               // If we have a row columns should be good, not tombstoned
               if (!row.getColumns().isEmpty()) {
                  Object key = row.getKey()._key;
                  if (key != null && 
                        (keysToExclude == null || !keysToExclude.contains(key))) {
                     keys.add(key);
                  }
               }
            }
         }
         return keys;
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }
   }

   /**
    * Closes all databases, ignoring exceptions, and nulls references to all database related
    * information.
    */
   @Override
   public void stop() throws CacheLoaderException {
      super.stop();
   }

   @Override
   public void clear() throws CacheLoaderException {
      try {
         // TODO: do we care about the expiry entry?  My thought it is no, it 
			// handles misses itself
         if (config.isSharedKeyspace()) {
	         IndexQuery<PrefixAndKey, String> query = 
	               _keyspace.prepareQuery(_entryColumnFamily)
	                  .searchWithIndex()
	                  .setRowLimit(SLICE_SIZE)
	                  .autoPaginateRows(true)
	                  .addPreparedExpressions(_preparedIndices)
	                  .withColumnSlice(ENTRY_COLUMN_NAME);
	         // Get the keys in SLICE_SIZE blocks
	         Rows<PrefixAndKey, String> rows;
	         // TODO the last key is a bit hacky, if we could fix the astyanax pagination to work with people deleting contents behind it we can change this to what it should be
	         PrefixAndKey lastKey = null;
	         while (!(rows = query.execute().getResult()).isEmpty()) {
	            MutationBatch mb = _keyspace.prepareMutationBatch()
	                  .setConsistencyLevel(writeConsistencyLevel);
	            if (lastKey != null) {
	            	mb.withRow(_entryColumnFamily, lastKey).delete();
	            }
	            // We can't delete the last row as we need it to query the next
	            Iterator<Row<PrefixAndKey, String>> iter = rows.iterator();
	            while (iter.hasNext()) {
	            	Row<PrefixAndKey, String> row = iter.next();
	            	if (iter.hasNext()) {
		            	// If we have row columns then it is not tombstoned, so delete it
		                if (!row.getColumns().isEmpty()) {
		                   mb.withRow(_entryColumnFamily, row.getKey()).delete();
		            	}
	            	}
	            	else {
	            		lastKey = row.getKey();
	            	}
	            }
	            mb.execute();
	         }
	         if (lastKey != null) {
	            MutationBatch mb = _keyspace.prepareMutationBatch()
	                     .setConsistencyLevel(writeConsistencyLevel);
				mb.withRow(_entryColumnFamily, lastKey).delete();
				mb.execute();
	         }
    	  }
    	  else {
    		  // If we have a shared keyspace, just blow away the entire column family - the expired ones will work themselves out
    		  _keyspace.truncateColumnFamily(_entryColumnFamily);
    	  }
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }
   }

   @Override
   public boolean remove(Object key) throws CacheLoaderException {
      try {
         if (trace)
            log.tracef("remove(\"%s\") ", key);
         return innerRemove(key);
      } catch (Exception e) {
          log.errorRemovingKey(key, e);
          return false;
      }
   }
   
   private boolean innerRemove(Object key) throws CacheLoaderException {
      Column<String> column;
      try {
          column = _keyspace.prepareQuery(_entryColumnFamily)
                  .getKey(new PrefixAndKey(entryKeyPrefix, key))
                  .getColumn(ENTRY_COLUMN_NAME)
                  .execute()
                  .getResult();
          
         boolean removed;
         if (column != null) {
            MutationBatch mb = _keyspace.prepareMutationBatch()
                  .setConsistencyLevel(writeConsistencyLevel);
            remove0(key, mb);
            mb.execute();
            removed = true;
         } else {
            removed = false;
         }
         return removed;
      }
      catch (NotFoundException e) {
         log.debugf("Key '%s' not removed due to not being not found", key);
         return false;
      }
      catch (ConnectionException e) {
          throw new CacheLoaderException(e);
      }
   }
   
   private void remove0(Object key, MutationBatch mutationBatch) {
      mutationBatch.withRow(_entryColumnFamily, new PrefixAndKey(entryKeyPrefix, key)).delete();
   }

   @Override
   public void store(InternalCacheEntry entry) throws CacheLoaderException {
      try {
         MutationBatch mb = _keyspace.prepareMutationBatch()
            .setConsistencyLevel(writeConsistencyLevel);
         store0(entry, mb);
         mb.execute();
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }
   }

   private void store0(InternalCacheEntry entry,
            MutationBatch mutationBatch) throws CacheLoaderException {
      Object key = entry.getKey();
      if (trace)
         log.tracef("store(\"%s\") ", key);
      mutationBatch.withRow(_entryColumnFamily, new PrefixAndKey(entryKeyPrefix, key))
         .putColumn(ENTRY_COLUMN_NAME, entry.toInternalCacheValue(), 
            _marshallerSerializer, null)
         .putColumn(CACHENAME_COLUMN_NAME, cacheName, null);
      if (entry.canExpire()) {
         mutationBatch.withRow(_expirationColumnFamily, expirationKey)
            .putEmptyColumn(new ExpirationAndKey(entry.getExpiryTime(), key), 
                  null);
      }
   }

   /**
    * Writes to a stream the entries terminated by a null value.
    */
   @Override
   public void toStream(ObjectOutput out) throws CacheLoaderException {
      try {
         Set<InternalCacheEntry> loadAll = loadAll();
         for (InternalCacheEntry entry : loadAll) {
            getMarshaller().objectToObjectStream(entry, out);
         }
         getMarshaller().objectToObjectStream(null, out);
      } catch (IOException e) {
         throw new CacheLoaderException(e);
      }
   }

   /**
    * Reads from a stream the entries terminated by a null.
    */
   @Override
   public void fromStream(ObjectInput in) throws CacheLoaderException {
      try {
         while (true) {
            InternalCacheEntry entry = (InternalCacheEntry) getMarshaller().objectFromObjectStream(
                     in);
            if (entry == null)
               break;
            store(entry);
         }
      } catch (IOException e) {
         throw new CacheLoaderException(e);
      } catch (ClassNotFoundException e) {
         throw new CacheLoaderException(e);
      } catch (InterruptedException ie) {
         if (log.isTraceEnabled())
            log.trace("Interrupted while reading from stream");
         Thread.currentThread().interrupt();
      }
   }

   /**
    * Purge expired entries. Expiration entries are stored in a single key (expirationKey) within a
    * specific ColumnFamily (set by configuration). The entries are grouped by expiration timestamp
    * in SuperColumns within which each entry's key is mapped to a column
    */
   @Override
   protected void purgeInternal() throws CacheLoaderException {
      if (trace)
         log.trace("purgeInternal");
      try {
         // TODO: we could use a column range max, but don't know easiest way without manually serializing the value
         // instead we just keep going until we find they haven't hit the current time which should work alright, 
         // at worst case we select 100 rows for no reason
         RowQuery<byte[], ExpirationAndKey> query = _keyspace
            .prepareQuery(_expirationColumnFamily)
            .setConsistencyLevel(readConsistencyLevel)
            .getKey(expirationKey)
            .autoPaginate(true)
            .withColumnRange(new RangeBuilder()
                    .setLimit(100)
                    .build()
            );
         
         boolean processMore = true;
         ColumnList<ExpirationAndKey> columns;
         while (processMore && !(columns = query.execute().getResult()).isEmpty()) {
            MutationBatch mb = _keyspace.prepareMutationBatch()
                  .setConsistencyLevel(writeConsistencyLevel);
            for (Column<ExpirationAndKey> column : columns) {
               ExpirationAndKey name = column.getName();
               if (name._expiry > System.currentTimeMillis()) {
                  processMore = false;
                  break;
               }
               mb.withRow(_entryColumnFamily, new PrefixAndKey(entryKeyPrefix, name._key)).delete();
               mb.withRow(_expirationColumnFamily, expirationKey).deleteColumn(name);
            }
            mb.execute();
         }
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }
   }

   @Override
   protected void applyModifications(List<? extends Modification> mods) throws CacheLoaderException {
      try {
         MutationBatch mb = _keyspace.prepareMutationBatch()
                 .setConsistencyLevel(writeConsistencyLevel);
         for (Modification m : mods) {
            switch (m.getType()) {
               case STORE:
                  store0(((Store) m).getStoredEntry(), mb);
                  break;
               case CLEAR:
                  clear();
                  // TODO: I don't know if this is right, but it would seem
                  // if a clear came down then all the previous mutations would
                  // be mute, so ignoring rest by starting a new batch
                  mb = _keyspace.prepareMutationBatch()
                        .setConsistencyLevel(writeConsistencyLevel);
                  break;
               case REMOVE:
                  Object key = ((Remove)m).getKey();
                  if (trace)
                     log.tracef("remove(\"%s\") ", key);
                  remove0(key, mb);
                  break;
               case PURGE_EXPIRED:
                  // TODO: this seems right...?
                  purgeInternal();
                  break;
               default:
                  throw new IllegalArgumentException("Unknown modification type " + m.getType());
            }
         }

         mb.execute();

      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }
   }

   @Override
   public String toString() {
      return "AstyanaxCacheStore";
   }
}
