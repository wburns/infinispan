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

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.infinispan.loaders.AbstractCacheStoreConfig;
import org.infinispan.loaders.cassandra.CassandraCacheStore;

/**
 * Configures {@link CassandraCacheStore}.
 */
public class AstyanaxCacheStoreConfig extends AbstractCacheStoreConfig {
   /** The serialVersionUID */
   private static final long serialVersionUID = -1731271362015494304L;

   /**
    * @configRef desc="The Cassandra keyspace"
    */
   String keySpace = "Infinispan";

   /**
    * @configRef desc="The Cassandra column family for entries"
    */
   String entryColumnFamily = "InfinispanEntries";

   /**
    * @configRef desc="The Cassandra column family for expirations"
    */
   String expirationColumnFamily = "InfinispanExpiration";

   /**
    * @configRef desc="Whether the keySpace is shared between multiple caches"
    */
   boolean sharedKeyspace = true;

   /**
    * @configRef desc="Which Cassandra consistency level to use when reading"
    */
   String readConsistencyLevel = "ONE";

   /**
    * @configRef desc="Which Cassandra consistency level to use when writing"
    */
   String writeConsistencyLevel = "ONE";

   // TODO: add support for tweaking connection pool?
   
   /**
    * @configRef desc=
    *            "Whether to automatically create the keyspace with the appropriate column families (true by default)"
    */
   boolean autoCreateKeyspace = true;

   String host = "localhost";
   
   int port = 9160;
   
   String password = null;
   
   String username = null;
   
   int replicationFactor = 3;

   public AstyanaxCacheStoreConfig() {
      setCacheLoaderClassName(AstyanaxCacheStore.class.getName());
   }

   public String getKeySpace() {
      return keySpace;
   }

   public void setKeySpace(String keySpace) {
      this.keySpace = keySpace;
   }

   public String getEntryColumnFamily() {
      return entryColumnFamily;
   }

   public void setEntryColumnFamily(String entryColumnFamily) {
      this.entryColumnFamily = entryColumnFamily;
   }

   public String getExpirationColumnFamily() {
      return expirationColumnFamily;
   }

   public void setExpirationColumnFamily(String expirationColumnFamily) {
      this.expirationColumnFamily = expirationColumnFamily;
   }

   public boolean isSharedKeyspace() {
      return sharedKeyspace;
   }

   public void setSharedKeyspace(boolean sharedKeyspace) {
      this.sharedKeyspace = sharedKeyspace;
   }

   public String getReadConsistencyLevel() {
      return readConsistencyLevel;
   }

   public void setReadConsistencyLevel(String readConsistencyLevel) {
      this.readConsistencyLevel = readConsistencyLevel;
   }

   public String getWriteConsistencyLevel() {
      return writeConsistencyLevel;
   }

   public void setWriteConsistencyLevel(String writeConsistencyLevel) {
      this.writeConsistencyLevel = writeConsistencyLevel;
   }

   public void setHost(String host) {
      this.host = host;
   }

   public String getHost() {
      return host;
   }

   public void setPort(int port) {
      this.port = port;
   }

   public int getPort() {
      return port;
   }

   public String getPassword() {
      return password;
   }

   public String getUsername() {
      return username;
   }

   public void setPassword(String password) {
      this.password = password;
   }

   public void setUsername(String username) {
      this.username = username;
   }

   public boolean isAutoCreateKeyspace() {
      return autoCreateKeyspace;
   }

   public void setAutoCreateKeyspace(boolean autoCreateKeyspace) {
      this.autoCreateKeyspace = autoCreateKeyspace;
   }

   public void setReadConsistencyLevel(ConsistencyLevel readConsistencyLevel) {
      this.readConsistencyLevel = readConsistencyLevel.toString();
   }

   public void setWriteConsistencyLevel(ConsistencyLevel writeConsistencyLevel) {
      this.writeConsistencyLevel = writeConsistencyLevel.toString();
   }

   public int getReplicationFactor() {
      return replicationFactor;
   }

   public void setReplicationFactor(int replicationFactor) {
      this.replicationFactor = replicationFactor;
   }
}
