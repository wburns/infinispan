/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.loaders.astyanax.configuration;

import java.util.Collections;
import java.util.List;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.infinispan.configuration.BuiltBy;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.configuration.cache.LegacyLoaderAdapter;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.loaders.astyanax.AstyanaxCacheStoreConfig;
import org.infinispan.util.TypedProperties;

@BuiltBy(AstyanaxCacheStoreConfigurationBuilder.class)
public class AstyanaxCacheStoreConfiguration extends AbstractStoreConfiguration implements
      LegacyLoaderAdapter<AstyanaxCacheStoreConfig> {

   private final boolean autoCreateKeyspace;
   private final String entryColumnFamily;
   private final String expirationColumnFamily;
   private final List<AstyanaxServerConfiguration> servers;
   private final String keySpace;
   private final String password;
   private final ConsistencyLevel readConsistencyLevel;
   private final boolean sharedKeyspace;
   private final String username;
   private final ConsistencyLevel writeConsistencyLevel;

   public AstyanaxCacheStoreConfiguration(boolean autoCreateKeyspace, String entryColumnFamily,
         String expirationColumnFamily, List<AstyanaxServerConfiguration> servers, String keySpace, String password,
         boolean sharedKeyspace, String username, ConsistencyLevel readConsistencyLevel,
         ConsistencyLevel writeConsistencyLevel, boolean purgeOnStartup, boolean purgeSynchronously, int purgerThreads,
         boolean fetchPersistentState, boolean ignoreModifications, TypedProperties properties,
         AsyncStoreConfiguration asyncStoreConfiguration, SingletonStoreConfiguration singletonStoreConfiguration) {
      super(purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications, properties,
            asyncStoreConfiguration, singletonStoreConfiguration);
      this.autoCreateKeyspace = autoCreateKeyspace;
      this.entryColumnFamily = entryColumnFamily;
      this.expirationColumnFamily = expirationColumnFamily;
      this.servers = Collections.unmodifiableList(servers);
      this.keySpace = keySpace;
      this.password = password;
      this.sharedKeyspace = sharedKeyspace;
      this.username = username;
      this.readConsistencyLevel = readConsistencyLevel;
      this.writeConsistencyLevel = writeConsistencyLevel;
   }

   public boolean autoCreateKeyspace() {
      return autoCreateKeyspace;
   }

   public String entryColumnFamily() {
      return entryColumnFamily;
   }

   public String expirationColumnFamily() {
      return expirationColumnFamily;
   }

   public List<AstyanaxServerConfiguration> hosts() {
      return servers;
   }

   public String keySpace() {
      return keySpace;
   }

   public List<AstyanaxServerConfiguration> servers() {
      return servers;
   }


   public boolean sharedKeyspace() {
      return sharedKeyspace;
   }

   public String password() {
      return password;
   }

   public String username() {
      return username;
   }

   public ConsistencyLevel readConsistencyLevel() {
      return readConsistencyLevel;
   }

   public ConsistencyLevel writeConsistencyLevel() {
      return writeConsistencyLevel;
   }

   @Override
   public AstyanaxCacheStoreConfig adapt() {
      AstyanaxCacheStoreConfig config = new AstyanaxCacheStoreConfig();

      LegacyConfigurationAdaptor.adapt(this, config);

      config.setAutoCreateKeyspace(autoCreateKeyspace);
      config.setEntryColumnFamily(entryColumnFamily);
      config.setExpirationColumnFamily(expirationColumnFamily);
      StringBuilder host = new StringBuilder();
      for (AstyanaxServerConfiguration s : this.servers) {
         if (host.length() > 0)
            host.append(";");
         host.append(s.host());
      }
      config.setHost(host.toString());
      config.setKeySpace(keySpace);
      config.setPassword(password);
      config.setReadConsistencyLevel(readConsistencyLevel);
      config.setSharedKeyspace(sharedKeyspace);
      config.setUsername(username);
      config.setWriteConsistencyLevel(writeConsistencyLevel);

      return config;
   }


}
