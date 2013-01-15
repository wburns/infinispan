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

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.cassandra.CassandraCacheStore;
import org.infinispan.util.TypedProperties;

/**
 * CassandraCacheStoreConfigurationBuilder. Configures a {@link CassandraCacheStore}
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class AstyanaxCacheStoreConfigurationBuilder extends
      AbstractStoreConfigurationBuilder<AstyanaxCacheStoreConfiguration, AstyanaxCacheStoreConfigurationBuilder>
      implements AstyanaxCacheStoreConfigurationChildBuilder<AstyanaxCacheStoreConfigurationBuilder> {

   private boolean autoCreateKeyspace = true;
   private String entryColumnFamily = "InfinispanEntries";
   private String expirationColumnFamily = "InfinispanExpiration";
   private List<AstyanaxServerConfigurationBuilder> servers = new ArrayList<AstyanaxServerConfigurationBuilder>();
   private String keySpace = "Infinispan";
   private String password;
   private ConsistencyLevel readConsistencyLevel = ConsistencyLevel.ONE;
   private boolean sharedKeyspace = false;
   private String username;
   private ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.ONE;

   public AstyanaxCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public AstyanaxCacheStoreConfigurationBuilder self() {
      return this;
   }

   @Override
   public AstyanaxServerConfigurationBuilder addServer() {
      AstyanaxServerConfigurationBuilder server = new AstyanaxServerConfigurationBuilder(this);
      servers.add(server);
      return server;
   }

   @Override
   public AstyanaxCacheStoreConfigurationBuilder autoCreateKeyspace(boolean autoCreateKeyspace) {
      this.autoCreateKeyspace = autoCreateKeyspace;
      return this;
   }

   @Override
   public AstyanaxCacheStoreConfigurationBuilder entryColumnFamily(String entryColumnFamily) {
      this.entryColumnFamily = entryColumnFamily;
      return this;
   }

   @Override
   public AstyanaxCacheStoreConfigurationBuilder expirationColumnFamily(String expirationColumnFamily) {
      this.expirationColumnFamily = expirationColumnFamily;
      return this;
   }

   @Override
   public AstyanaxCacheStoreConfigurationBuilder keySpace(String keySpace) {
      this.keySpace = keySpace;
      return this;
   }

   @Override
   public AstyanaxCacheStoreConfigurationBuilder password(String password) {
      this.password = password;
      return this;
   }

   @Override
   public AstyanaxCacheStoreConfigurationBuilder readConsistencyLevel(ConsistencyLevel readConsistencyLevel) {
      this.readConsistencyLevel = readConsistencyLevel;
      return this;
   }

   @Override
   public AstyanaxCacheStoreConfigurationBuilder username(String username) {
      this.username = username;
      return this;
   }

   @Override
   public AstyanaxCacheStoreConfigurationBuilder writeConsistencyLevel(ConsistencyLevel writeConsistencyLevel) {
      this.writeConsistencyLevel = writeConsistencyLevel;
      return this;
   }



   @Override
   public void validate() {
      super.validate();
      if (servers.isEmpty()) {
         throw new ConfigurationException("No servers specified");
      }
   }

   @Override
   public AstyanaxCacheStoreConfiguration create() {
      List<AstyanaxServerConfiguration> remoteServers = new ArrayList<AstyanaxServerConfiguration>();
      for (AstyanaxServerConfigurationBuilder server : servers) {
         remoteServers.add(server.create());
      }
      return new AstyanaxCacheStoreConfiguration(autoCreateKeyspace, entryColumnFamily, expirationColumnFamily, 
            remoteServers, keySpace, password, sharedKeyspace, username, readConsistencyLevel, writeConsistencyLevel, 
            purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications, 
            TypedProperties.toTypedProperties(properties), async.create(), singletonStore.create());
   }

   @Override
   public AstyanaxCacheStoreConfigurationBuilder read(AstyanaxCacheStoreConfiguration template) {
      autoCreateKeyspace = template.autoCreateKeyspace();
      entryColumnFamily = template.entryColumnFamily();
      expirationColumnFamily = template.expirationColumnFamily();
      for(AstyanaxServerConfiguration server : template.servers()) {
         this.addServer().host(server.host()).port(server.port());
      }
      keySpace = template.keySpace();
      password = template.password();
      readConsistencyLevel = template.readConsistencyLevel();
      sharedKeyspace = template.sharedKeyspace();
      username = template.username();
      writeConsistencyLevel = template.writeConsistencyLevel();

      // AbstractStore-specific configuration
      fetchPersistentState = template.fetchPersistentState();
      ignoreModifications = template.ignoreModifications();
      properties = template.properties();
      purgeOnStartup = template.purgeOnStartup();
      purgeSynchronously = template.purgeSynchronously();
      async.read(template.async());
      singletonStore.read(template.singletonStore());

      return this;
   }
}
