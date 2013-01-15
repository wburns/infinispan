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

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.infinispan.configuration.cache.AbstractStoreConfigurationChildBuilder;
import org.infinispan.loaders.keymappers.Key2StringMapper;

/**
 * AbstractCassandraCacheStoreConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public abstract class AbstractCassandraCacheStoreConfigurationChildBuilder<T> extends
      AbstractStoreConfigurationChildBuilder<T> implements AstyanaxCacheStoreConfigurationChildBuilder<T> {

   private AstyanaxCacheStoreConfigurationBuilder builder;

   protected AbstractCassandraCacheStoreConfigurationChildBuilder(AstyanaxCacheStoreConfigurationBuilder builder) {
      super(builder);
      this.builder = builder;
   }

   @Override
   public AstyanaxCacheStoreConfigurationBuilder autoCreateKeyspace(boolean autoCreateKeyspace) {
      return builder.autoCreateKeyspace(autoCreateKeyspace);
   }

   @Override
   public AstyanaxCacheStoreConfigurationBuilder entryColumnFamily(String entryColumnFamily) {
      return builder.entryColumnFamily(entryColumnFamily);
   }

   @Override
   public AstyanaxCacheStoreConfigurationBuilder expirationColumnFamily(String expirationColumnFamily) {
      return builder.expirationColumnFamily(expirationColumnFamily);
   }

   @Override
   public AstyanaxCacheStoreConfigurationBuilder keySpace(String keySpace) {
      return builder.keySpace(keySpace);
   }

   @Override
   public AstyanaxCacheStoreConfigurationBuilder password(String password) {
      return builder.password(password);
   }

   @Override
   public AstyanaxCacheStoreConfigurationBuilder readConsistencyLevel(ConsistencyLevel readConsistencyLevel) {
      return builder.readConsistencyLevel(readConsistencyLevel);
   }

   @Override
   public AstyanaxCacheStoreConfigurationBuilder username(String username) {
      return builder.username(username);
   }

   @Override
   public AstyanaxCacheStoreConfigurationBuilder writeConsistencyLevel(ConsistencyLevel writeConsistencyLevel) {
      return builder.writeConsistencyLevel(writeConsistencyLevel);
   }

   @Override
   public AstyanaxServerConfigurationBuilder addServer() {
      return builder.addServer();
   }

}
