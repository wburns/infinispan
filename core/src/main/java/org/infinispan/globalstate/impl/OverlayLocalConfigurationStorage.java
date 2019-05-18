package org.infinispan.globalstate.impl;

import static org.infinispan.commons.util.Util.renameTempFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.util.concurrent.ConcurrentHashSet;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalStateConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.globalstate.LocalConfigurationStorage;

/**
 * An implementation of {@link LocalConfigurationStorage} which stores
 * {@link org.infinispan.commons.api.CacheContainerAdmin.AdminFlag#PERMANENT}
 *
 * This component persists cache configurations to the {@link GlobalStateConfiguration#persistentLocation()} in a
 * <pre>caches.xml</pre> file which is read on startup.
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public class OverlayLocalConfigurationStorage extends VolatileLocalConfigurationStorage {
   private ConcurrentHashSet<String> persistentCaches = new ConcurrentHashSet<>();

   @Override
   public void validateFlags(EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      GlobalConfiguration globalConfiguration = configurationManager.getGlobalConfiguration();
      if (flags.contains(CacheContainerAdmin.AdminFlag.PERMANENT) && !globalConfiguration.globalState().enabled())
         throw log.globalStateDisabled();
   }

   public void createCache(String name, String template, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      super.createCache(name, template, configuration, flags);
      if (flags.contains(CacheContainerAdmin.AdminFlag.PERMANENT)) {
         persistentCaches.add(name);
         storeAll();
      }
   }

   public void removeCache(String name, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      if (persistentCaches.remove(name)) {
         storeAll();
      }
      super.removeCache(name, flags);
   }

   public Map<String, Configuration> loadAll() {
      // Load any persisted configs

      try (FileInputStream fis = new FileInputStream(getPersistentFile())) {
         Map<String, Configuration> configurations = new HashMap<>();
         ConfigurationBuilderHolder holder = parserRegistry.parse(fis);
         for (Map.Entry<String, ConfigurationBuilder> entry : holder.getNamedConfigurationBuilders().entrySet()) {
            String name = entry.getKey();
            Configuration configuration = entry.getValue().build();
            configurations.put(name, configuration);
         }
         return configurations;
      } catch (FileNotFoundException e) {
         // Ignore
         return Collections.emptyMap();
      } catch (IOException e) {
         throw new CacheConfigurationException(e);
      }
   }

   private void storeAll() {
      try {
         GlobalConfiguration globalConfiguration = configurationManager.getGlobalConfiguration();
         File sharedDirectory = new File(globalConfiguration.globalState().sharedPersistentLocation());
         sharedDirectory.mkdirs();
         File temp = File.createTempFile("caches", null, sharedDirectory);
         Map<String, Configuration> configurationMap = new HashMap<>();
         for (String cacheName : persistentCaches) {
            configurationMap.put(cacheName, configurationManager.getConfiguration(cacheName, true));
         }
         try (FileOutputStream f = new FileOutputStream(temp)) {
            parserRegistry.serialize(f, null, configurationMap);
         }
         File persistentFile = getPersistentFile();
         try {
            renameTempFile(temp, getPersistentFileLock(), persistentFile);
         } catch (Exception e) {
             throw log.cannotRenamePersistentFile(temp.getAbsolutePath(), persistentFile, e);
         }
      } catch (Exception e) {
         throw log.errorPersistingGlobalConfiguration(e);
      }
   }

   private File getPersistentFile() {
      return new File(configurationManager.getGlobalConfiguration().globalState().sharedPersistentLocation(), "caches.xml");
   }

   private File getPersistentFileLock() {
      return new File(configurationManager.getGlobalConfiguration().globalState().sharedPersistentLocation(), "caches.xml.lck");
   }
}
