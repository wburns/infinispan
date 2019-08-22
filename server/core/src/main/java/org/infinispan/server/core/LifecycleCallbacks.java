package org.infinispan.server.core;

import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.dataconversion.BinaryTranscoder;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.server.core.dataconversion.JBossMarshallingTranscoder;
import org.infinispan.server.core.dataconversion.JavaSerializationTranscoder;
import org.infinispan.server.core.dataconversion.JsonTranscoder;
import org.infinispan.server.core.dataconversion.ProtostreamBinaryTranscoder;
import org.infinispan.server.core.dataconversion.XMLTranscoder;

/**
 * Module lifecycle callbacks implementation that enables module specific
 * {@link org.infinispan.commons.marshall.AdvancedExternalizer} implementations to be registered.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@InfinispanModule(name = "server-core", requiredModules = "core")
public class LifecycleCallbacks implements ModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      ClassWhiteList classWhiteList = gcr.getComponent(EmbeddedCacheManager.class).getClassWhiteList();
      ClassLoader classLoader = globalConfiguration.classLoader();
      Marshaller marshaller = jbossMarshaller(classLoader, classWhiteList);

      EncoderRegistry encoderRegistry = gcr.getComponent(EncoderRegistry.class);
      JsonTranscoder jsonTranscoder = new JsonTranscoder(classLoader, classWhiteList);

      encoderRegistry.registerTranscoder(jsonTranscoder);
      encoderRegistry.registerTranscoder(new XMLTranscoder(classLoader, classWhiteList));
      encoderRegistry.registerTranscoder(new JavaSerializationTranscoder(classWhiteList));
      encoderRegistry.registerTranscoder(new ProtostreamBinaryTranscoder());

      if (marshaller != null) {
         encoderRegistry.registerTranscoder(new JBossMarshallingTranscoder(jsonTranscoder, marshaller));
         BinaryTranscoder transcoder = encoderRegistry.getTranscoder(BinaryTranscoder.class);
         transcoder.overrideMarshaller(marshaller);
      }
   }

   Marshaller jbossMarshaller(ClassLoader classLoader, ClassWhiteList classWhiteList) {
      try {
         Class<?> marshallerClass = classLoader.loadClass("org.infinispan.jboss.marshalling.commons.GenericJBossMarshaller");
         return Util.newInstanceOrNull(marshallerClass.asSubclass(Marshaller.class),
               new Class[] { ClassLoader.class, ClassWhiteList.class}, classLoader, classWhiteList);
      } catch (ClassNotFoundException e) {
         return null;
      }
   }
}
