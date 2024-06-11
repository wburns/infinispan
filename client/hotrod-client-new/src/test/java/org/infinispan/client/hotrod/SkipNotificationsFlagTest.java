package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.Flag.SKIP_LISTENER_NOTIFICATION;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.extractInterceptorChain;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.interceptors.BaseAsyncInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests if the {@link Flag#SKIP_LISTENER_NOTIFICATION} flag is received on HotRod server.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 10
 */
@Test(testName = "client.hotrod.SkipNotificationsFlagTest", groups = "functional")
@CleanupAfterTest
public class SkipNotificationsFlagTest extends SingleCacheManagerTest {

   private static final String KEY = "key";
   private static final String VALUE = "value";
   private FlagCheckCommandInterceptor commandInterceptor;
   private RemoteCache<String, String> remoteCache;
   private RemoteCacheManager remoteCacheManager;
   private HotRodServer hotRodServer;

   public void testPut() {
      performTest(RequestType.PUT);
   }

   public void testReplace() {
      performTest(RequestType.REPLACE);
   }

   public void testPutIfAbsent() {
      performTest(RequestType.PUT_IF_ABSENT);
   }

   public void testReplaceIfUnmodified() {
      performTest(RequestType.REPLACE_IF_UNMODIFIED);
   }

   public void testRemove() {
      performTest(RequestType.REMOVE);
   }

   public void testRemoveIfUnmodified() {
      performTest(RequestType.REMOVE_IF_UNMODIFIED);
   }

   public void testPutAll() {
      performTest(RequestType.PUT_ALL);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      cache = cacheManager.getCache();

      hotRodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);

      Properties hotRodClientConf = new Properties();
      hotRodClientConf.put("infinispan.client.hotrod.server_list", "localhost:" + hotRodServer.getPort());
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("localhost").port(hotRodServer.getPort());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCache = remoteCacheManager.getCache();
      return cacheManager;
   }

   @Override
   protected void teardown() {
      HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
      remoteCacheManager = null;
      HotRodClientTestingUtil.killServers(hotRodServer);
      hotRodServer = null;
      super.teardown();
   }

   private void performTest(RequestType type) {
      commandInterceptor.expectSkipIndexingFlag = true;
      type.execute(remoteCache.withFlags(SKIP_LISTENER_NOTIFICATION));
   }

   @BeforeClass(alwaysRun = true)
   private void injectCommandInterceptor() {
      if (remoteCache == null) {
         return;
      }

      this.commandInterceptor = new FlagCheckCommandInterceptor();
      extractInterceptorChain(cache).addInterceptor(commandInterceptor, 1);
   }

   @AfterClass(alwaysRun = true)
   private void resetCommandInterceptor() {
      if (commandInterceptor != null) {
         commandInterceptor.expectSkipIndexingFlag = false;
      }
   }

   private enum RequestType {
      PUT {
         @Override
         void execute(RemoteCache<String, String> cache) {
            cache.put(KEY, VALUE);
         }
      },
      REPLACE {
         @Override
         void execute(RemoteCache<String, String> cache) {
            cache.replace(KEY, VALUE);
         }
      },
      PUT_IF_ABSENT {
         @Override
         void execute(RemoteCache<String, String> cache) {
            cache.putIfAbsent(KEY, VALUE);
         }
      },
      REPLACE_IF_UNMODIFIED {
         @Override
         void execute(RemoteCache<String, String> cache) {
            cache.replaceWithVersion(KEY, VALUE, 0);
         }
      },
      REMOVE {
         @Override
         void execute(RemoteCache<String, String> cache) {
            cache.remove(KEY);
         }
      },
      REMOVE_IF_UNMODIFIED {
         @Override
         void execute(RemoteCache<String, String> cache) {
            cache.removeWithVersion(KEY, 0);
         }
      },
      PUT_ALL {
         @Override
         void execute(RemoteCache<String, String> cache) {
            Map<String, String> data = new HashMap<>();
            data.put(KEY, VALUE);
            cache.putAll(data);
         }
      },
      ;

      abstract void execute(RemoteCache<String, String> cache);
   }

   static class FlagCheckCommandInterceptor extends BaseAsyncInterceptor {

      private volatile boolean expectSkipIndexingFlag;

      @Override
      public Object visitCommand(InvocationContext ctx, VisitableCommand command) {
         if (command instanceof FlagAffectedCommand) {
            boolean hasFlag = ((FlagAffectedCommand) command).hasAnyFlag(FlagBitSets.SKIP_LISTENER_NOTIFICATION);
            if (expectSkipIndexingFlag && !hasFlag) {
               throw new CacheException("SKIP_LISTENER_NOTIFICATION flag is expected!");
            } else if (!expectSkipIndexingFlag && hasFlag) {
               throw new CacheException("SKIP_LISTENER_NOTIFICATION flag is *not* expected!");
            }
         }
         return invokeNext(ctx, command);
      }
   }


}
