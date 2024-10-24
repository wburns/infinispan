package org.infinispan.spring.remote.provider.sample;

import org.infinispan.spring.common.InfinispanTestExecutionListener;
import org.infinispan.spring.remote.provider.SpringRemoteCacheManager;
import org.infinispan.spring.remote.provider.sample.service.CachedBookService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.testng.annotations.Test;

/**
 * Tests using remote cache manager.
 *
 * @author Matej Cimbora (mcimbora@redhat.com)
 */
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Test(testName = "spring.provider.SampleRemoteCacheTest", groups = "functional")
@ContextConfiguration(locations = "classpath:/org/infinispan/spring/remote/provider/sample/SampleRemoteCacheTestConfig.xml")
@TestExecutionListeners(value = InfinispanTestExecutionListener.class,  mergeMode =  TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS)
public class SampleRemoteCacheTest extends AbstractTestTemplateJsr107 {

   @Qualifier(value = "cachedBookServiceImpl")
   @Autowired(required = true)
   private CachedBookService bookService;

   @Autowired(required = true)
   private SpringRemoteCacheManager cacheManager;

   @Override
   public CachedBookService getBookService() {
      return bookService;
   }

   @Override
   public CacheManager getCacheManager() {
      return cacheManager;
   }
}
