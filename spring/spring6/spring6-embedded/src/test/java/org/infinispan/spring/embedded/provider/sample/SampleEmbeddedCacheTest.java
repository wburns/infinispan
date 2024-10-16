package org.infinispan.spring.embedded.provider.sample;

import org.infinispan.spring.common.InfinispanTestExecutionListener;
import org.infinispan.spring.embedded.provider.SpringEmbeddedCacheManager;
import org.infinispan.spring.embedded.provider.sample.service.CachedBookService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.testng.annotations.Test;

/**
 * Tests using embedded cache manager.
 *
 * @author Matej Cimbora (mcimbora@redhat.com)
 */
@Test(testName = "spring.embedded.provider.SampleEmbeddedCacheTest", groups = "functional")
@ContextConfiguration(locations = "classpath:/org/infinispan/spring/embedded/provider/sample/SampleEmbeddedCacheTestConfig.xml")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestExecutionListeners(value = InfinispanTestExecutionListener.class,  mergeMode =  TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS)
public class SampleEmbeddedCacheTest extends AbstractTestTemplateJsr107 {

   @Qualifier(value = "cachedBookServiceImpl")
   @Autowired
   private CachedBookService bookService;

   @Autowired
   private SpringEmbeddedCacheManager cacheManager;

   @Override
   public CachedBookService getBookService() {
      return bookService;
   }

   @Override
   public CacheManager getCacheManager() {
      return cacheManager;
   }
}
