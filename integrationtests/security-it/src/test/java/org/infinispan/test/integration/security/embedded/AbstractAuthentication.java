package org.infinispan.test.integration.security.embedded;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.Map.Entry;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.GlobalRoleConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.Security;
import org.infinispan.test.integration.security.utils.LoginHandler;
import org.infinispan.transaction.LockingMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author <a href="mailto:vjuranek@redhat.com">Vojtech Juranek</a>
 * @since 7.0
 */
public abstract class AbstractAuthentication {

   public static final String DEFAULT_DEPLOY_CONTAINER = "testnode";
   public static final String CACHE_NAME = "secureCache";
   public static final String TEST_ENTRY_VALUE = "predefined value";

   protected EmbeddedCacheManager manager;
   public static final String TEST_ENTRY_KEY = "predefined key";
   protected Cache<Object, Object> secureCache;
   private GlobalConfigurationBuilder globalConfig;
   private ConfigurationBuilder cacheConfig;

   public abstract String getSecurityDomainName();

   public abstract Map<String, AuthorizationPermission[]> getRolePermissionMap();

   public abstract PrincipalRoleMapper getPrincipalRoleMapper();

   public abstract Subject getAdminSubject() throws LoginException;

   public abstract Subject getWriterSubject() throws LoginException;

   public abstract Subject getReaderSubject() throws LoginException;

   public abstract Subject getUnprivilegedSubject() throws LoginException;

   public Subject authenticate(String login, String password) throws LoginException {
      final String securityDomain = System.getProperty("jboss.security.domain", getSecurityDomainName());
      LoginContext lc = new LoginContext(securityDomain, new LoginHandler(login, password));
      lc.login();
      return lc.getSubject();
   }

   public Subject authenticateWithKrb(String krbSecurityDomain) throws LoginException {
      LoginContext lc = new LoginContext(krbSecurityDomain, new LoginHandler(null, null));
      lc.login();
      return lc.getSubject();
   }

   @Before
   public void setupCache() throws Exception {
      //global setup
      globalConfig = new GlobalConfigurationBuilder();

      GlobalAuthorizationConfigurationBuilder globalRoles = globalConfig.security().authorization().enable()
            .principalRoleMapper(getPrincipalRoleMapper());

      //cache setup
      cacheConfig = new ConfigurationBuilder();
      cacheConfig.transaction().lockingMode(LockingMode.PESSIMISTIC);
      cacheConfig.invocationBatching().enable();
      AuthorizationConfigurationBuilder authConfig = cacheConfig.security().authorization().enable();

      //authorization setup
      Map<String, AuthorizationPermission[]> rolePermissionMap = getRolePermissionMap();
      for (Entry<String, AuthorizationPermission[]> role : rolePermissionMap.entrySet()) {
         authConfig = authConfig.role(role.getKey());
         GlobalRoleConfigurationBuilder roleBuilder = globalRoles.role(role.getKey());
         for (AuthorizationPermission permission : role.getValue()) {
            roleBuilder = roleBuilder.permission(permission);
         }
      }

      Subject admin = getAdminSubject();
      Security.doAs(admin, () -> {
         manager = new DefaultCacheManager(globalConfig.build());
         manager.defineConfiguration(CACHE_NAME, cacheConfig.build());
         secureCache = manager.getCache(CACHE_NAME);
         secureCache.put(TEST_ENTRY_KEY, TEST_ENTRY_VALUE);
      });
   }

   @After
   public void tearDown() throws Exception {
      if (manager != null) {
         Subject admin = getAdminSubject();
         Security.doAs(admin, () -> manager.stop());
      }
   }

   @Test
   public void testAdminCRUD() throws Exception {
      Subject admin = getAdminSubject();
      Security.doAs(admin, () -> {
         assertEquals(TEST_ENTRY_VALUE, secureCache.get(TEST_ENTRY_KEY));
         secureCache.putIfAbsent("test", "test value");
         assertEquals("test value", secureCache.get("test"));
         secureCache.put("test", "test value2");
         assertEquals("test value2", secureCache.get("test"));
         secureCache.remove("test");
         assertEquals(null, secureCache.get("test"));
         secureCache.clear();
         assertEquals(0, secureCache.size());
      });
   }

   @Test
   public void testWriterWrite() throws Exception {
      Subject writer = getWriterSubject();
      Security.doAs(writer, () -> secureCache.put("test", "test value")
      );
   }

   @Test
   public void testWriterRemove() throws Exception {
      Subject writer = getWriterSubject();
      Security.doAs(writer, () -> secureCache.remove(TEST_ENTRY_KEY));
   }

   @Test(expected = SecurityException.class)
   public void testWriterRead() throws Exception {
      Subject writer = getWriterSubject();
      Security.doAs(writer, () -> secureCache.get(TEST_ENTRY_KEY));
   }

   @Test
   public void testReaderRead() throws Exception {
      Subject reader = getReaderSubject();
      Security.doAs(reader, () -> assertEquals(TEST_ENTRY_VALUE, secureCache.get(TEST_ENTRY_KEY)));
   }

   @Test(expected = SecurityException.class)
   public void testReaderWrite() throws Exception {
      Subject reader = getReaderSubject();
      Security.doAs(reader, () -> secureCache.put("test", "test value"));
   }

   @Test(expected = SecurityException.class)
   public void testReaderRemove() throws Exception {
      Subject reader = getReaderSubject();
      Security.doAs(reader, () -> secureCache.remove(TEST_ENTRY_KEY));
   }

   @Test(expected = SecurityException.class)
   public void testUnprivilegedRead() throws Exception {
      Subject unprivileged = getUnprivilegedSubject();
      Security.doAs(unprivileged, () -> secureCache.get(TEST_ENTRY_KEY));
   }

   @Test(expected = SecurityException.class)
   public void testUnprivilegedWrite() throws Exception {
      Subject unprivileged = getUnprivilegedSubject();
      Security.doAs(unprivileged, ()-> secureCache.put("test", "test value"));
   }

   @Test(expected = SecurityException.class)
   public void testUnprivilegedRemove() throws Exception {
      Subject unprivileged = getUnprivilegedSubject();
      Security.doAs(unprivileged, () -> secureCache.remove(TEST_ENTRY_KEY));
   }

   @Test(expected = SecurityException.class)
   public void testUnauthenticatedRead() {
      secureCache.get(TEST_ENTRY_KEY);
   }

   @Test(expected = SecurityException.class)
   public void testUnauthenticatedWrite() {
      secureCache.put("test", "value");
   }

   @Test(expected = SecurityException.class)
   public void testUnauthenticatedRemove() {
      secureCache.remove(TEST_ENTRY_KEY);
   }
}
