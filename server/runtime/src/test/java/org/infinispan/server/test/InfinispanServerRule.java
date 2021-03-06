package org.infinispan.server.test;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.test.Exceptions;
import org.infinispan.test.fwk.TestResourceTracker;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.model.Statement;

import net.spy.memcached.MemcachedClient;

/**
 * Creates a cluster of servers to be used for running multiple tests It performs the following tasks:
 * <ul>
 * <li>It creates a temporary directory using the test name</li>
 * <li>It creates a common configuration directory to be shared by all servers</li>
 * <li>It creates a runtime directory structure for each server in the cluster (data, log, lib)</li>
 * <li>It populates the configuration directory with multiple certificates (ca.pfx, server.pfx, user1.pfx, user2.pfx)</li>
 * </ul>
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class InfinispanServerRule implements TestRule {
   public static final Log log = LogFactory.getLog(InfinispanServerRule.class);
   private final InfinispanServerTestConfiguration configuration;
   private final InfinispanServerDriver serverDriver;

   public InfinispanServerRule(InfinispanServerTestConfiguration configuration) {
      this.configuration = configuration;
      this.serverDriver = configuration.runMode().newDriver(configuration);
   }

   public InfinispanServerDriver getServerDriver() {
      return serverDriver;
   }

   @Override
   public Statement apply(Statement base, Description description) {
      return new Statement() {
         @Override
         public void evaluate() throws Throwable {
            String testName = description.getTestClass().getName();
            RunWith runWith = description.getTestClass().getAnnotation(RunWith.class);
            boolean inSuite = runWith != null && runWith.value() == Suite.class;
            if (!inSuite) {
               TestResourceTracker.testStarted(testName);
            }
            boolean manageServer = serverDriver.getStatus() == ComponentStatus.INSTANTIATED;
            try {
               if (manageServer) {
                  serverDriver.before(testName);
               }
               InfinispanServerRule.this.before(testName);
               base.evaluate();
            } finally {
               InfinispanServerRule.this.after(testName);
               if (manageServer) {
                  serverDriver.after(testName);
               }
               if (!inSuite) {
                  TestResourceTracker.testFinished(testName);
               }
            }
         }
      };
   }

   private void before(String name) {
   }

   private void after(String name) {
   }

   /**
    * @return a client configured against the first Hot Rod endpoint exposed by the server
    */
   RemoteCacheManager newHotRodClient() {
      return newHotRodClient(new ConfigurationBuilder());
   }

   /**
    * @return a client configured against the first Hot Rod endpoint exposed by the server
    */
   RemoteCacheManager newHotRodClient(ConfigurationBuilder builder) {
      // Add all known server addresses
      for (int i = 0; i < serverDriver.configuration.numServers(); i++) {
         InetSocketAddress serverAddress = serverDriver.getServerAddress(i, 11222);
         builder.addServer().host(serverAddress.getHostName()).port(serverAddress.getPort());
      }
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(builder.build());
      return remoteCacheManager;
   }

   public RestClient newRestClient() {
      return newRestClient(new RestClientConfigurationBuilder());
   }

   public RestClient newRestClient(RestClientConfigurationBuilder builder) {
      // Add all known server addresses
      for (int i = 0; i < serverDriver.configuration.numServers(); i++) {
         InetSocketAddress serverAddress = serverDriver.getServerAddress(i, 11222);
         builder.addServer().host(serverAddress.getHostName()).port(serverAddress.getPort());
      }
      return RestClient.forConfiguration(builder.build());
   }

   /**
    * @return a client configured against the first Memcached endpoint exposed by the server
    */
   CloseableMemcachedClient newMemcachedClient() {
      List<InetSocketAddress> addresses = new ArrayList<>();
      for (int i = 0; i < serverDriver.configuration.numServers(); i++) {
         InetSocketAddress unresolved = serverDriver.getServerAddress(i, 11221);
         addresses.add(new InetSocketAddress(unresolved.getHostName(), unresolved.getPort()));
      }
      MemcachedClient memcachedClient = Exceptions.unchecked(() -> new MemcachedClient(addresses));
      return new CloseableMemcachedClient(memcachedClient);
   }

   public static class CloseableMemcachedClient implements Closeable {
      final MemcachedClient client;

      public CloseableMemcachedClient(MemcachedClient client) {
         this.client = client;
      }

      public MemcachedClient getClient() {
         return client;
      }

      @Override
      public void close() {
         client.shutdown();
      }
   }
}
