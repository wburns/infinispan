package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class Reproducer {

   private RemoteCacheManager cacheManager;
   private RemoteCache<String, Object> cache;

   static final int NUMBER_OF_ENTRIES = 100000;
   static final int THREAD_COUNT = 10;
   static final int GET_OPERATIONS = 1000000;

   public static void main(String[] args) throws Exception {
      Reproducer testCase = new Reproducer();
      testCase.putTest();
      testCase.getTest();
   }

   public Reproducer() throws Exception {
      cacheManager = new RemoteCacheManager(new ConfigurationBuilder().addServer().host("localhost").port(11222).build());
      cache = cacheManager.getCache();
      cache.clear();
   }

   public void putTest() throws Exception {
      Thread[] threads = new Thread[THREAD_COUNT];
      for (int i = 0; i < THREAD_COUNT; i++) {
         final int thread_index = i;
         threads[i] = new Thread(() -> {
            for(int j = 0; j < NUMBER_OF_ENTRIES; j++) {
               cache.put("key_" + thread_index + "_" + j, UUID.randomUUID().toString());
            }
         });
      }
      Long start = System.nanoTime();
      for (int i = 0; i < THREAD_COUNT; i++) {
         threads[i].start();
      }
      for (int i = 0; i < THREAD_COUNT; i++) {
         threads[i].join();
      }
      Long elapsed = System.nanoTime() - start;
      System.out.format("Puts took: %,d s", TimeUnit.NANOSECONDS.toSeconds(elapsed));
   }

   public void getTest() throws Exception {
      Thread[] threads = new Thread[THREAD_COUNT];
      for (int i = 0; i < THREAD_COUNT; i++) {
         final int thread_index = i;
         threads[i] = new Thread(() -> {
            Random r = new Random(thread_index);
            for(int j = 0; j < GET_OPERATIONS; j++) {
               int key_id = r.nextInt(200000);
               cache.get("key_" + thread_index + "_" + key_id);
            }
         });
      }
      Long start = System.nanoTime();
      for (int i = 0; i < THREAD_COUNT; i++) {
         threads[i].start();
      }
      for (int i = 0; i < THREAD_COUNT; i++) {
         threads[i].join();
      }
      Long elapsed = System.nanoTime() - start;
      System.out.format("\nGets took: %,d s", TimeUnit.NANOSECONDS.toSeconds(elapsed));
   }
}
