package org.infinispan.server.hotrod.event;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.server.hotrod.test.TestClientListener;
import org.infinispan.server.hotrod.test.TestCustomEvent;
import org.infinispan.server.hotrod.test.TestKeyEvent;
import org.infinispan.server.hotrod.test.TestKeyWithVersionEvent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertByteArrayEquals;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

/**
 * @author Galder Zamarreño
 */
class EventLogListener extends TestClientListener {
   private final BlockingQueue<TestKeyWithVersionEvent> createdEvents = new ArrayBlockingQueue<>(128);
   private final BlockingQueue<TestKeyWithVersionEvent> modifiedEvents = new ArrayBlockingQueue<>(128);
   private final BlockingQueue<TestKeyEvent> removedEvents = new ArrayBlockingQueue<>(128);
   private final BlockingQueue<TestCustomEvent> customEvents = new ArrayBlockingQueue<>(128);
   private final AdvancedCache cache;

   EventLogListener(AdvancedCache cache) {
      this.cache = cache;
   }

   @Override
   public int queueSize(Event.Type eventType) {
      return queue(eventType).size();
   }

   @Override
   public Object pollEvent(Event.Type eventType) {
      try {
         return queue(eventType).poll(10, TimeUnit.SECONDS)
      } catch (InterruptedException e) {
         throw new CacheException(e);
      }
   }

   private BlockingQueue<?> queue(Event.Type eventType) {
      switch (eventType) {
         case CACHE_ENTRY_CREATED: return createdEvents;
         case CACHE_ENTRY_MODIFIED: return modifiedEvents;
         case CACHE_ENTRY_REMOVED: return removedEvents;
         default: throw new IllegalStateException("Unexpected event type: " + eventType);
      }
   }

   @Override
   public void onCreated(TestKeyWithVersionEvent event) {
      createdEvents.add(event);
   }

   @Override
   public void onModified(TestKeyWithVersionEvent event) {
      modifiedEvents.add(event);
   }

   @Override
   public void onRemoved(TestKeyEvent event) {
      removedEvents.add(event);
   }

   @Override
   public int customQueueSize() {
      return customEvents.size();
   }

   @Override
   public TestCustomEvent pollCustom() {
      try {
         return customEvents.poll(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
         throw new CacheException(e);
      }
   }

   @Override
   public void onCustom(TestCustomEvent event) {
      customEvents.add(event);
   }

   @Override
   public byte[] getId() {
      return new byte[]{1, 2, 3};
   }

   public void expectNoEvents() {
      expectNoEvents(Optional.empty());
   }

   public void expectNoEvents(Optional<Event.Type> eventType) {
      if (eventType.isPresent()) {
         assertEquals(0, queueSize(eventType.get()));
      } else {
         assertEquals(0, queueSize(Event.Type.CACHE_ENTRY_CREATED));
         assertEquals(0, queueSize(Event.Type.CACHE_ENTRY_MODIFIED));
         assertEquals(0, queueSize(Event.Type.CACHE_ENTRY_REMOVED));
         assertEquals(0, customQueueSize());
      }
   }

   public void expectOnlyRemovedEvent(byte[] k) {
      expectSingleEvent(k, Event.Type.CACHE_ENTRY_REMOVED);
      expectNoEvents(Optional.of(Event.Type.CACHE_ENTRY_CREATED));
      expectNoEvents(Optional.of(Event.Type.CACHE_ENTRY_MODIFIED));
   }

   public void expectOnlyModifiedEvent(byte[] k) {
      expectSingleEvent(k, Event.Type.CACHE_ENTRY_MODIFIED);
      expectNoEvents(Optional.of(Event.Type.CACHE_ENTRY_CREATED));
      expectNoEvents(Optional.of(Event.Type.CACHE_ENTRY_REMOVED));
   }

   public void expectOnlyCreatedEvent(byte[] k) {
      expectSingleEvent(k, Event.Type.CACHE_ENTRY_CREATED);
      expectNoEvents(Optional.of(Event.Type.CACHE_ENTRY_MODIFIED));
      expectNoEvents(Optional.of(Event.Type.CACHE_ENTRY_REMOVED));
   }

   public void expectSingleEvent(byte[] k, Event.Type eventType) {
      expectEvent(k, eventType);
      assertEquals(0, queueSize(eventType));
   }

   public void expectEvent(byte[] k, Event.Type eventType) {
      Object event = pollEvent(eventType);
      assertNotNull(event);
      if (event instanceof TestKeyWithVersionEvent) {
         assertByteArrayEquals(k, ((TestKeyWithVersionEvent) event).key);
         assertEquals(serverDataVersion(k, cache), ((TestKeyWithVersionEvent) event).dataVersion);
      } else if (event instanceof TestKeyEvent) {
         assertByteArrayEquals(k, ((TestKeyEvent) event).key);
      } else {
         throw new IllegalArgumentException("Unsupported event: " + event);
      }
   }

   boolean checkUnorderedKeyEvent(List<byte[]> assertedKeys, byte[] key, byte[] eventKey) {
      if (Arrays.equals(key, eventKey)) {
         assertFalse(assertedKeys.contains(key));
         assertedKeys.add(key);
         return true;
      } else return false;
   }

   public void expectUnorderedEvents(List<byte[]> keys, Event.Type eventType) {
      List<byte[]> assertedKeys = new ArrayList<>();

      for (int i = 0; i < keys.size(); ++i) {
         Object event = pollEvent(eventType);
         assertNotNull(event);
         int initialSize = assertedKeys.size();
         keys.forEach(key -> {
            if (event instanceof TestKeyWithVersionEvent) {
               TestKeyWithVersionEvent t = (TestKeyWithVersionEvent) event;
               boolean keyMatched = checkUnorderedKeyEvent(assertedKeys, key, t.key);
               if (keyMatched)
                  assertEquals(serverDataVersion(key, cache), t.dataVersion);
            } else if (event instanceof TestKeyEvent) {
               checkUnorderedKeyEvent(assertedKeys, key, ((TestKeyEvent) event).key);
            } else {
               throw new IllegalArgumentException("Unsupported event!" + event);
            }
         });
         int finalSize = assertedKeys.size();
         assertEquals(initialSize + 1, finalSize);
      }
   }

   public void expectSingleCustomEvent(byte[] eventData) {
      TestCustomEvent event = pollCustom();
      assertNotNull(event);
      assertByteArrayEquals(eventData, event.eventData);
      int remaining = customQueueSize();
      assertEquals(0, remaining);
   }

   public long serverDataVersion(byte[] k, AdvancedCache cache) {
      return ((NumericVersion) cache.getCacheEntry(k).getMetadata().version()).getVersion();
   }

}
