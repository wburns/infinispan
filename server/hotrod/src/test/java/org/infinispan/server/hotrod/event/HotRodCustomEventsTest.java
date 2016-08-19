package org.infinispan.server.hotrod.event;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.HotRodSingleNodeTest;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.util.KeyValuePair;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.k;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.v;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.withClientListener;

/**
 * @author Galder Zamarreño
 */
@Test(groups = "functional", testName = "server.hotrod.event.HotRodCustomEventsTest")
class HotRodCustomEventsTest extends HotRodSingleNodeTest {

   @Override
   public HotRodServer createStartHotRodServer(EmbeddedCacheManager cacheManager) {
      HotRodServer server = HotRodTestingUtil.startHotRodServer(cacheManager);
      server.addCacheEventConverterFactory("static-converter-factory", new StaticConverterFactory());
      server.addCacheEventConverterFactory("dynamic-converter-factory", new DynamicConverterFactory());
      return server;
   }

   public void testCustomEvents(Method m) {
      EventLogListener eventListener = new EventLogListener();
      withClientListener(client(), eventListener, Optional.empty(),
              Optional.of(new KeyValuePair<>("static-converter-factory", Collections.emptyList())), () -> {
         eventListener.expectNoEvents();
         byte[] key = k(m);
         client().remove(key);
         eventListener.expectNoEvents();
         byte keyLength = (byte) key.length;
         byte[] value = v(m);
         byte valueLength = (byte) value.length;
         client().put(key, 0, 0, value);

         byte[] event = Arrays.copyOf(key, keyLength + valueLength);
         System.arraycopy(value, 0, event, keyLength, valueLength);
         eventListener.expectSingleCustomEvent(event);
         byte[] value2 = v(m, "v2-");
         byte value2Length = (byte) value2.length;
         client().put(key, 0, 0, value2);

         byte[] event2 = Arrays.copyOf(key, keyLength + value2Length);
         System.arraycopy(value2, 0, event, keyLength, value2Length);
         eventListener.expectSingleCustomEvent(event2);
         client().remove(key);
         eventListener.expectSingleCustomEvent(key);
      });
   }

   public void testParameterBasedConversion(Method m) {
      EventLogListener eventListener = new EventLogListener();
      byte[] customConvertKey = new byte[] {4, 5, 6};
      withClientListener(client(), eventListener, Optional.empty(), Optional.of(new KeyValuePair<>("dynamic-converter-factory", Collections.singletonList(new byte[] {4, 5, 6}))), () -> {
         eventListener.expectNoEvents();
         byte[] key = k(m);
         byte keyLength = (byte) key.length;
         byte[] value = v(m);
         byte valueLength = (byte) value.length;
         client().put(key, 0, 0, value);

         byte[] event = Arrays.copyOf(key, keyLength + valueLength);
         System.arraycopy(value, 0, event, keyLength, valueLength);
         eventListener.expectSingleCustomEvent(event);
         byte[] value2 = v(m, "v2-");
         byte value2Length = (byte) value2.length;
         client().put(key, 0, 0, value2);

         byte[] event2 = Arrays.copyOf(key, keyLength + value2Length);
         System.arraycopy(value2, 0, event, keyLength, value2Length);
         eventListener.expectSingleCustomEvent(event2);
         client().remove(key);
         eventListener.expectSingleCustomEvent(key);
         client().put(customConvertKey, 0, 0, value);
         eventListener.expectSingleCustomEvent(customConvertKey);
      });
   }

   public void testConvertedEventsNoReplay(Method m) {
      EventLogListener eventListener = new EventLogListener();
      byte[] key = new byte[] {1};
      byte[] value = new byte[] {2};
      client().put(key, 0, 0, value);
      withClientListener(client(), eventListener, Optional.empty(), Optional.of(new KeyValuePair<>("static-converter-factory", Collections.emptyList())), () ->
         eventListener.expectNoEvents());
   }

   public void testConvertedEventsReplay(Method m) {
      EventLogListener eventListener = new EventLogListener();
      byte[] key = new byte[] {1};
      byte keyLength = (byte) key.length;
      byte[] value = new byte[] {2};
      byte valueLength = (byte) value.length;
      client().put(key, 0, 0, value);
      withClientListener(client(), eventListener, Optional.empty(), Optional.of(new KeyValuePair<>("static-converter-factory", Collections.emptyList())), true, true, () -> {
         byte[] event = Arrays.copyOf(key, keyLength + valueLength);
         System.arraycopy(value, 0, event, keyLength, valueLength);
         eventListener.expectSingleCustomEvent(event);
      });
   }

   class StaticConverterFactory implements CacheEventConverterFactory {
      @Override
      public <K, V, C> CacheEventConverter<K, V, C> getConverter(Object[] params) {
         CacheEventConverter<byte[], byte[], byte[]> converter = (key, prevValue, prevMetadata, value, metadata, eventType) -> {
            byte keyLength = (byte) key.length;
            if (value == null) return key;
            else {
               byte[] event = Arrays.copyOf(key, keyLength + value.length);
               System.arraycopy(value, 0, event, keyLength, value.length);
               return event;
            }
         };
         return (CacheEventConverter<K, V, C>) converter;
      }
   }

   class DynamicConverterFactory implements CacheEventConverterFactory {
      @Override
      public <K, V, C> CacheEventConverter<K, V, C> getConverter(Object[] params) {
         CacheEventConverter<byte[], byte[], byte[]> converter = (key, prevValue, prevMetadata, value, metadata, eventType) -> {
            byte keyLength = (byte) key.length;
            if (value == null || Arrays.equals((byte[]) params[0], key))
               return key;
            else {
               byte[] event = Arrays.copyOf(key, keyLength + value.length);
               System.arraycopy(value, 0, event, keyLength, value.length);
               return event;
            }
         };
         return (CacheEventConverter<K, V, C>) converter;
      }
   }

}
