package org.infinispan.notifications.cachelistener;

import org.infinispan.notifications.cachelistener.event.Event;

/**
 * @author wburns
 * @since 9.0
 */
public class EventWrapper<K, V, E extends Event<K, V>> {
   private E event;
   private final K key;

   public EventWrapper(K key, E event) {
      this.event = event;
      this.key = key;
   }

   public E getEvent() {
      return event;
   }

   public void setEvent(E event) {
      this.event = event;
   }

   public K getKey() {
      return key;
   }
}
