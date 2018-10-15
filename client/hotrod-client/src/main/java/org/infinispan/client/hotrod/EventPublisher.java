package org.infinispan.client.hotrod;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.infinispan.client.hotrod.event.ClientCacheEntryCreatedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryCustomEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryExpiredEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryModifiedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryRemovedEvent;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;

/**
 * @author wburns
 * @since 10.0
 */
public interface EventPublisher<K> {
   EventPublisher<K> includeCurrentState();

   EventPublisher<K> filter(String ickleQuery);

   EventPublisher<ClientEvent> filterByEventType(Set<ClientEvent.Type> types);

   <R> Publisher<ClientCacheEntryCustomEvent<R>> filterAndMap(String ickleQuery);

   Publisher<ClientEvent> asPublisher();

   AutoCloseable subscribe(Consumer<? super ClientEvent> event);

   AutoCloseable subscribe(Consumer<? super ClientEvent> event,
         Consumer<? super Throwable> errorConsumer);

   AutoCloseable subscribe(Consumer<? super ClientCacheEntryCreatedEvent<K>> createdConsumer,
         Consumer<? super ClientCacheEntryModifiedEvent<K>> modifiedConsumer,
         Consumer<? super ClientCacheEntryRemovedEvent<K>> removedConsumer,
         Consumer<? super ClientCacheEntryExpiredEvent<K>> expiredConsumer,
         Consumer<? super Throwable> errorConsumer);
}


interface SafeCloseable extends AutoCloseable {
   @Override
   void close();


}

class TEst {
   void boo() {
      Flowable.fromPublisher(new EventPublisher<>().asPublisher()).su
   }
}