package org.infinispan.client.hotrod;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.infinispan.client.hotrod.event.ClientCacheEntryCreatedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryExpiredEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryModifiedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryRemovedEvent;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.reactivestreams.Publisher;

/**
 * @author wburns
 * @since 10.0
 */
public interface EventPublisher<K> {
   EventPublisher<K> includeCurrentState();

   EventPublisher<K> filter(Predicate<? super ClientEvent> filter);

   <R> Publisher<R> map(Function<? super ClientEvent, ? extends R> function);

   <R> Publisher<R> flatMap(Function<? super ClientEvent, ? extends Publisher<? extends R>> function);

   Publisher<ClientCacheEntryCreatedEvent<K>> onlyCreatedEvents();

   Publisher<ClientCacheEntryModifiedEvent<K>> onlyModifiedEvents();

   Publisher<ClientCacheEntryRemovedEvent<K>> onlyRemovedEvents();

   Publisher<ClientCacheEntryExpiredEvent<K>> onlyExpiredEvents();

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
