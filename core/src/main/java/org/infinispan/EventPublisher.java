package org.infinispan;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryExpiredEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.reactivestreams.Publisher;

/**
 * @author wburns
 * @since 10.0
 */
public interface EventPublisher<K, V> {
   EventPublisher<K, V> filter(Predicate<? super CacheEntryEvent<K, V>> filter);

   <R> Publisher<R> map(Function<? super CacheEntryEvent<K, V>, ? extends R> function);

   <R> Publisher<R> flatMap(Function<? super CacheEntryEvent<K, V>, ? extends Publisher<? extends R>> function);

   Publisher<CacheEntryCreatedEvent<K, V>> onlyCreatedEvents();

   Publisher<CacheEntryModifiedEvent<K, V>> onlyModifiedEvents();

   Publisher<CacheEntryRemovedEvent<K, V>> onlyRemovedEvents();

   Publisher<CacheEntryExpiredEvent<K, V>> onlyExpiredEvents();

   Publisher<CacheEntryEvent<K, V>> asPublisher();

   AutoCloseable subscribe(Consumer<? super CacheEntryEvent<K, V>> event);

   AutoCloseable subscribe(Consumer<? super CacheEntryEvent<K, V>> event,
         Consumer<? super Throwable> errorConsumer);

   AutoCloseable subscribe(Consumer<? super CacheEntryCreatedEvent<K, V>> createdConsumer,
         Consumer<? super CacheEntryModifiedEvent<K, V>> modifiedConsumer,
         Consumer<? super CacheEntryRemovedEvent<K, V>> removedConsumer,
         Consumer<? super CacheEntryExpiredEvent<K, V>> expiredConsumer,
         Consumer<? super Throwable> errorConsumer);
}
