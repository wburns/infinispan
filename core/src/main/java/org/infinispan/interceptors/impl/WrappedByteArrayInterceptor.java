package org.infinispan.interceptors.impl;

import java.io.Serializable;
import java.util.Spliterator;
import java.util.concurrent.CompletableFuture;

import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.cache.impl.Caches;
import org.infinispan.commands.LocalFlagAffectedCommand;
import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorMapper;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.Util;
import org.infinispan.compat.TypeConverter;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.BasicInvocationStage;
import org.infinispan.interceptors.compat.BaseTypeConverterInterceptor;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.stream.impl.interceptor.AbstractDelegatingEntryCacheSet;
import org.infinispan.stream.impl.interceptor.AbstractDelegatingKeyCacheSet;
import org.infinispan.stream.impl.spliterators.IteratorAsSpliterator;
import org.infinispan.util.function.RemovableFunction;

/**
 * @author wburns
 * @since 9.0
 */
public class WrappedByteArrayInterceptor<K, V> extends BaseTypeConverterInterceptor<K, V> {
   @Override
   protected TypeConverter<Object, Object, Object, Object> determineTypeConverter(LocalFlagAffectedCommand command) {
      return new WrappedByteArrayConverter();
   }

   public static class WrappedByteArrayConverter implements TypeConverter<Object, Object, Object, Object> {
      @Override
      public Object unboxKey(Object key) {
         return key instanceof WrappedByteArray ? ((WrappedByteArray) key).getBytes() : key;
      }

      @Override
      public Object unboxValue(Object value) {
         return value instanceof WrappedByteArray ? ((WrappedByteArray) value).getBytes() : value;
      }

      @Override
      public Object boxKey(Object target) {
         return target instanceof byte[] ? new WrappedByteArray((byte[]) target) : target;
      }

      @Override
      public Object boxValue(Object target) {
         return target instanceof byte[] ? new WrappedByteArray((byte[]) target) : target;
      }

      @Override
      public boolean supportsInvocation(Flag flag) {
         // Shouldn't be used
         return false;
      }

      @Override
      public void setMarshaller(Marshaller marshaller) {
         // Do nothing
      }
   }

   protected void addVersionIfNeeded(MetadataAwareCommand cmd) {
      // Do nothing, no versions
   }

   @Override
   public BasicInvocationStage visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
      if (!ctx.isOriginLocal())
         return invokeNext(ctx, command);

      return invokeNext(ctx, command).thenApply((rCtx, rCommand, rv) -> {
         EntrySetCommand entrySetCommand = (EntrySetCommand) rCommand;

         TypeConverter<Object, Object, Object, Object> converter = determineTypeConverter(entrySetCommand);
         CacheSet<CacheEntry<K, V>> set = (CacheSet<CacheEntry<K, V>>) rv;
         return new AbstractDelegatingEntryCacheSet<K, V>(Caches.getCacheWithFlags(cache, entrySetCommand), set) {
            @Override
            public CloseableIterator<CacheEntry<K, V>> iterator() {
               return new TypeConverterIterator<>(super.iterator(), converter, entryFactory);
            }

            @Override
            public CloseableSpliterator<CacheEntry<K, V>> spliterator() {
               return new IteratorAsSpliterator.Builder<>(iterator())
                     .setEstimateRemaining(super.spliterator().estimateSize()).setCharacteristics(
                           Spliterator.CONCURRENT | Spliterator.DISTINCT | Spliterator.NONNULL).get();
            }

            @Override
            protected CacheStream<CacheEntry<K, V>> getStream(boolean parallel) {
               CacheStream<CacheEntry<K, V>> stream = parallel ? set.parallelStream() : set.stream();
               return stream.map(instance);
            }
         };
      });
   }

   static final EntryMapper instance = new EntryMapper();

   static class EntryMapper<K, V> implements RemovableFunction<CacheEntry<K, V>, CacheEntry<K, V>>, Serializable {
      private transient InternalEntryFactory entryFactory;

      @Inject
      public void injectFactory(InternalEntryFactory factory) {
         this.entryFactory = factory;
      }

      @Override
      public CacheEntry<K, V> apply(CacheEntry<K, V> e) {
         K key = e.getKey();
         Object newKey = key instanceof WrappedByteArray ? ((WrappedByteArray) key).getBytes() : key;
         V value = e.getValue();
         Object newValue = value instanceof WrappedByteArray ? ((WrappedByteArray) value).getBytes() : value;
         if (key != newKey || value != newValue) {
            return (CacheEntry<K, V>) entryFactory.create(newKey, newValue, e.getMetadata());
         }
         return e;
      }
   }


   @Override
   public BasicInvocationStage visitKeySetCommand(InvocationContext ctx, KeySetCommand command)
         throws Throwable {
      if (!ctx.isOriginLocal())
         return invokeNext(ctx, command);

      return invokeNext(ctx, command).thenApply((rCtx, rCommand, rv) -> {

         KeySetCommand keySetCommand = (KeySetCommand) rCommand;
         TypeConverter<Object, Object, Object, Object> converter = determineTypeConverter(keySetCommand);
         CacheSet<K> set = (CacheSet<K>) rv;
         return new AbstractDelegatingKeyCacheSet<K, V>(Caches.getCacheWithFlags(cache, keySetCommand), set) {
            @Override
            public CloseableIterator<K> iterator() {
               return new CloseableIteratorMapper<>(super.iterator(), k -> (K) converter.unboxKey(k));
            }

            @Override
            public CloseableSpliterator<K> spliterator() {
               return new IteratorAsSpliterator.Builder<>(iterator())
                     .setEstimateRemaining(super.spliterator().estimateSize()).setCharacteristics(
                           Spliterator.CONCURRENT | Spliterator.DISTINCT | Spliterator.NONNULL).get();
            }

            @Override
            protected CacheStream<K> getStream(boolean parallel) {
               CacheStream<K> stream = parallel ? set.parallelStream() : set.stream();
               return stream.map((RemovableFunction<K, K> & Serializable) k -> k instanceof WrappedByteArray ? (K) ((WrappedByteArray) k).getBytes() : k);
            }
         };
      });
   }

}
