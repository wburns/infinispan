package org.infinispan.interceptors.impl;

import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.marshall.core.WrappedByteArray;

/**
 * @author wburns
 * @since 9.0
 */
public class WrappedByteArrayInterceptor extends DDAsyncInterceptor {

   private InternalEntryFactory entryFactory;

   @Inject
   protected void init(InternalEntryFactory entryFactory) {
      this.entryFactory = entryFactory;
   }

   @Override
   public CompletableFuture<Void> visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         Object key = command.getKey();
         if (key instanceof byte[]) {
            command.setKey(new WrappedByteArray((byte[]) key));
         }
         return unwrapReturn(ctx);
      } else {
         return handleDefault(ctx, command);
      }
   }

   @Override
   public CompletableFuture<Void> visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
      Object key = command.getKey();
      if (key instanceof byte[]) {
         command.setKey(new WrappedByteArray((byte[]) key));
      }
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         if (throwable != null)
            throw throwable;

         if (rv == null) {
            return null;
         }

         CacheEntry entry = (CacheEntry) rv;
         Object value = entry.getValue();

         if (value instanceof WrappedByteArray) {
            // Create a copy of the entry to avoid modifying the internal entry
            return CompletableFuture.completedFuture(entryFactory.create(entry.getKey(),
                  ((WrappedByteArray) value).getBytes(), entry.getMetadata(), entry.getLifespan(), entry.getMaxIdle()));
         } else {
            return null;
         }
      });
   }

   @Override
   public CompletableFuture<Void> visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         Object key = command.getKey();
         if (key instanceof byte[]) {
            command.setKey(new WrappedByteArray((byte[]) key));
         }
         Object value = command.getValue();
         if (value instanceof byte[]) {
            command.setValue(new WrappedByteArray((byte[]) value));
         }
         return unwrapReturn(ctx);
      } else {
         return handleDefault(ctx, command);
      }
   }

   @Override
   public CompletableFuture<Void> visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         Object key = command.getKey();
         if (key instanceof byte[]) {
            command.setKey(new WrappedByteArray((byte[]) key));
         }
         Object value = command.getOldValue();
         if (value instanceof byte[]) {
            command.setOldValue(new WrappedByteArray((byte[]) value));
         }
         value = command.getNewValue();
         if (value instanceof byte[]) {
            command.setNewValue(new WrappedByteArray((byte[]) value));
         }
         return unwrapReturn(ctx);
      } else {
         return handleDefault(ctx, command);
      }
   }

   private static CompletableFuture<Void> unwrapReturn(InvocationContext ctx) {
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         if (throwable != null)
            throw throwable;

         if (rv instanceof WrappedByteArray) {
            return CompletableFuture.completedFuture(((WrappedByteArray) rv).getBytes());
         } else {
            return null;
         }
      });
   }
}
