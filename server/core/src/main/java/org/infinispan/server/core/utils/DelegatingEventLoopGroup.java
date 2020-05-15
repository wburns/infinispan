package org.infinispan.server.core.utils;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;

public abstract class DelegatingEventLoopGroup implements EventLoopGroup {
   protected abstract EventLoopGroup delegate();

   @Override
   public boolean isShuttingDown() {
      return delegate().isShuttingDown();
   }

   @Override
   public Future<?> shutdownGracefully() {
      return delegate().shutdownGracefully();
   }

   @Override
   public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
      return delegate().shutdownGracefully(quietPeriod, timeout, unit);
   }

   @Override
   public Future<?> terminationFuture() {
      return delegate().terminationFuture();
   }

   @Override
   public void shutdown() {
      delegate().shutdown();
   }

   @Override
   public List<Runnable> shutdownNow() {
      return delegate().shutdownNow();
   }

   @Override
   public boolean isShutdown() {
      return delegate().isShutdown();
   }

   @Override
   public boolean isTerminated() {
      return delegate().isTerminated();
   }

   @Override
   public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return delegate().awaitTermination(timeout, unit);
   }

   @Override
   public EventLoop next() {
      return delegate().next();
   }

   @Override
   public Iterator<EventExecutor> iterator() {
      return delegate().iterator();
   }

   @Override
   public Future<?> submit(Runnable task) {
      return delegate().submit(task);
   }

   @Override
   public <T> List<java.util.concurrent.Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
      return delegate().invokeAll(tasks);
   }

   @Override
   public <T> List<java.util.concurrent.Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
      return delegate().invokeAll(tasks, timeout, unit);
   }

   @Override
   public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
      return delegate().invokeAny(tasks);
   }

   @Override
   public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return delegate().invokeAny(tasks, timeout, unit);
   }

   @Override
   public <T> Future<T> submit(Runnable task, T result) {
      return delegate().submit(task, result);
   }

   @Override
   public <T> Future<T> submit(Callable<T> task) {
      return delegate().submit(task);
   }

   @Override
   public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
      return delegate().schedule(command, delay, unit);
   }

   @Override
   public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
      return delegate().schedule(callable, delay, unit);
   }

   @Override
   public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
      return delegate().scheduleAtFixedRate(command, initialDelay, period, unit);
   }

   @Override
   public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
      return delegate().scheduleWithFixedDelay(command, initialDelay, delay, unit);
   }

   @Override
   public ChannelFuture register(Channel channel) {
      return delegate().register(channel);
   }

   @Override
   public ChannelFuture register(ChannelPromise promise) {
      return delegate().register(promise);
   }

   @Override
   public ChannelFuture register(Channel channel, ChannelPromise promise) {
      return delegate().register(channel, promise);
   }

   @Override
   public void execute(Runnable command) {
      delegate().execute(command);
   }
}
