package org.infinispan.util.rxjava;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.FlowableSubscriber;
import io.reactivex.annotations.Nullable;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.internal.functions.ObjectHelper;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.SpscArrayQueue;
import io.reactivex.internal.subscriptions.BasicIntQueueSubscription;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;

/**
 * @author wburns
 * @since 10.0
 */
// Copied from FlowableOnBackpressureBuffer from rxjava 2.2.2 and modified to replace with clear notification
public class FlowableOnBackpressureBufferClear<T> extends AbstractFlowableWithUpstream<T, T> {
   final int bufferSize;
   final boolean delayError;
   final Callable<? extends T> cleared;

   public FlowableOnBackpressureBufferClear(Flowable<T> source, int bufferSize, boolean delayError,
         Callable<? extends T> cleared) {
      super(source);
      this.bufferSize = ObjectHelper.verifyPositive(bufferSize, "bufferSize");
      this.delayError = delayError;
      this.cleared = ObjectHelper.requireNonNull(cleared, "cleared is null");
   }

   @Override
   protected void subscribeActual(Subscriber<? super T> s) {
      source.subscribe(new FlowableOnBackpressureBufferClear.BackpressureBufferSubscriber<T>(s, bufferSize, delayError,
            cleared));
   }

   static final class BackpressureBufferSubscriber<T> extends BasicIntQueueSubscription<T> implements FlowableSubscriber<T> {

      private static final long serialVersionUID = -2514538129242366402L;

      final Subscriber<? super T> downstream;
      final SimplePlainQueue<T> queue;
      final boolean delayError;
      final Callable<? extends T> cleared;

      Subscription upstream;

      volatile boolean cancelled;

      volatile boolean done;
      Throwable error;

      final AtomicLong requested = new AtomicLong();

      boolean outputFused;

      BackpressureBufferSubscriber(Subscriber<? super T> actual, int bufferSize, boolean delayError,
            Callable<? extends T> cleared) {
         this.downstream = actual;
         this.delayError = delayError;
         this.cleared = cleared;

         this.queue = new SpscArrayQueue<T>(bufferSize);
      }

      @Override
      public void onSubscribe(Subscription s) {
         if (SubscriptionHelper.validate(this.upstream, s)) {
            this.upstream = s;
            downstream.onSubscribe(this);
            s.request(Long.MAX_VALUE);
         }
      }

      @Override
      public void onNext(T t) {
         if (!queue.offer(t)) {
            // When queue is full replace contents with the cleared indicator
            queue.clear();

            try {
               T clearedEntry = cleared.call();
               queue.offer(clearedEntry);
            } catch (Throwable e) {
               Exceptions.throwIfFatal(e);
               onError(e);
               return;
            }
         }
         if (outputFused) {
            downstream.onNext(null);
         } else {
            drain();
         }
      }

      @Override
      public void onError(Throwable t) {
         error = t;
         done = true;
         if (outputFused) {
            downstream.onError(t);
         } else {
            drain();
         }
      }

      @Override
      public void onComplete() {
         done = true;
         if (outputFused) {
            downstream.onComplete();
         } else {
            drain();
         }
      }

      @Override
      public void request(long n) {
         if (!outputFused) {
            if (SubscriptionHelper.validate(n)) {
               BackpressureHelper.add(requested, n);
               drain();
            }
         }
      }

      @Override
      public void cancel() {
         if (!cancelled) {
            cancelled = true;
            upstream.cancel();

            if (getAndIncrement() == 0) {
               queue.clear();
            }
         }
      }

      void drain() {
         if (getAndIncrement() == 0) {
            int missed = 1;
            final SimplePlainQueue<T> q = queue;
            final Subscriber<? super T> a = downstream;
            for (;;) {

               if (checkTerminated(done, q.isEmpty(), a)) {
                  return;
               }

               long r = requested.get();

               long e = 0L;

               while (e != r) {
                  boolean d = done;
                  T v = q.poll();
                  boolean empty = v == null;

                  if (checkTerminated(d, empty, a)) {
                     return;
                  }

                  if (empty) {
                     break;
                  }

                  a.onNext(v);

                  e++;
               }

               if (e == r) {
                  boolean d = done;
                  boolean empty = q.isEmpty();

                  if (checkTerminated(d, empty, a)) {
                     return;
                  }
               }

               if (e != 0L) {
                  if (r != Long.MAX_VALUE) {
                     requested.addAndGet(-e);
                  }
               }

               missed = addAndGet(-missed);
               if (missed == 0) {
                  break;
               }
            }
         }
      }

      boolean checkTerminated(boolean d, boolean empty, Subscriber<? super T> a) {
         if (cancelled) {
            queue.clear();
            return true;
         }
         if (d) {
            if (delayError) {
               if (empty) {
                  Throwable e = error;
                  if (e != null) {
                     a.onError(e);
                  } else {
                     a.onComplete();
                  }
                  return true;
               }
            } else {
               Throwable e = error;
               if (e != null) {
                  queue.clear();
                  a.onError(e);
                  return true;
               } else
               if (empty) {
                  a.onComplete();
                  return true;
               }
            }
         }
         return false;
      }

      @Override
      public int requestFusion(int mode) {
         if ((mode & ASYNC) != 0) {
            outputFused = true;
            return ASYNC;
         }
         return NONE;
      }

      @Nullable
      @Override
      public T poll() throws Exception {
         return queue.poll();
      }

      @Override
      public void clear() {
         queue.clear();
      }

      @Override
      public boolean isEmpty() {
         return queue.isEmpty();
      }
   }

}
