package org.infinispan.reactive.publisher.impl;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;
import java.util.function.Supplier;

import org.infinispan.commons.util.IntSet;
import org.infinispan.reactive.publisher.impl.commands.batch.PublisherResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.internal.queue.SpscArrayQueue;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;

/**
 * Note that this publisher can only be subscribed to by one subscriber
 * @param <R>
 */
public class RemoteSegmentPublisher<K, I, R> extends AtomicLong implements Publisher<R>, Subscription {
   protected final static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   protected final static boolean trace = log.isTraceEnabled();

   private final ClusterPublisherManagerImpl<K, ?>.PublisherSubscription<I, R> parent;
   private final SpscArrayQueue<R> queue;
   private final Supplier<Map.Entry<Address, IntSet>> supplier;
   private final int batchSize;
   private final Map<Address, Set<K>> excludedKeys;
   private final int topologyId;

   private volatile Subscriber<? super R> subscriber;
   private final AtomicInteger requestors = new AtomicInteger();

   private volatile boolean alreadyCreated;

   private AtomicReference<Map.Entry<Address, IntSet>> currentTarget = new AtomicReference<>();

   public RemoteSegmentPublisher(ClusterPublisherManagerImpl<K, ?>.PublisherSubscription<I, R> parent,
         int batchSize, Supplier<Map.Entry<Address, IntSet>> supplier, Map<Address, Set<K>> excludedKeys, int topologyId) {
      this.parent = parent;
      this.queue = new SpscArrayQueue<>(batchSize);
      this.supplier = supplier;
      this.batchSize = batchSize;
      this.excludedKeys = excludedKeys;
      this.topologyId = topologyId;
   }

   public RemoteSegmentPublisher(ClusterPublisherManagerImpl<K, ?>.PublisherSubscription<I, R> parent,
         int batchSize, Supplier<Map.Entry<Address, IntSet>> supplier, Map<Address, Set<K>> excludedKeys, int topologyId,
         Map.Entry<Address, IntSet> specificTarget) {
      this.parent = parent;
      this.queue = new SpscArrayQueue<>(batchSize);
      this.supplier = supplier;
      this.batchSize = batchSize;
      this.excludedKeys = excludedKeys;
      this.topologyId = topologyId;

      this.currentTarget.set(Objects.requireNonNull(specificTarget));
   }

   @Override
   public void subscribe(Subscriber<? super R> s) {
      if (trace) {
         log.tracef("Subscribed to %s via %s", parent.requestId, s);
      }
      this.subscriber = s;
      s.onSubscribe(this);
   }

   @Override
   public void request(long n) {
      if (SubscriptionHelper.validate(n)) {
         BackpressureHelper.add(this, n);
         // Only if there are no other requestors can we proceed
         if (requestors.getAndIncrement() == 0) {
            sendRequest(get());
         }
      }
   }

   @Override
   public void cancel() {
      Map.Entry<Address, IntSet> target = currentTarget.get();
      if (target != null) {
         parent.sendCancelCommand(target.getKey());
      }
   }

   private void sendRequest(long remaining) {
      while (remaining > 0 && !queue.isEmpty()) {
         int produced = 0;
         while (produced < remaining) {
            // Use any of the queued values if present first
            R queuedValue = queue.poll();
            if (queuedValue != null) {
               subscriber.onNext(queuedValue);
               produced++;
            } else {
               break;
            }
         }

         remaining = BackpressureHelper.produced(this, produced);
      }

      // We produced some entries from the overflow queue and had leftovers or exactly enough - try to release
      // control of the pendingRequest boolean
      if (remaining == 0) {
         remaining = continueWithRemaining(0);
         if (remaining == 0) {
            return;
         } else if (queue.isEmpty()) {
            sendRequest(remaining);
         }
      }

      assert remaining > 0;

      Map.Entry<Address, IntSet> target = currentTarget.get();
      if (target == null) {
         alreadyCreated = false;
         target = supplier.get();
         if (target == null) {
            if (trace) {
               log.tracef("Completing subscription %s", this);
            }
            subscriber.onComplete();
            return;
         } else {
            currentTarget.set(target);
         }
      }

      Address address = target.getKey();
      IntSet segments = target.getValue();

      CompletionStage<PublisherResponse> stage;
      if (alreadyCreated) {
         stage = parent.sendNextCommand(address, topologyId);
      } else {
         alreadyCreated = true;
         stage = parent.sendInitialCommand(address, segments, batchSize, excludedKeys.remove(address), topologyId);
      }
      stage.whenComplete((values, t) -> {
         if (t != null) {
            handleThrowableInResponse(t, address, segments);
            return;
         }
         try {
            IntSet completedSegments = values.getCompletedSegments();
            if (completedSegments != null) {
               if (trace) {
                  log.tracef("Completed segments %s for id %s from %s", completedSegments, parent.requestId, address);
               }
               completedSegments.forEach((IntConsumer) parent::completeSegment);
               completedSegments.forEach((IntConsumer) segments::remove);
            }

            IntSet lostSegments = values.getLostSegments();
            if (lostSegments != null) {
               if (trace) {
                  log.tracef("Lost segments %s for id %s from %s", completedSegments, parent.requestId, address);
               }
               lostSegments.forEach((IntConsumer) segments::remove);
            }

            R[] valueArray = (R[]) values.getResults();

            if (trace) {
               // Note the size of the array may not be the amount of entries as it isn't resized (can contain nulls)
               log.tracef("Received %s sized array for id %s from %s", values.getSize(), parent.requestId, address);
            }

            boolean complete = values.isComplete();
            if (complete) {
               // Need to get a new target
               currentTarget.set(null);
            } else {
               int segment = segments.iterator().nextInt();
               values.forEachSegmentValue(parent, segment);
            }

            long requested = get();
            assert requested > 0;

            int produced = 0;

            Object lastValue = null;

            for (R value : valueArray) {
               if (value == null) {
                  // Local execution doesn't trim array down
                  break;
               }
               // Note that consumed is always equal to how many have been sent to onNext - thus
               // once it is equal to the requested we have to enqueue any additional values - so they can be requested
               // later
               if (produced >= requested) {
                  queue.offer(value);
               } else {
                  subscriber.onNext(value);
                  produced++;
               }
               lastValue = value;
            }

            if (completedSegments != null) {
               // Tell the parent of the last enqueued value we have
               parent.notifySegmentsComplete(completedSegments, lastValue);
            }

            trySendRequest(produced);
         } catch (Throwable innerT) {
            handleThrowableInResponse(innerT, address, segments);
         }
      });
   }

   private void handleThrowableInResponse(Throwable t, Address address, IntSet segments) {
      if (parent.handleThrowable(t, address, segments)) {
         // We were told to continue processing - so ignore those segments and try the next target if possible
         // Since we never invoked parent.compleSegment they may get retried
         currentTarget.set(null);
         trySendRequest(0);
      }
   }

   private void trySendRequest(long produced) {
      long innerRemaining = continueWithRemaining(produced);
      if (innerRemaining > 0) {
         sendRequest(innerRemaining);
      }
   }

   // Determines if the method should continue processing due to outstanding requests - That means we either
   // need to read from the queue or if empty submit remotely for more elements
   private long continueWithRemaining(long produced) {
      long remaining = BackpressureHelper.produced(this, produced);
      if (remaining > 0) {
         return remaining;
      }
      // We try to unregister ourself from being a requestor
      if (requestors.decrementAndGet() == 0) {
         return 0;
      }
      // However if there was another we have to continue going
      remaining = get();
      assert remaining > 0;
      return remaining;
   }
}
