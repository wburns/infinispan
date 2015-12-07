package org.infinispan.stream.impl.termop.object;

import org.infinispan.commons.CacheException;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stream.impl.ClusterStreamManager;
import org.infinispan.stream.impl.StreamCloseableSupplier;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Created by wburns on 10/23/15.
 */
public class SortedCoalescingResults2<R> implements ClusterStreamManager.ResultsCallback<Iterable<R>>,
        Consumer<Iterable<R>> {
   private final static Log log = LogFactory.getLog(SortedCoalescingResults2.class);

   private final Address localAddress;
   private final Set<Address> targets;
   private final Comparator<? super R> comparator;

   private volatile int targetsCompleted;

   private final R[] currentTargets;
   private final Map<R, Address> currentAddressForValue = new HashMap<>();
   private final Map<Address, Queue<R>> sortableValues;
   private final Queue<R> queueToWriteTo;

   private final AtomicBoolean completionSignal;

   private Address awaitingMoreFrom;

   public SortedCoalescingResults2(Address localAddress, Collection<Address> targets, Comparator<? super R> comparator,
                                   Queue<R> queueToWriteTo, AtomicBoolean completionSignal) {
      this.localAddress = localAddress;
      if (targets.size() <= 1) {
         throw new IllegalArgumentException("Requires more than 1 target, use a simple queued callback instead");
      }
      this.targets = new ConcurrentHashSet<>();
      Objects.nonNull(comparator);
      this.comparator = comparator;
      this.targetsCompleted = 0;
      this.currentTargets = (R[]) new Object[targets.size()];
      this.sortableValues = new HashMap<>();
      this.queueToWriteTo = queueToWriteTo;
      this.completionSignal = completionSignal;
   }

   @Override
   public Set<Integer> onIntermediateResult(Address address, Iterable<R> results) {
      log.tracef("Received intermediate result from %s", address);
      if (completionSignal.get()) {
         // We were completed elsewhere, don't compute values
         return Collections.emptySet();
      }
      Queue<R> queue = sortableValues.get(address);
      // If the queue is null it means we have the first response from this address
      if (queue == null) {
         // We use a concurrent queue so we can safely poll from the head and write to it if need be
         queue = new ConcurrentLinkedQueue<>();
         boolean shouldMoveQueue;
         // Note we synchronize because we don't want 2 threads moving queue contents at the same time
         synchronized (this) {
            sortableValues.put(address, queue);
            // If we have a response from all nodes, do the first queue moving
            shouldMoveQueue = sortableValues.size() == targets.size();
         }
         if (shouldMoveQueue) {
            awaitingMoreFrom = moveToQueue();
         }
      } else {
         results.forEach(queue::add);
         if (address.equals(awaitingMoreFrom)) {
            awaitingMoreFrom = moveToQueue();
         }
      }

      if (awaitingMoreFrom == null) {
         completionSignal.set(true);
      }

      return Collections.emptySet();
   }

   /**
    * Sorts through all of the target queues until it exhausts one.  All of the entries up to that point are sorted
    * and pass the value to the underlying queue.
    * @return The address whose queue is ehxausted first is returned to tell the
    * caller which address they must wait for a response from.  If null is returned that means all values have been
    * added to the underlying queue.
    */
   private Address moveToQueue() {
      Address nextAddressToUse = awaitingMoreFrom;
      while (true) {
         // If last value from isn't set this is the first value, so we have to pull an entry from each address
         if (nextAddressToUse == null) {
            int offset = 0;
            Iterator<Map.Entry<Address, Queue<R>>> iter = sortableValues.entrySet().iterator();
            while (iter.hasNext()) {
               Map.Entry<Address, Queue<R>> entry = iter.next();
               Address address = entry.getKey();
               R value = entry.getValue().poll();
               if (value == null) {
                  // This means this node returned nothing for us, so remove it's queue and that it is a target
                  iter.remove();
                  targets.remove(address);
               } else {
                  currentTargets[offset++] = value;
                  currentAddressForValue.put(value, entry.getKey());
               }
            }
            // This means no values were found!
            if (offset > 0) {
               return null;
            }
            Arrays.sort(currentTargets, 0, offset, comparator);
         } else {
            Queue<R> q = sortableValues.get(nextAddressToUse);
            R value = q.poll();
            // If the value is null that means that address no longer has elements for us - so we can mark them completed
            if (value == null) {
               // If this is still present it means we haven't found a completion from them yet.
               if (targets.contains(nextAddressToUse)) {
                  return nextAddressToUse;
               } else {
                  // If we have completed all of the addresses we are done!
                  if (++targetsCompleted == currentTargets.length) {
                     return null;
                  }
               }
            } else {
               // TODO: we can optimize for the case when only 1 address is left
               currentTargets[targetsCompleted] = value;
               Arrays.sort(currentTargets, targetsCompleted, currentTargets.length, comparator);
               currentAddressForValue.put(value, nextAddressToUse);
            }
         }
         R value = currentTargets[targetsCompleted];
         queueToWriteTo.add(value);
         nextAddressToUse = currentAddressForValue.remove(value);
      }
   }

   @Override
   public void onCompletion(Address address, Set<Integer> completedSegments, Iterable<R> results) {
      log.tracef("Received final result from %s", address);
      targets.remove(address);
      onIntermediateResult(address, results);
   }

   @Override
   public void onSegmentsLost(Set<Integer> segments) {
      // We aren't rehash aware with this
   }

   @Override
   public void accept(Iterable<R> rs) {
      onIntermediateResult(localAddress, rs);
   }
}
