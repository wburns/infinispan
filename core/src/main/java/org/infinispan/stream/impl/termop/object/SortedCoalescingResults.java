package org.infinispan.stream.impl.termop.object;

import org.infinispan.remoting.transport.Address;
import org.infinispan.stream.impl.ClusterStreamManager;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by wburns on 10/23/15.
 */
public class SortedCoalescingResults<R> implements ClusterStreamManager.ResultsCallback<Iterable<R>>, AutoCloseable {
   private final static Log log = LogFactory.getLog(SortedCoalescingResults.class);

   private final Set<Address> targets;
   private final Comparator<? super R> comparator;

   private int targetsCompleted;

   private final R[] currentTargets;
   private final Map<R, Address> currentAddressForValue = new HashMap<>();
   private final Map<Address, BlockingQueue<R>> sortableValues;

   private final AtomicBoolean closed = new AtomicBoolean(false);

   private Address lastValueFrom;

   public SortedCoalescingResults(Set<Address> targets, int batchSize, Comparator<? super R> comparator) {
      if (targets.size() <= 1) {
         throw new IllegalArgumentException("Requires more than 1 target, use a simple queued callback instead");
      }
      this.targets = new ConcurrentHashSet<>();
      targets.forEach(this.targets::add);
      Objects.nonNull(comparator);
      this.comparator = comparator;
      this.targetsCompleted = 0;
      this.currentTargets = (R[]) new Object[targets.size()];
      this.sortableValues = new HashMap<>();
      if (batchSize <= 0) {
         throw new IllegalArgumentException("Batch size must be 1 or greater!");
      }
      for (Address target : targets) {
         sortableValues.put(target, new ArrayBlockingQueue<R>(batchSize));
      }
   }

   @Override
   public Set<Integer> onIntermediateResult(Address address, Iterable<R> results) {
      BlockingQueue<R> queue = sortableValues.get(address);
      Iterator<R> iterator = results.iterator();
      while (!closed.get() && iterator.hasNext()) {
         R value = iterator.next();
         try {
            while (!queue.offer(value, 100, TimeUnit.MILLISECONDS)) {
               if (closed.get()) {
                  log.tracef("Operation was closed by another thread, ignoring results");
                  break;
               }
            }
         } catch (InterruptedException e) {
            log.tracef("Operation was interrupted, closing sorted results!");
            close();
         }
      }

      return Collections.emptySet();
   }

   @Override
   public void onCompletion(Address address, Set<Integer> completedSegments, Iterable<R> results) {
      onIntermediateResult(address, results);
      targets.remove(address);
   }

   @Override
   public void onSegmentsLost(Set<Integer> segments) {
      // We aren't rehash aware with this
   }

   /**
    *
    * @return
    * @throws InterruptedException
    */
   public R getNextValue() throws InterruptedException {
      // If last value from isn't set this is the first value, so we have to pull an entry from each address
      if (lastValueFrom == null) {
         int offset = 0;
         for (Map.Entry<Address, BlockingQueue<R>> entry : sortableValues.entrySet()) {
            Address address = entry.getKey();
            R value = retrieveFromQueue(address, entry.getValue());
            currentTargets[offset++] = value;
            currentAddressForValue.put(value, entry.getKey());
         }
         Arrays.sort(currentTargets, comparator);
      } else {
         BlockingQueue<R> q = sortableValues.get(lastValueFrom);
         R value = retrieveFromQueue(lastValueFrom, q);
         // If the value is null that means that address no longer has elements for us - so we can mark them completed
         if (value == null) {
            targetsCompleted++;
         } else {
            // TODO: we can optimize for the case when only 1 address is left
            currentTargets[targetsCompleted] = value;
            Arrays.sort(currentTargets, targetsCompleted, currentTargets.length, comparator);
            currentAddressForValue.put(value, lastValueFrom);
         }
      }
      if (targetsCompleted >= currentTargets.length) {
         return null;
      }
      R value = currentTargets[targetsCompleted];
      lastValueFrom = currentAddressForValue.remove(value);
      return value;
   }

   private R retrieveFromQueue(Address address, BlockingQueue<R> queue) throws InterruptedException {
      // If it doesn't contain than we don't need to do waits
      if (!targets.contains(address)) {
         return queue.poll();
      }
      R value;
      while ((value = queue.poll(100, TimeUnit.MILLISECONDS)) == null) {
         if (closed.get()) {
            throw new InterruptedException();
         } else if (!targets.contains(address)) {
            // This can happen if a completed response comes in with no values in the iterable
            break;
         }
      }
      return value;
   }

   @Override
   public void close() {
      closed.set(true);
   }
}
