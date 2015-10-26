package org.infinispan.stream.impl.termop.object;

import org.infinispan.commons.CacheException;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stream.impl.ClusterStreamManager;
import org.infinispan.stream.impl.StreamCloseableSupplier;
import org.infinispan.util.CloseableSupplier;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Created by wburns on 10/23/15.
 */
public class SortedCoalescingResults<R> implements ClusterStreamManager.ResultsCallback<Iterable<R>>,
        StreamCloseableSupplier<R>, Consumer<Iterable<R>> {
   private final static Log log = LogFactory.getLog(SortedCoalescingResults.class);

   private final Address localAddress;
   private final Set<Address> targets;
   private final Comparator<? super R> comparator;

   private int targetsCompleted;

   private final R[] currentTargets;
   private final Map<R, Address> currentAddressForValue = new HashMap<>();
   private final Map<Address, BlockingQueue<R>> sortableValues;

   private final AtomicBoolean closed = new AtomicBoolean(false);
   private volatile CacheException exception;

   private Address lastValueFrom;

   private Consumer<? super R> consumer;
   private UUID identifier;

   public SortedCoalescingResults(Address localAddress, Collection<Address> targets, int batchSize,
           Comparator<? super R> comparator) {
      this.localAddress = localAddress;
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
      log.tracef("Received intermediate result from %s", address);
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
      log.tracef("Received final result from %s", address);
      onIntermediateResult(address, results);
      targets.remove(address);
   }

   @Override
   public void onSegmentsLost(Set<Integer> segments) {
      // We aren't rehash aware with this
   }

   public R get() {
      // If last value from isn't set this is the first value, so we have to pull an entry from each address
      if (lastValueFrom == null) {
         int offset = 0;
         for (Map.Entry<Address, BlockingQueue<R>> entry : sortableValues.entrySet()) {
            Address address = entry.getKey();
            R value = retrieveFromQueue(address, entry.getValue());
            if (value == null) {
               targetsCompleted++;
               // we have to shift all of the values over 1
               for (int i = offset; i > 0; --i) {
                  currentTargets[i] = currentTargets[i - 1];
               }
               offset++;
               continue;
            }
            currentTargets[offset++] = value;
            currentAddressForValue.put(value, entry.getKey());
         }
         try {
            Arrays.sort(currentTargets, comparator);
         } catch (NullPointerException e) {
            System.currentTimeMillis();
         }
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

   private R retrieveFromQueue(Address address, BlockingQueue<R> queue) {
      // If it doesn't contain than we don't need to do waits
      if (!targets.contains(address)) {
         return queue.poll();
      }
      R value;

      try {
         while ((value = queue.poll(100, TimeUnit.MILLISECONDS)) == null) {
            if (!targets.contains(address) || isClosed()) {
               // This can happen if a completed response comes in with no values in the iterable
               break;
            }
         }
      } catch (InterruptedException e) {
         throw new CacheException(e);
      }
      return value;
   }

   private boolean isClosed() {
      if (closed.get()) {
         if (exception != null) {
            throw exception;
         }
         return true;
      }
      return false;
   }

   @Override
   public void close() {
      closed.set(true);
   }

   public void close(CacheException e) {
      exception = e;
      closed.set(true);
   }

   @Override
   public void addConsumerOnSupply(Consumer<? super R> consumer) {
      this.consumer = consumer;
   }

   @Override
   public void setIdentifier(UUID identifier) {
      this.identifier = identifier;
   }

   @Override
   public void accept(Iterable<R> rs) {
      onIntermediateResult(localAddress, rs);
   }
}
