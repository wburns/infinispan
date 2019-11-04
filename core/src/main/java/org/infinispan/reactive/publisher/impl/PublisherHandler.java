package org.infinispan.reactive.publisher.impl;

import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.reactive.publisher.impl.commands.batch.InitialPublisherCommand;
import org.infinispan.reactive.publisher.impl.commands.batch.KeyPublisherResponse;
import org.infinispan.reactive.publisher.impl.commands.batch.PublisherResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.functions.Consumer;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import net.jcip.annotations.GuardedBy;

/**
 * Handler for holding publisher results between requests of data
 * @since 10.1
 */
@Scope(Scopes.NAMED_CACHE)
@Listener(observation = Listener.Observation.POST)
public class PublisherHandler {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final boolean trace = log.isTraceEnabled();

   private final ConcurrentMap<Object, PublisherState> currentRequests = new ConcurrentHashMap<>();

   @Inject CacheManagerNotifier managerNotifier;
   @Inject @ComponentName(KnownComponentNames.ASYNC_OPERATIONS_EXECUTOR)
   ExecutorService cpuExecutor;
   @Inject LocalPublisherManager lpm;

   @ViewChanged
   public void viewChange(ViewChangedEvent event) {
      List<Address> newMembers = event.getNewMembers();
      Iterator<PublisherState> iter = currentRequests.values().iterator();
      while (iter.hasNext()) {
         PublisherState state = iter.next();

         Address owner = state.getOrigin();
         // If an originating node is no longer here then we have to close their publishers - null means local node
         // so that can't be suspected
         if (owner != null && !newMembers.contains(owner)) {
            log.tracef("View changed and no longer contains %s, closing %s publisher", owner, state.requestId);
            state.cancel();
            iter.remove();
         }
      }
   }

   @Start
   public void start() {
      managerNotifier.addListener(this);
   }

   @Stop
   public void stop() {
      // If our cache is stopped we should remove our listener, since this doesn't mean the cache manager is stopped
      managerNotifier.removeListener(this);
   }

   /**
    * Registers a publisher given the initial command arguments. The value returned will eventually contain the
    * first batched response for the publisher of the given id.
    * @param command the command with arguments to start a publisher with
    * @param <I> input type
    * @param <R> output type
    * @return future that will or eventually will contain the first response
    */
   public <I, R> CompletableFuture<PublisherResponse> register(InitialPublisherCommand<?, I, R> command) {
      PublisherState publisherState;
      Object requestId = command.getRequestId();
      if (command.isTrackKeys()) {
         publisherState = new KeyPublisherState(requestId, command.getOrigin(), command.getBatchSize());
      } else {
         publisherState = new PublisherState(requestId, command.getOrigin(), command.getBatchSize());
      }

      PublisherState previousState;
      if ((previousState = currentRequests.put(requestId, publisherState)) != null) {
         if (!previousState.complete) {
            currentRequests.remove(requestId);
            throw new IllegalStateException("There was already a publisher registered for id " + requestId + " that wasn't complete!");
         }
         // We have a previous state that is already completed - this is most likely due to a failover and our node
         // now owns another segment but the async thread hasn't yet cleaned up our state.
         if (trace) {
            log.tracef("Closing prior state for %s to make room for a new request", requestId);
         }
         previousState.cancel();
      }

      publisherState.startProcessing(command);

      return publisherState.results();
   }

   /**
    * Retrieves the next response for the same request id that was configured on the command when invoking
    * {@link #register(InitialPublisherCommand)}.
    * @param requestId the unique request id to continue the response with
    * @return future that will or eventually will contain the next response
    */
   public CompletableFuture<PublisherResponse> getNext(Object requestId) {
      PublisherState publisherState = currentRequests.get(requestId);
      if (publisherState == null) {
         throw new IllegalStateException("Publisher for requestId " + requestId + " doesn't exist!");
      }
      return publisherState.results();
   }

   /**
    * Returns how many publishers are currently open
    * @return how many publishers are currently open
    */
   public int openPublishers() {
      return currentRequests.size();
   }

   /**
    * Closes the publisher that maps to the given request id
    * @param requestId unique identifier for the request
    */
   public void closePublisher(Object requestId) {
      PublisherState state;
      if ((state = currentRequests.remove(requestId)) != null) {
         if (trace) {
            log.tracef("Closed publisher using requestId %s", requestId);
         }
         state.cancel();
      }
   }

   /**
    * Optionally closes the state if this state is still registered for the given requestId
    * @param requestId unique identifier for the given request
    * @param state state to cancel if it is still registered
    */
   private void closePublisher(Object requestId, PublisherState state) {
      if (currentRequests.remove(requestId, state)) {
         if (trace) {
            log.tracef("Closed publisher from completion using requestId %s", requestId);
         }
         state.cancel();
      } else if (trace) {
         log.tracef("A concurrent request already closed the prior state for %s", requestId);
      }
   }

   /**
    * Actual subscriber that listens to the local publisher and stores state and prepares responses as they are ready.
    * This subscriber works by initially requesting {@code batchSize + 1} entries when it is subscribed. The {@code +1}
    * is done purposefully due to how segment completion is guaranteed to be notified just before the next value of
    * a different segment is returned. This way a given batchSize will have a complete view of which segments were
    * completed in it. Subsequent requests will only request {@code batchSize} since our outstanding request count
    * is always 1 more.
    * <p>
    * When a batch size is retrieved or the publisher is complete we create a PublisherResponse that is either
    * passed to the waiting CompletableFuture or registers a new CompletableFuture for a pending request to receive.
    * <p>
    * The state keeps track of all segments that have completed or lost during the publisher response and are returned
    * on the next response. The state keeps track of where the last segment completed {@code segmentStart}, thus
    * our response can tell which which values were not part of the completed segments. It also allows us to drop entries
    * from a segment that was just lost. This is preferable since otherwise the coordinator will have to resend this
    * value or retrieve the value a second time, thus reducing how often they keys need to be replicated.
    * <p>
    * This class relies heavily upon the fact that the reactive stream spec specifies that {@code onNext},
    * {@code onError}, and {@code onComplete} are invoked in a thread safe manner as well as the {@code accept} method
    * on the {@code IntConsumer} when a segment is completed or lost. This allows us to use a simple array with an offset
    * that is used to collect the response.
    */
   private class PublisherState implements Subscriber<Object>, Runnable {
      final Object requestId;
      final Address origin;
      final int batchSize;

      // Stores future responses - Normally this only ever contains zero or one result. This can contain two in the
      // case of having a single entry in the last result. Due to the nature of having to request one additional
      // entry to see segment completion, this is the tradeoff
      @GuardedBy("this")
      private CompletableFuture<PublisherResponse> futureResponse = null;

      Subscription upstream;

      // The remainder of the values hold the values between results received - These do not need synchronization
      // as the Subscriber contract guarantees these are invoked serially and has proper visibility
      Object[] results;
      int pos;
      IntSet completedSegments;
      IntSet lostSegments;
      int segmentStart;
      // Set to true when the last futureResponse has been set - meaning the next response will be the last
      volatile boolean complete;

      private PublisherState(Object requestId, Address origin, int batchSize) {
         this.requestId = requestId;
         this.origin = origin;
         this.batchSize = batchSize;

         results = new Object[batchSize];
      }

      void startProcessing(InitialPublisherCommand command) {
         SegmentAwarePublisher sap;
         if (command.isEntryStream()) {
            sap = lpm.entryPublisher(command.getSegments(), command.getKeys(), command.getExcludedKeys(),
                  command.isIncludeLoader(), command.getDeliveryGuarantee(), command.getTransformer());
         } else {
            sap = lpm.keyPublisher(command.getSegments(), command.getKeys(), command.getExcludedKeys(),
                  command.isIncludeLoader(), command.getDeliveryGuarantee(), command.getTransformer());
         }

         sap.subscribe(this, this::segmentComplete, this::segmentLost);
      }

      @Override
      public void onSubscribe(Subscription s) {
         if (SubscriptionHelper.validate(this.upstream, s)) {
            this.upstream = s;
            // We request 1 extra to guarantee we see the segment complete/lost message
            s.request(batchSize + 1);
         }
      }

      @Override
      public void onError(Throwable t) {
         complete = true;
         synchronized (this) {
            futureResponse = CompletableFutures.completedExceptionFuture(t);
         }
      }

      @Override
      public void onComplete() {
         prepareResponse(true);
      }

      @Override
      public void onNext(Object o) {
         // Means we just finished a batch
         if (pos == results.length) {
            prepareResponse(false);
         }
         results[pos++] = o;
      }

      public void segmentComplete(int segment) {
         if (completedSegments == null) {
            completedSegments = IntSets.mutableEmptySet();
         }
         completedSegments.add(segment);
         segmentStart = pos;
      }

      public void segmentLost(int segment) {
         if (lostSegments == null) {
            lostSegments = IntSets.mutableEmptySet();
         }
         lostSegments.add(segment);
         // Just reset the pos back to the segment start - ignoring those entries
         // This saves us from sending these entries back and then having to resend the key to the new owner
         pos = segmentStart;
      }

      public void cancel() {
         Subscription subscription = upstream;
         if (subscription != null) {
            subscription.cancel();
         }
      }

      void resetValues() {
         this.results = new Object[batchSize];
         this.completedSegments = null;
         this.lostSegments = null;
         this.pos = 0;
         this.segmentStart = 0;
      }

      PublisherResponse generateResponse(boolean complete) {
         return new PublisherResponse(results, completedSegments, lostSegments, pos, complete, segmentStart);
      }

      void prepareResponse(boolean complete) {
         PublisherResponse response = generateResponse(complete);

         if (trace) {
            log.tracef("Response ready %s with id %s for requestor %s", response, requestId, origin);
         }

         if (!complete) {
            // Have to reset the values if we expect to send another response
            resetValues();
         }

         this.complete = complete;

         CompletableFuture<PublisherResponse> futureToComplete = null;
         synchronized (this) {
            if (futureResponse != null) {
               if (futureResponse.isDone()) {
                  // If future was done, that means we prefetched the response - so we may as well merge the results
                  // together (this happens if last entry was by itself - so we will return batchSize + 1)
                  PublisherResponse prevResponse = futureResponse.join();
                  PublisherResponse newResponse = mergeResponses(prevResponse, response);
                  futureResponse = CompletableFuture.completedFuture(newResponse);
                  if (trace) {
                     log.tracef("Received additional response, merged responses together %d for request id %s", System.identityHashCode(futureResponse), requestId);
                  }
               } else {
                  futureToComplete = futureResponse;
                  futureResponse = null;
               }
            } else {
               futureResponse = CompletableFuture.completedFuture(response);
               if (trace) {
                  log.tracef("Eager response completed %d for request id %s", System.identityHashCode(futureResponse), requestId);
               }
            }
         }
         if (futureToComplete != null) {
            if (trace) {
               log.tracef("Completing waiting future %d for request id %s", System.identityHashCode(futureToComplete), requestId);
            }
            // Complete this outside of synchronized block
            futureToComplete.complete(response);
         }
      }

      PublisherResponse mergeResponses(PublisherResponse response1, PublisherResponse response2) {
         IntSet completedSegments = mergeSegments(response1.getCompletedSegments(), response2.getCompletedSegments());
         IntSet lostSegments = mergeSegments(response1.getLostSegments(), response2.getLostSegments());
         int newSize = response1.getSize() + response2.getSize();
         Object[] newArray = new Object[newSize];
         int offset = 0;
         for (Object obj : response1.getResults()) {
            if (obj == null) {
               break;
            }
            newArray[offset++] = obj;
         }
         for (Object obj : response2.getResults()) {
            if (obj == null) {
               break;
            }
            newArray[offset++] = obj;
         }
         // This should always be true
         boolean complete = response2.isComplete();
         return new PublisherResponse(newArray, completedSegments, lostSegments, newSize, complete, newArray.length);
      }

      IntSet mergeSegments(IntSet segments1, IntSet segments2) {
         if (segments1 == null) {
            return segments2;
         } else if (segments2 == null) {
            return segments1;
         }
         segments1.addAll(segments2);
         return segments1;
      }

      public Address getOrigin() {
         return origin;
      }

      /**
       * Retrieves the either already completed result or registers a new future to be completed. This also prestarts
       * the next batch to be ready for the next request as it comes, which is submitted on the {@code cpuExecutor}.
       * @return future that will contain the publisher response with the data
       */
      CompletableFuture<PublisherResponse> results() {
         boolean submitRequest = false;
         CompletableFuture<PublisherResponse> currentFuture;
         synchronized (this) {
            if (futureResponse == null) {
               currentFuture = new CompletableFuture<>();
               currentFuture.thenRunAsync(this, cpuExecutor);
               futureResponse = currentFuture;
            } else {
               currentFuture = futureResponse;
               futureResponse = null;
               submitRequest = true;
            }
         }
         if (submitRequest) {
            // Handles closing publisher or requests next batch if not complete
            // Note this is not done in synchronized block in case if executor is within thread
            cpuExecutor.execute(this);
         }
         if (trace) {
            log.tracef("Retrieved future %d for request id %s", System.identityHashCode(currentFuture), requestId);
         }
         return currentFuture;
      }

      /**
       * This will either request the next batch of values or completes the request. Note the completion has to be done
       * after the last result is returned, thus it cannot be eagerly closed in most cases.
       */
      @Override
      public void run() {
         if (trace) {
            log.tracef("Running handler for request id %s", requestId);
         }
         if (!complete) {
            int requestAmount = batchSize;
            if (trace) {
               log.tracef("Requesting %d additional entries for %s", requestAmount, requestId);
            }
            upstream.request(requestAmount);
         } else {
            synchronized (this) {
               if (futureResponse == null) {
                  closePublisher(requestId, this);
               } else if (trace) {
                  log.tracef("Skipping run as handler is complete, but still has some results for id %s", requestId);
               }
            }
         }
      }
   }

   /**
    * Special PublisherState that listens also to what key generates a given set of values. This state is only used
    * when keys must be tracked (EXACTLY_ONCE guarantee with map or flatMap)
    * <p>
    * The general idea is the publisher will notify when a key or entry is sent down the pipeline and we can view
    * all values that result from that (assumes the transformations are synchronous). Thus we only send a result when
    * we have enough values (>= batchSize) but also get to a new key. This means that we can actually return more
    * values than the batchSize when flatMap returns more than 1 value for a given key.
    */
   class KeyPublisherState extends PublisherState implements Consumer<Object> {
      Object[] extraValues;
      int extraPos;
      Object[] keys;
      int keyPos;

      private KeyPublisherState(Object requestId, Address origin, int batchSize) {
         super(requestId, origin, batchSize);
         // Worst case is keys found is same size as the batch 1:1
         keys = new Object[batchSize];
      }

      void startProcessing(InitialPublisherCommand command) {
         SegmentAwarePublisher sap;
         if (command.isEntryStream()) {
            Function<Publisher<CacheEntry>, Publisher<Object>> function = original ->
                  (Publisher) command.getTransformer().apply(
                        Flowable.fromPublisher(original)
                              .doOnNext(ce -> accept(ce.getKey())));
            sap = lpm.entryPublisher(command.getSegments(), command.getKeys(), command.getExcludedKeys(),
                  command.isIncludeLoader(), DeliveryGuarantee.EXACTLY_ONCE, function);
         } else {
            Function<Publisher<Object>, Publisher<Object>> function = original ->
                  (Publisher) command.getTransformer().apply(
                        Flowable.fromPublisher(original)
                              .doOnNext(this));
            sap = lpm.keyPublisher(command.getSegments(), command.getKeys(), command.getExcludedKeys(),
                  command.isIncludeLoader(), DeliveryGuarantee.EXACTLY_ONCE, function);
         }

         sap.subscribe(this, this::segmentComplete, this::segmentLost);
      }

      @Override
      PublisherResponse generateResponse(boolean complete) {
         return new KeyPublisherResponse(results, completedSegments, lostSegments, pos, complete, extraValues, extraPos,
               keys, keyPos);
      }

      @Override
      public void onNext(Object o) {
         if (pos == results.length) {
            // Write any overflow into our buffer
            if (extraValues == null) {
               extraValues = new Object[8];
            }
            if (extraPos == extraValues.length) {
               Object[] expandedArray = new Object[extraValues.length << 1];
               System.arraycopy(extraValues, 0, expandedArray, 0, extraPos);
               extraValues = expandedArray;
            }
            extraValues[extraPos++] = o;
         } else {
            results[pos++] = o;
         }
      }

      @Override
      void resetValues() {
         super.resetValues();
         keyResetValues();
      }

      void keyResetValues() {
         extraValues = null;
         extraPos = 0;
         keys = null;
         keyPos = 0;
      }

      @Override
      public void segmentComplete(int segment) {
         super.segmentComplete(segment);
         // If a segment is complete - we don't need to send back the keys for it anymore
         keyPos = 0;
         tryPrepareResponse();
      }

      @Override
      public void segmentLost(int segment) {
         super.segmentLost(segment);
         // We discard any extra values - the super method already discarded the ones found
         keyResetValues();
      }

      /**
       * This method is invoked every time a new key is pulled from the original publisher
       * When this occurs we need to send a response if we have filled the batchSize
       * We then register the key to possibly be sent
       * @param key new key to process
       */
      @Override
      public void accept(Object key) {
         tryPrepareResponse();
         if (keys == null) {
            keys = new Object[batchSize];
         }
         keys[keyPos++] = key;
      }

      void tryPrepareResponse() {
         // We hit the batch size already - so send the prior data
         if (pos == results.length) {
            prepareResponse(false);
         }
      }
   }
}
