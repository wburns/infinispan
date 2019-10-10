package org.infinispan.stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.reactive.publisher.impl.ClusterPublisherManager;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.reactive.publisher.impl.LocalPublisherManager;
import org.infinispan.reactive.publisher.impl.PublisherHandler;
import org.infinispan.reactive.publisher.impl.SegmentCompletionPublisher;
import org.infinispan.reactive.publisher.impl.commands.batch.InitialPublisherCommand;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.Mocks;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.mockito.AdditionalAnswers;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import io.reactivex.Flowable;
import io.reactivex.functions.Consumer;

/**
 * Test to verify distributed stream iterator
 *
 * @author wburns
 * @since 8.0
 */
@Test(groups = {"functional", "smoke"}, testName = "iteration.DistributedStreamIteratorTest")
public class DistributedStreamIteratorTest extends BaseClusteredStreamIteratorTest {
   public DistributedStreamIteratorTest() {
      this(false, CacheMode.DIST_SYNC);
   }

   public DistributedStreamIteratorTest(boolean tx, CacheMode cacheMode) {
      super(tx, cacheMode);
      // This is needed since we kill nodes
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected Object getKeyTiedToCache(Cache<?, ?> cache) {
      return new MagicKey(cache);
   }

   @Test
   public void verifyNodeLeavesBeforeGettingData() throws TimeoutException, InterruptedException, ExecutionException {
      Map<Object, String> values = putValueInEachCache(3);

      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);

      CheckPoint checkPoint = new CheckPoint();
      checkPoint.triggerForever(Mocks.AFTER_RELEASE);
      waitUntilSendingResponse(cache1, checkPoint, true);

      final BlockingQueue<String> returnQueue = new ArrayBlockingQueue<>(10);
      Future<Void> future = fork(() -> {
         Iterator<String> iter = cache0.values().stream().iterator();
         while (iter.hasNext()) {
            String entry = iter.next();
            returnQueue.add(entry);
         }
         return null;
      });

      // Make sure the thread is waiting for the response
      checkPoint.awaitStrict(Mocks.BEFORE_INVOCATION, 10, TimeUnit.SECONDS);

      // Now kill the cache - we should recover
      killMember(1, CACHE_NAME);

      checkPoint.trigger(Mocks.BEFORE_RELEASE);

      future.get(10, TimeUnit.SECONDS);

      for (Map.Entry<Object, String> entry : values.entrySet()) {
         assertTrue("Entry wasn't found:" + entry, returnQueue.contains(entry.getValue()));
      }
   }

   /**
    * This test is to verify proper behavior when a node dies after sending a batch to the requestor
    */
   @Test
   public void verifyNodeLeavesAfterSendingBackSomeData() throws TimeoutException, InterruptedException, ExecutionException {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);

      Map<Object, String> values = new HashMap<>();
      int chunkSize = cache0.getCacheConfiguration().clustering().stateTransfer().chunkSize();
      // Now insert 10 more values than the chunk size into the node we will kill
      for (int i = 0; i < chunkSize + 10; ++i) {
         MagicKey key = new MagicKey(cache1);
         cache1.put(key, key.toString());
         values.put(key, key.toString());
      }

      CheckPoint checkPoint = new CheckPoint();
      // Let the first request come through fine
      checkPoint.trigger(Mocks.BEFORE_RELEASE);
      waitUntilSendingResponse(cache1, checkPoint, false);

      final BlockingQueue<Map.Entry<Object, String>> returnQueue = new LinkedBlockingQueue<>();
      Future<Void> future = fork(() -> {
         Iterator<Map.Entry<Object, String>> iter = cache0.entrySet().stream().iterator();
         while (iter.hasNext()) {
            Map.Entry<Object, String> entry = iter.next();
            returnQueue.add(entry);
         }
         return null;
      });

      // Now wait for them to send back first results
      checkPoint.awaitStrict(Mocks.AFTER_INVOCATION, 10, TimeUnit.SECONDS);
      checkPoint.trigger(Mocks.AFTER_RELEASE);

      // We should get a value now, note all values are currently residing on cache1 as primary
      Map.Entry<Object, String> value = returnQueue.poll(10, TimeUnit.SECONDS);

      // Now kill the cache - we should recover
      killMember(1, CACHE_NAME);

      future.get(10, TimeUnit.SECONDS);

      for (Map.Entry<Object, String> entry : values.entrySet()) {
         assertTrue("Entry wasn't found:" + entry, returnQueue.contains(entry) || entry.equals(value));
      }
   }

   @Test
   public void waitUntilProcessingResults() throws TimeoutException, InterruptedException, ExecutionException {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);

      Map<Object, String> values = new HashMap<>();
      for (int i = 0; i < 501; ++i) {
         MagicKey key = new MagicKey(cache1);
         cache1.put(key, key.toString());
         values.put(key, key.toString());
      }

      CheckPoint checkPoint = new CheckPoint();
      checkPoint.triggerForever(Mocks.AFTER_RELEASE);

      ClusterPublisherManager<Object, String> spy = replaceComponentWithSpy(cache0, ClusterPublisherManager.class);

      doAnswer(invocation -> {
         SegmentCompletionPublisher result = (SegmentCompletionPublisher) invocation.callRealMethod();
         return Mocks.blockingPublisher(result, checkPoint);
      }).when(spy).entryPublisher(any(), any(), any(), anyBoolean(), any(), anyInt(), any());

      final BlockingQueue<Map.Entry<Object, String>> returnQueue = new LinkedBlockingQueue<>();
      Future<Void> future = fork(() -> {
            Iterator<Map.Entry<Object, String>> iter = cache0.entrySet().stream().iterator();
            while (iter.hasNext()) {
               Map.Entry<Object, String> entry = iter.next();
               returnQueue.add(entry);
            }
            return null;
      });

      // Now wait for them to send back first results but don't let them process
      checkPoint.awaitStrict(Mocks.BEFORE_INVOCATION, 10, TimeUnit.SECONDS);

      // Now let them process the results
      checkPoint.triggerForever(Mocks.BEFORE_RELEASE);

      // Now kill the cache - we should recover and get appropriate values
      killMember(1, CACHE_NAME);

      future.get(10, TimeUnit.SECONDS);


      KeyPartitioner keyPartitioner = TestingUtil.extractComponent(cache0, KeyPartitioner.class);
      Map<Integer, Set<Map.Entry<Object, String>>> expected = generateEntriesPerSegment(keyPartitioner, values.entrySet());
      Map<Integer, Set<Map.Entry<Object, String>>> answer = generateEntriesPerSegment(keyPartitioner, returnQueue);

      for (Map.Entry<Integer, Set<Map.Entry<Object, String>>> entry : expected.entrySet()) {
         Integer segment = entry.getKey();
         Set<Map.Entry<Object, String>> answerForSegment = answer.get(segment);
         if (answerForSegment != null) {
            for (Map.Entry<Object, String> exp : entry.getValue()) {
               if (!answerForSegment.contains(exp)) {
                  log.errorf("Segment %d, missing %s", segment, exp);
               }
            }
            for (Map.Entry<Object, String> ans : answerForSegment) {
               if (!entry.getValue().contains(ans)) {
                  log.errorf("Segment %d, extra %s", segment, ans);
               }
            }
         }
         assertEquals(entry.getValue().size(), answerForSegment.size());
         assertEquals("Segment " + segment + " had a mismatch", entry.getValue(), answerForSegment);
      }
   }

   @Test
   public void testNodeLeavesWhileIteratingOverContainerCausingRehashToLoseValues() throws TimeoutException,
                                                                                           InterruptedException,
                                                                                           ExecutionException {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      // We put some entries into cache1, which will be shut down below. The batch size is only 2 so we won't be able
      // to get them all in 1 remote call - this way we can block until we know we touch the data container, so at least
      // the second request will give us an issue
      Map<Object, String> values = new HashMap<>();
      values.put(new MagicKey(cache0), "ignore");
      values.put(new MagicKey(cache0), "ignore");
      values.put(new MagicKey(cache0), "ignore");
      values.put(new MagicKey(cache1), "ignore");
      cache1.putAll(values);


      CheckPoint checkPoint = new CheckPoint();
      checkPoint.triggerForever("post_iterator_released");
      waitUntilDataContainerWillBeIteratedOn(cache0, checkPoint);

      final BlockingQueue<Map.Entry<Object, String>> returnQueue = new LinkedBlockingQueue<>();
      Future<Void> future = fork(() -> {
         // Put batch size to a lower number just to make sure it doesn't retrieve them all in 1 go
         Iterator<Map.Entry<Object, String>> iter = cache2.entrySet().stream().distributedBatchSize(2).iterator();
         while (iter.hasNext()) {
            Map.Entry<Object, String> entry = iter.next();
            returnQueue.add(entry);
         }
         return null;
      });

      // Now wait for them to send back first results but don't let them process
      checkPoint.awaitStrict("pre_iterator_invoked", 10, TimeUnit.SECONDS);

      // Now kill the cache - we should recover and get appropriate values
      killMember(0, CACHE_NAME, false);

      // Now let them process the results
      checkPoint.triggerForever("pre_iterator_released");

      future.get(10, TimeUnit.SECONDS);


      KeyPartitioner keyPartitioner = TestingUtil.extractComponent(cache1, KeyPartitioner.class);
      Map<Integer, Set<Map.Entry<Object, String>>> expected = generateEntriesPerSegment(keyPartitioner, values.entrySet());
      Map<Integer, Set<Map.Entry<Object, String>>> answer = generateEntriesPerSegment(keyPartitioner, returnQueue);

      for (Map.Entry<Integer, Set<Map.Entry<Object, String>>> entry : expected.entrySet()) {
         try {
            assertEquals("Segment " + entry.getKey() + " had a mismatch", entry.getValue(), answer.get(entry.getKey()));
         } catch (AssertionError e) {
            log.fatal("TEST ENDED");
            throw e;
         }
      }
   }

   @Test
   public void testLocallyForcedStream() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      Map<Object, String> values = new HashMap<>();
      for (int i = 0; i < 501; ++i) {
         switch (i % 3) {
            case 0:
               MagicKey key = new MagicKey(cache0);
               cache0.put(key, key.toString());
               values.put(key, key.toString());
               break;
            case 1:
               // Force it so only cache0 has it's primary owned keys
               key = magicKey(cache1, cache2);
               // write from backup so that the test works on scattered cache, too
               cache2.put(key, key.toString());
               break;
            case 2:
               // Force it so only cache0 has it's primary owned keys
               key = magicKey(cache2, cache1);
               // write from backup so that the test works on scattered cache, too
               cache1.put(key, key.toString());
               break;
            default:
               fail("Unexpected switch case!");
         }
      }

      int count = 0;
      Iterator<Map.Entry<Object, String>> iter = cache0.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).entrySet().
              stream().iterator();
      while (iter.hasNext()) {
         Map.Entry<Object, String> entry = iter.next();
         String cacheValue = cache0.get(entry.getKey());
         assertNotNull(cacheValue);
         assertEquals(cacheValue, entry.getValue());
         count++;
      }

      assertEquals(values.size(), count);
   }

   public void testIteratorClosedProperlyOnClose() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      // We have to insert over the buffer size default - which iterator uses
      for (int i = 0; i < Flowable.bufferSize() + 2; ++i) {
         // We insert 2 values into caches where we aren't the owner (they have to be in same node or else iterator
         // will finish early)
         cache0.put(magicKey(cache1, cache2), "not-local");
      }

      PublisherHandler handler = TestingUtil.extractComponent(cache1, PublisherHandler.class);
      assertEquals(0, handler.openPublishers());

      try (CacheStream<Map.Entry<Object, String>> stream = cache0.entrySet().stream()) {
         Iterator<Map.Entry<Object, String>> iter = stream.distributedBatchSize(1).iterator();
         assertTrue(iter.hasNext());
         assertEquals(1, handler.openPublishers());
      }

      // The close is done asynchronously
      eventuallyEquals(0, handler::openPublishers);
   }

   public void testIteratorClosedWhenIteratedFully() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      // We have to insert over the buffer size default - which iterator uses
      for (int i = 0; i < Flowable.bufferSize() + 2; ++i) {
         // We insert 2 values into caches where we aren't the owner (they have to be in same node or else iterator
         // will finish early)
         cache0.put(magicKey(cache1, cache2), "not-local");
      }

      PublisherHandler handler = TestingUtil.extractComponent(cache1, PublisherHandler.class);
      assertEquals(0, handler.openPublishers());

      Iterator<Map.Entry<Object, String>> iter = cache0.entrySet().stream().distributedBatchSize(1).iterator();
      assertTrue(iter.hasNext());
      assertEquals(1, handler.openPublishers());

      iter.forEachRemaining(ignore -> {});

      // The close is done asynchronously
      eventuallyEquals(0, handler::openPublishers);
   }

   protected MagicKey magicKey(Cache<Object, String> cache1, Cache<Object, String> cache2) {
      if (cache1.getCacheConfiguration().clustering().hash().numOwners() < 2) {
         return new MagicKey(cache1);
      } else {
         return new MagicKey(cache1, cache2);
      }
   }

   @Test
   public void testStayLocalIfAllSegmentsPresentLocallyWithReHash() throws Exception {
      testStayLocalIfAllSegmentsPresentLocally(true);
   }

   @Test
   public void testStayLocalIfAllSegmentsPresentLocallyWithoutRehash() throws Exception {
      testStayLocalIfAllSegmentsPresentLocally(false);
   }

   private void testStayLocalIfAllSegmentsPresentLocally(boolean rehashAware) throws Exception {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);

      RpcManager rpcManager = replaceComponentWithSpy(cache0, RpcManager.class);

      IntStream.rangeClosed(0, 499).boxed().forEach(i -> cache0.put(i, i.toString()));

      KeyPartitioner keyPartitioner = TestingUtil.extractComponent(cache0, KeyPartitioner.class);
      ConsistentHash ch = cache0.getAdvancedCache().getDistributionManager().getWriteConsistentHash();
      IntSet segmentsCache0 = IntSets.from(ch.getSegmentsForOwner(address(0)));

      CacheStream<Map.Entry<Object, String>> stream = cache0.entrySet().stream();
      if (!rehashAware) stream = stream.disableRehashAware();

      Map<Object, String> entries = mapFromIterator(stream.filterKeySegments(segmentsCache0).iterator());

      Map<Integer, Set<Map.Entry<Object, String>>> entriesPerSegment = generateEntriesPerSegment(keyPartitioner, entries.entrySet());

      // We should not see keys from other segments, but there may be segments without any keys
      assertTrue(segmentsCache0.containsAll(entriesPerSegment.keySet()));
      verify(rpcManager, never()).invokeCommand(any(Address.class), any(InitialPublisherCommand.class), any(), any());
   }

   protected void waitUntilSendingResponse(final Cache<?, ?> cache, final CheckPoint checkPoint, boolean hasMap) {
      Mocks.blockingMock(checkPoint, LocalPublisherManager.class, cache,
            (stub, m) -> {
               if (hasMap) {
                  stub.when(m).entryPublisher(any(), any(), any(), anyBoolean(), any(Consumer.class), any(Function.class));
               } else {
                  stub.when(m).entryPublisher(any(), any(), any(), anyBoolean(), any(DeliveryGuarantee.class), any(Function.class));
               }
            }
      );
   }

   protected void waitUntilDataContainerWillBeIteratedOn(final Cache<?, ?> cache, final CheckPoint checkPoint) {
      InternalDataContainer dataContainer = TestingUtil.extractComponent(cache, InternalDataContainer.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(dataContainer);
      InternalDataContainer mockContainer = mock(InternalDataContainer.class, withSettings().defaultAnswer(forwardedAnswer));
      final AtomicInteger invocationCount = new AtomicInteger();
      Answer blockingAnswer = invocation -> {
         boolean waiting = false;
         if (invocationCount.getAndIncrement() == 0) {
            waiting = true;
            // Wait for main thread to sync up
            checkPoint.trigger("pre_iterator_invoked");
            // Now wait until main thread lets us through
            checkPoint.awaitStrict("pre_iterator_released", 10, TimeUnit.SECONDS);
         }

         try {
            return forwardedAnswer.answer(invocation);
         } finally {
            invocationCount.getAndDecrement();
            if (waiting) {
               // Wait for main thread to sync up
               checkPoint.trigger("post_iterator_invoked");
               // Now wait until main thread lets us through
               checkPoint.awaitStrict("post_iterator_released", 10, TimeUnit.SECONDS);
            }
         }
      };
      doAnswer(blockingAnswer).when(mockContainer).publisher(anyInt());
      TestingUtil.replaceComponent(cache, InternalDataContainer.class, mockContainer, true);
   }
}
