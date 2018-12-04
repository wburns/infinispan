package org.infinispan.stream;

import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.CacheCollection;
import org.infinispan.CacheStream;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.reactive.publisher.impl.ClusterPublisherManager;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.function.SerializableFunction;
import org.infinispan.util.rxjava.RxJavaInterop;
import org.reactivestreams.Publisher;
import org.testng.annotations.Test;

import io.reactivex.Flowable;

/**
 * Verifies stream tests work on a regular distrbuted stream
 */
@Test(groups = "functional", testName = "streams.DistributedStreamTest")
public class DistributedStreamTest extends BaseStreamTest {

   public DistributedStreamTest() {
      super(false);
      cacheMode(CacheMode.DIST_SYNC);
   }

   @Override
   protected <E> CacheStream<E> createStream(CacheCollection<E> entries) {
      // This forces parallel distribution since iterator defaults to sequential
      return entries.stream().parallelDistribution();
   }

   public void testCount() throws InterruptedException, ExecutionException, TimeoutException {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());

      ClusterPublisherManager<Integer, String> cpm = TestingUtil.extractComponent(cache, ClusterPublisherManager.class);

      SerializableFunction<Publisher<?>, CompletionStage<Long>> transformer = cp ->
            Flowable.fromPublisher(cp)
                  .count()
                  .to(RxJavaInterop.singleToCompletionStage());
      SerializableFunction<Publisher<Long>, CompletionStage<Long>> finalizer = results ->
            Flowable.fromPublisher(results)
                  .reduce((long) 0, Long::sum)
                  .to(RxJavaInterop.singleToCompletionStage());

      CompletionStage<Long> count = cpm.keyComposition(true, null, null, null, true, DeliveryGuarantee.EXACTLY_ONCE,
            transformer, finalizer);

      assertEquals(range, count.toCompletableFuture().get(10, TimeUnit.SECONDS).intValue());
   }
}
