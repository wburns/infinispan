package org.infinispan.conflict.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.eviction.impl.EvictionManagerImpl;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

@Scope(Scopes.NAMED_CACHE)
public class SegmentHashTracker {

   @Inject Configuration configuration;
   @Inject @ComponentName(KnownComponentNames.INTERNAL_MARSHALLER) Marshaller marshaller;
   @Inject EvictionManager<?, ?> evictionManager;

   private AtomicLongArray[] bucketHashes;
   private AtomicIntegerArray[] bucketCounts;
   private int bucketCount;
   private boolean enabled;
   private boolean hasStores;

   @Start
   public void start() {
      hasStores = configuration.persistence().usingStores();
      enabled = configuration.clustering().cacheMode().isClustered()
            && configuration.clustering().partitionHandling().resolveConflictsOnMerge()
            && configuration.persistence().stores().stream().noneMatch(StoreConfiguration::shared);

      if (!enabled) return;

      if (evictionManager instanceof EvictionManagerImpl<?, ?> emi) {
         emi.setSegmentHashTracker(this);
      }

      int numSegments = configuration.clustering().hash().numSegments();
      bucketCount = configuration.clustering().partitionHandling().hashBuckets();

      bucketHashes = new AtomicLongArray[numSegments];
      bucketCounts = new AtomicIntegerArray[numSegments];
      for (int i = 0; i < numSegments; i++) {
         bucketHashes[i] = new AtomicLongArray(bucketCount);
         bucketCounts[i] = new AtomicIntegerArray(bucketCount);
      }
   }

   public boolean isEnabled() {
      return enabled;
   }

   public boolean hasStores() {
      return hasStores;
   }

   public int bucketCount() {
      return bucketCount;
   }

   public SegmentHasher.HashAndBucket computeHashAndBucket(Object key, Object value) {
      return SegmentHasher.computeHashAndBucket(key, value, bucketCount, marshaller);
   }

   public SegmentHasher.HashAndBucket computeHashAndBucket(Object key, Object value, int bucketForKey) {
      long hash = SegmentHasher.computeEntryHash(key, value, marshaller);
      return new SegmentHasher.HashAndBucket(hash, bucketForKey);
   }

   public int computeBucketIndex(Object key) {
      return SegmentHasher.computeBucket(key, bucketCount, marshaller);
   }

   public void recordInsert(int segment, int bucket, long hash) {
      bucketHashes[segment].getAndAccumulate(bucket, hash, (prev, h) -> prev ^ h);
      bucketCounts[segment].getAndIncrement(bucket);
   }

   public void recordRemove(int segment, int bucket, long hash) {
      bucketHashes[segment].getAndAccumulate(bucket, hash, (prev, h) -> prev ^ h);
      bucketCounts[segment].getAndDecrement(bucket);
   }

   public void recordUpdate(int segment, int bucket, long oldHash, long newHash) {
      long delta = oldHash ^ newHash;
      bucketHashes[segment].getAndAccumulate(bucket, delta, (prev, d) -> prev ^ d);
   }

   public void resetSegment(int segment) {
      for (int b = 0; b < bucketCount; b++) {
         bucketHashes[segment].set(b, 0);
         bucketCounts[segment].set(b, 0);
      }
   }

   public void resetAllSegments() {
      for (int s = 0; s < bucketHashes.length; s++) {
         resetSegment(s);
      }
   }

   public List<BucketHash> getBucketHashes(int segment) {
      List<BucketHash> result = new ArrayList<>(bucketCount);
      for (int b = 0; b < bucketCount; b++) {
         result.add(new BucketHash(segment, b, bucketHashes[segment].get(b), bucketCounts[segment].get(b)));
      }
      return result;
   }

   public List<BucketHash> getAllBucketHashes(IntSet segments) {
      List<BucketHash> result = new ArrayList<>(segments.size() * bucketCount);
      segments.forEach((int seg) -> result.addAll(getBucketHashes(seg)));
      return result;
   }

   void initForTest(Marshaller marshaller, int numSegments, int bucketCount) {
      this.marshaller = marshaller;
      this.bucketCount = bucketCount;
      this.enabled = true;
      this.hasStores = false;
      this.bucketHashes = new AtomicLongArray[numSegments];
      this.bucketCounts = new AtomicIntegerArray[numSegments];
      for (int i = 0; i < numSegments; i++) {
         bucketHashes[i] = new AtomicLongArray(bucketCount);
         bucketCounts[i] = new AtomicIntegerArray(bucketCount);
      }
   }
}
