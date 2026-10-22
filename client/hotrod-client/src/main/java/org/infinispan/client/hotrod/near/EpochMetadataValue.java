package org.infinispan.client.hotrod.near;

import java.util.Objects;

import org.infinispan.client.hotrod.MetadataValue;

final class EpochMetadataValue<V> implements MetadataValue<V> {
   private final MetadataValue<V> delegate;
   private final int epoch;
   private final int segment;

   EpochMetadataValue(MetadataValue<V> delegate, int epoch, int segment) {
      this.delegate = Objects.requireNonNull(delegate, "delegate");
      this.epoch = epoch;
      this.segment = segment;
   }

   MetadataValue<V> getDelegate() {
      return delegate;
   }

   int getEpoch() {
      return epoch;
   }

   int getSegment() {
      return segment;
   }

   @Override
   public long getCreated() {
      return delegate.getCreated();
   }

   @Override
   public int getLifespan() {
      return delegate.getLifespan();
   }

   @Override
   public long getLastUsed() {
      return delegate.getLastUsed();
   }

   @Override
   public int getMaxIdle() {
      return delegate.getMaxIdle();
   }

   @Override
   public long getVersion() {
      return delegate.getVersion();
   }

   @Override
   public V getValue() {
      return delegate.getValue();
   }

   @Override
   public String toString() {
      return "EpochMetadataValue[epoch=" + epoch + ", delegate=" + delegate + "]";
   }
}
