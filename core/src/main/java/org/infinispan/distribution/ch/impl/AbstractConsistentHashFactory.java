package org.infinispan.distribution.ch.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public abstract class AbstractConsistentHashFactory<CH extends ConsistentHash> implements ConsistentHashFactory<CH> {
   protected void checkCapacityFactors(List<Address> members, Map<Address, Float> capacityFactors) {
      if (capacityFactors != null) {
         float totalCapacity = 0;
         for (Address node : members) {
            Float capacityFactor = capacityFactors.get(node);
            if (capacityFactor == null || capacityFactor < 0)
               throw new IllegalArgumentException("Invalid capacity factor for node " + node);
            totalCapacity += capacityFactor;
         }
         if (totalCapacity == 0)
            throw new IllegalArgumentException("There must be at least one node with a non-zero capacity factor");
      }
   }

   /**
    * @return The worst primary owner, or {@code null} if the remaining nodes own 0 segments.
    */
   protected Address findWorstPrimaryOwner(Builder builder, List<Address> nodes) {
      Address worst = null;
      float maxSegmentsPerCapacity = -1;
      for (Address owner : nodes) {
         float capacityFactor = builder.getCapacityFactor(owner);
         float ratio = capacityFactor != 0 ? (builder.getPrimaryOwned(owner) - 1) / capacityFactor : 0;
         if (ratio > maxSegmentsPerCapacity || (ratio == maxSegmentsPerCapacity && isLowerUUID(owner, worst))) {
            worst = owner;
            maxSegmentsPerCapacity = ratio;
         }
      }
      return worst;
   }

   /**
    * @return The candidate with the worst primary-owned segments/capacity ratio that is also not in the excludes list.
    */
   protected Address findNewPrimaryOwner(Builder builder, Collection<Address> candidates,
                                         Address primaryOwner) {
      float initialCapacityFactor = primaryOwner != null ? builder.getCapacityFactor(primaryOwner) : 0;

      // We want the owned/capacity ratio of the actual primary owner after removing the current segment to be bigger
      // than the owned/capacity ratio of the new primary owner after adding the current segment, so that a future pass
      // won't try to switch them back.
      Address best = null;
      float bestSegmentsPerCapacity = initialCapacityFactor != 0 ? (builder.getPrimaryOwned(primaryOwner) - 1) /
            initialCapacityFactor : Float.MAX_VALUE;
      for (Address candidate : candidates) {
         int primaryOwned = builder.getPrimaryOwned(candidate);
         float capacityFactor = builder.getCapacityFactor(candidate);
         float ratio = capacityFactor != 0 ? (primaryOwned + 1) / capacityFactor : Float.MAX_VALUE;
         if (ratio < bestSegmentsPerCapacity || (ratio == bestSegmentsPerCapacity && isLowerUUID(candidate, best))) {
            best = candidate;
            bestSegmentsPerCapacity = ratio;
         }
      }
      return best;
   }

   /**
    * Stable UUID-based tiebreaker: returns true if {@code a} should be preferred over {@code b}
    * when their load ratios are equal. Uses MSB first, then LSB, so the result is independent
    * of member list order and therefore independent of join order across caches.
    */
   static boolean isLowerUUID(Address a, Address b) {
      if (b == null) return true;
      int msbCmp = Long.compare(a.getMostSignificantBits(), b.getMostSignificantBits());
      if (msbCmp != 0) return msbCmp < 0;
      return Long.compare(a.getLeastSignificantBits(), b.getLeastSignificantBits()) < 0;
   }

   abstract static class Builder {
      protected final OwnershipStatistics stats;
      protected final List<Address> members;
      protected final Map<Address, Float> capacityFactors;
      // For debugging
      protected int modCount = 0;

      public Builder(OwnershipStatistics stats, List<Address> members, Map<Address, Float> capacityFactors) {
         this.stats = stats;
         this.members = members;
         this.capacityFactors = capacityFactors;
      }

      public Builder(Builder other) {
         this.members = other.members;
         this.capacityFactors = other.capacityFactors;
         this.stats = new OwnershipStatistics(other.stats);
      }

      public List<Address> getMembers() {
         return members;
      }

      public int getNumNodes() {
         return getMembers().size();
      }

      public abstract int getPrimaryOwned(Address candidate);

      public Map<Address, Float> getCapacityFactors() {
         return capacityFactors;
      }

      public float getCapacityFactor(Address node) {
         return capacityFactors != null ? capacityFactors.get(node) : 1;
      }
   }
}
