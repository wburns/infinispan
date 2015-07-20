package org.infinispan.util.concurrent.locks;

import org.infinispan.atomic.DeltaCompositeKey;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;

import java.util.Collection;
import java.util.Objects;

/**
 * Utility methods for locking keys.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class LockUtil {

   private LockUtil() {
   }

   /**
    * It filters the {@code keys} collection by the primary and backup owner.
    * <p>
    * The ownership is compared with this node. The {@code primary} collection will have the keys in which this node is
    * primary and the {@code backup} will have the keys in which this node is the backup.
    *
    * @param keys                     the keys to filter.
    * @param primary                  the collection to store the primary owned keys. It can be {@code null} if the
    *                                 invoker does not need them.
    * @param backup                   the collection to store the backup owned keys. It can be {@code null} if the
    *                                 invoker does not need them.
    * @param clusteringDependentLogic the {@link ClusteringDependentLogic} to check the ownership of the keys.
    * @throws NullPointerException if {@code clusteringDependentLogic} is {@code null}.
    */
   public static void filterByLockOwnership(Collection<?> keys, Collection<Object> primary, Collection<Object> backup,
                                            ClusteringDependentLogic clusteringDependentLogic) {
      Objects.requireNonNull(clusteringDependentLogic, "Cluster Dependent Logic shouldn't be null.");

      if (keys == null || keys.isEmpty()) {
         return;
      }

      for (Object key : keys) {
         Object keyToCheck = key instanceof DeltaCompositeKey ?
               ((DeltaCompositeKey) key).getDeltaAwareValueKey() :
               key;
         switch (getLockOwnership(keyToCheck, clusteringDependentLogic)) {
            case PRIMARY:
               if (primary != null) {
                  primary.add(key);
               }
               break;
            case BACKUP:
               if (backup != null) {
                  backup.add(key);
               }
               break;
            default:
               break; //no-op
         }
      }
   }

   /**
    * It filters the {@code key} by lock ownership.
    *
    * @param key                      the key to check
    * @param clusteringDependentLogic the {@link ClusteringDependentLogic} to check the ownership of the keys.
    * @return the {@link org.infinispan.util.concurrent.locks.LockUtil.LockOwnership}.
    * @throws NullPointerException if {@code clusteringDependentLogic} is {@code null}.
    */
   public static LockOwnership getLockOwnership(Object key, ClusteringDependentLogic clusteringDependentLogic) {
      if (clusteringDependentLogic.localNodeIsPrimaryOwner(key)) {
         return LockOwnership.PRIMARY;
      } else if (clusteringDependentLogic.localNodeIsOwner(key)) {
         return LockOwnership.BACKUP;
      } else {
         return LockOwnership.NO_OWNER;
      }
   }


   public enum LockOwnership {
      /**
       * This node is not an owner.
       */
      NO_OWNER,
      /**
       * This node is the primary lock owner.
       */
      PRIMARY,
      /**
       * this node is the backup lock owner.
       */
      BACKUP
   }

}
