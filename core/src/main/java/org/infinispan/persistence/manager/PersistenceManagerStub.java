package org.infinispan.persistence.manager;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

import javax.transaction.Transaction;

import org.infinispan.commons.util.IntSet;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.support.BatchModification;
import org.infinispan.util.concurrent.CompletableFutures;
import org.reactivestreams.Publisher;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@SurvivesRestarts
public class PersistenceManagerStub implements PersistenceManager {
   @Override
   public void start() {
   }

   @Override
   public void stop() {
   }

   @Override
   public boolean isEnabled() {
      return false;
   }

   @Override
   public boolean isPreloaded() {
      return false;
   }

   @Override
   public CompletionStage<Void> preload() {
      return CompletableFutures.completedNull();
   }

   @Override
   public void disableStore(String storeType) {
   }

   @Override
   public <T> Set<T> getStores(Class<T> storeClass) {
      return Collections.EMPTY_SET;
   }

   @Override
   public Collection<String> getStoresAsString() {
      return Collections.EMPTY_SET;
   }

   @Override
   public void purgeExpired() {
   }

   @Override
   public CompletionStage<Void> clearAllStores(AccessMode mode) {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Boolean> deleteFromAllStores(Object key, int segment, AccessMode mode) {
      return CompletableFutures.completedFalse();
   }

   @Override
   public <K, V> Publisher<MarshalledEntry<K, V>> publishEntries(Predicate<? super K> filter, boolean fetchValue,
         boolean fetchMetadata, AccessMode mode) {
      return null;
   }

   @Override
   public <K, V> Publisher<MarshalledEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata, AccessMode mode) {
      return null;
   }

   @Override
   public <K> Publisher<K> publishKeys(Predicate<? super K> filter, AccessMode mode) {
      return null;
   }

   @Override
   public <K> Publisher<K> publishKeys(IntSet segments, Predicate<? super K> filter, AccessMode mode) {
      return null;
   }

   @Override
   public CompletionStage<MarshalledEntry> loadFromAllStores(Object key, boolean localInvocation, boolean includeStores) {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<MarshalledEntry> loadFromAllStores(Object key, int segment, boolean localInvocation, boolean includeStores) {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> writeToAllNonTxStores(MarshalledEntry marshalledEntry, int segment, AccessMode modes, long flags) {
      return CompletableFutures.completedNull();
   }

   @Override
   public AdvancedCacheLoader getStateTransferProvider() {
      return null;
   }

   @Override
   public CompletionStage<Integer> size() {
      return CompletableFuture.completedFuture(0);
   }

   @Override
   public CompletionStage<Integer> size(IntSet segments) {
      return CompletableFuture.completedFuture(0);
   }

   @Override
   public void setClearOnStop(boolean clearOnStop) {
   }

   @Override
   public CompletionStage<Void> prepareAllTxStores(Transaction transaction, BatchModification batchModification, AccessMode accessMode) throws PersistenceException {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> commitAllTxStores(Transaction transaction, AccessMode accessMode) {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> rollbackAllTxStores(Transaction transaction, AccessMode accessMode) {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> writeBatchToAllNonTxStores(Iterable<MarshalledEntry> entries, AccessMode accessMode, long flags) {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> deleteBatchFromAllNonTxStores(Iterable<Object> keys, AccessMode accessMode, long flags) {
      return CompletableFutures.completedNull();
   }

   @Override
   public boolean isAvailable() {
      return true;
   }
}
