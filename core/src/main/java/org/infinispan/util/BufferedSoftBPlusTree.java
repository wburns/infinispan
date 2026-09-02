package org.infinispan.util;

import java.io.IOException;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

/**
 * A {@link SoftBPlusTree} that buffers the writes produced by mutations instead of persisting them
 * eagerly, coalescing repeated mutations of the same nodes into a single write. Buffered state is
 * persisted by {@link #flush()}, which is also invoked automatically once {@code flushMutationCount}
 * mutations have accumulated and unconditionally by {@link #saveTree()}.
 * <p>
 * Two kinds of buffered change are tracked:
 * <ul>
 *    <li><b>Value overwrites</b> (same key, new value) keep the leaf's serialized size unchanged, so they
 *    are recorded and later re-written in place exactly once. The whole {@link PinnableSoftNode} path down
 *    to the mutated leaf is pinned with a strong reference until flush: pinning only the leaf would let GC
 *    clear an ancestor's soft reference, reload it, and orphan the pinned leaf (serving stale bytes).</li>
 *    <li><b>Structural changes</b> (inserts, removes, splits, merges) create plain (non-{@link SoftNode})
 *    nodes that remain strongly reachable from the root — hence GC-safe — and are persisted at flush via
 *    {@link #persistNewNodes}. The blocks they replace are accumulated in {@code pendingFrees} and only
 *    reclaimed after their replacements are on disk, so a block the on-disk tree still references is never
 *    reused (or truncated past) mid-batch.</li>
 * </ul>
 * Durability is unaffected: buffered updates that are lost on an unclean shutdown are rebuilt from the data
 * log, which remains the source of truth.
 *
 * @author William Burns
 */
public class BufferedSoftBPlusTree<V> extends SoftBPlusTree<V> {
   private final int flushMutationCount;
   /** True when buffered mutations have not yet been persisted via {@link #flush()}. */
   private boolean dirty;
   /** Mutations applied since the last flush; when it reaches {@link #flushMutationCount} a flush is triggered. */
   private int mutationCount;
   /** SoftNodes whose backing leaf received in-place value overwrites that are pending a flush. */
   private final Set<PinnableSoftNode<V>> dirtyOverwrites = new HashSet<>();
   /** Every SoftNode currently pinned on the path(s) to a buffered overwrite (see {@link PinnableSoftNode}). */
   private final Set<PinnableSoftNode<V>> pinnedNodes = new HashSet<>();

   /**
    * @param flushMutationCount number of mutations to buffer before an automatic {@link #flush()};
    *                           the remaining parameters are as in
    *                           {@link SoftBPlusTree#SoftBPlusTree(int, int, NodeStore, ValueSerializer, KeyLoader, short, long)}
    */
   public BufferedSoftBPlusTree(int minNodeSize, int maxNodeSize, NodeStore store,
                                ValueSerializer<V> serializer, KeyLoader<V> keyLoader,
                                short blockAlignment, long initialStoreSize, int flushMutationCount) {
      super(minNodeSize, maxNodeSize, store, serializer, keyLoader, blockAlignment, initialStoreSize);
      this.flushMutationCount = flushMutationCount;
   }

   /**
    * @see #BufferedSoftBPlusTree(int, int, NodeStore, ValueSerializer, KeyLoader, short, long, int)
    * @see SoftBPlusTree#SoftBPlusTree(int, int, NodeStore, ValueSerializer, KeyLoader)
    */
   public BufferedSoftBPlusTree(int minNodeSize, int maxNodeSize, NodeStore store,
                                ValueSerializer<V> serializer, KeyLoader<V> keyLoader, int flushMutationCount) {
      super(minNodeSize, maxNodeSize, store, serializer, keyLoader);
      this.flushMutationCount = flushMutationCount;
   }

   /** @return true if there are buffered mutations not yet persisted via {@link #flush()}. */
   public boolean isDirty() {
      return dirty;
   }

   @Override
   protected SoftNode<V> newSoftNode(Node<V> actual, long diskOffset, short occupiedSpace) {
      return new PinnableSoftNode<>(actual, diskOffset, occupiedSpace, this);
   }

   @Override
   protected SoftNode<V> newSoftNode(long diskOffset, short occupiedSpace) {
      return new PinnableSoftNode<>(diskOffset, occupiedSpace, this);
   }

   @Override
   protected void onValueOverwritten(Deque<PathEntry<V>> stack, LeafNode<V> leaf, int entryIndex) {
      if (stack.isEmpty()) return;
      PathEntry<V> parent = stack.peek();
      Node<V> childNode = parent.node().children[parent.index()];
      if (!(childNode instanceof PinnableSoftNode<V> softNode)) return;
      // Pin every SoftNode on the path (top-level child down to the leaf) so no ancestor can be reloaded
      // and orphan the mutated leaf, then record the leaf for a single coalesced in-place write at flush
      // time. Plain (non-Soft) ancestors are already GC-safe. No eager write, unlike the base class.
      for (PathEntry<V> pathEntry : stack) {
         if (pathEntry.node().children[pathEntry.index()] instanceof PinnableSoftNode<V> pathNode) {
            pin(pathNode);
         }
      }
      dirtyOverwrites.add(softNode);
      dirty = true;
   }

   @Override
   protected boolean tryInPlaceLeafUpdate(Deque<PathEntry<V>> stack, LeafNode<V> oldLeaf, LeafNode<V> newLeaf) {
      // Structural leaf changes fall back to path-copy so the new nodes accumulate as strongly-referenced
      // dirty nodes and coalesce until flush, rather than being written in place immediately.
      return false;
   }

   @Override
   protected void onChildrenReplaced(Node<V>[] oldChildren, int from, int to) {
      super.onChildrenReplaced(oldChildren, from, to);
      for (int i = from; i <= to; i++) {
         if (oldChildren[i] instanceof PinnableSoftNode<V> soft) {
            // Replaced structurally; the replacement carries any overwritten value forward, so drop the
            // now-stale buffered write and release the pin.
            dirtyOverwrites.remove(soft);
            if (pinnedNodes.remove(soft)) {
               soft.clearPin();
            }
         }
      }
   }

   @Override
   protected void afterMutation() throws IOException {
      // Defer both the writes and the frees; flush() applies them once the replacements are on disk.
      dirty = true;
      if (++mutationCount >= flushMutationCount) {
         flush();
      }
   }

   @Override
   public NodeSpace saveTree() throws IOException {
      flush();
      return super.saveTree();
   }

   @Override
   public void clear() {
      super.clear();
      dirtyOverwrites.clear();
      for (PinnableSoftNode<V> pinned : pinnedNodes) {
         pinned.clearPin();
      }
      pinnedNodes.clear();
      dirty = false;
      mutationCount = 0;
   }

   /**
    * Persists all buffered mutations to the store. Newly created nodes on modified paths are written and
    * converted to {@link SoftNode}s; leaves that received in-place value overwrites are re-serialized once at
    * their existing offset; and the blocks vacated during the batch are reclaimed only now that their
    * replacements are on disk. The root itself is not written here — it is persisted separately by
    * {@link #saveTree()}. A no-op when nothing is buffered.
    */
   public void flush() throws IOException {
      mutationCount = 0;
      if (!dirty) {
         return;
      }
      Node<V> r = getRoot();
      if (r instanceof InnerNode<V> inner) {
         persistNewNodes(inner);
      }
      for (PinnableSoftNode<V> soft : dirtyOverwrites) {
         writeNodeInPlace(soft.resolve(), soft.diskOffset, soft.occupiedSpace);
      }
      dirtyOverwrites.clear();
      // The buffered overwrites are now on disk, so the pinned paths can be released for GC.
      for (PinnableSoftNode<V> pinned : pinnedNodes) {
         pinned.clearPin();
      }
      pinnedNodes.clear();
      // Reclaim the blocks replaced during this batch only now that their replacements are on disk.
      for (NodeSpace ns : pendingFrees) {
         freeBlock(ns.offset(), ns.occupiedSpace());
      }
      pendingFrees.clear();
      dirty = false;
   }

   /** Pins a SoftNode with a strong reference to its resolved node until the next {@link #flush()}. */
   private void pin(PinnableSoftNode<V> node) {
      if (pinnedNodes.add(node)) {
         node.markDirtyPinned(node.resolve());
      }
   }

   /**
    * A {@link SoftNode} that can be pinned: while pinned, {@link #resolve()} returns the strongly-held node
    * instead of consulting (and possibly reloading a stale value through) its soft reference. Used to keep a
    * mutated node reachable until its buffered write is flushed.
    */
   static final class PinnableSoftNode<V> extends SoftNode<V> {
      private volatile Node<V> pin;

      PinnableSoftNode(Node<V> actual, long diskOffset, short occupiedSpace, SoftBPlusTree<V> tree) {
         super(actual, diskOffset, occupiedSpace, tree);
      }

      PinnableSoftNode(long diskOffset, short occupiedSpace, SoftBPlusTree<V> tree) {
         super(diskOffset, occupiedSpace, tree);
      }

      @Override
      Node<V> resolve() {
         Node<V> pinned = this.pin;
         if (pinned != null) {
            return pinned;
         }
         return super.resolve();
      }

      void markDirtyPinned(Node<V> resolved) {
         this.pin = resolved;
      }

      void clearPin() {
         this.pin = null;
      }
   }
}
