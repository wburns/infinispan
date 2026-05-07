package org.infinispan.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A {@link BPlusTree} subclass that persists each node independently to a
 * {@link NodeStore} and wraps every inner-node child in a {@link SoftNode}.
 * <p>
 * After each {@link #put} or {@link #remove}, only the nodes on the modified
 * path (from the affected leaf up to the root's children) are serialized and
 * written — typically O(height) writes of a few hundred bytes each. Unmodified
 * subtrees keep their existing disk locations. A {@link SoftReference} at every
 * inner-node level lets the JVM evict cached subtrees under memory pressure;
 * they are reloaded from disk on demand.
 * <p>
 * Keys are not stored on disk. On deserialization, keys are loaded from the
 * external data source via the {@link KeyLoader} callback. This matches the
 * old {@code IndexNode} disk format exactly.
 * <p>
 * Disk space is managed via {@link NodeStore#allocate} and {@link NodeStore#free},
 * reusing freed blocks to avoid index file growth.
 *
 * @param <V> value type
 */
public class SoftBPlusTree<V> extends BPlusTree<V> {
   static final byte HAS_LEAVES = 1;
   static final byte HAS_NODES = 2;

   private final NodeStore store;
   private final ValueSerializer<V> serializer;
   private final KeyLoader<V> keyLoader;
   private final List<NodeSpace> pendingFrees = new ArrayList<>();

   public SoftBPlusTree(int minNodeSize, int maxNodeSize, NodeStore store,
                        ValueSerializer<V> serializer, KeyLoader<V> keyLoader) {
      super(minNodeSize, maxNodeSize);
      this.store = store;
      this.serializer = serializer;
      this.keyLoader = keyLoader;
   }

   @Override
   protected void onChildrenReplaced(Node<V>[] oldChildren, int from, int to) {
      for (int i = from; i <= to; i++) {
         collectFreedNodes(oldChildren[i]);
      }
   }

   private void collectFreedNodes(Node<V> node) {
      if (node instanceof SoftNode<V> soft) {
         pendingFrees.add(new NodeSpace(soft.diskOffset, soft.occupiedSpace));
      }
   }

   @Override
   protected void onValueOverwritten(Deque<PathEntry<V>> stack, LeafNode<V> leaf,
         int entryIndex) throws IOException {
      if (stack.isEmpty()) return;
      PathEntry<V> parent = stack.peek();
      Node<V> childNode = parent.node().children[parent.index()];
      if (!(childNode instanceof SoftNode<V> softNode)) return;
      int valueSize = serializer.serializedSize(leaf.values[entryIndex]);
      int valueOffset = leaf.valuesOffset + entryIndex * valueSize;
      ByteBuffer data = ByteBuffer.allocate(valueSize);
      serializer.write(leaf.values[entryIndex], data);
      data.flip();
      store.write(data, softNode.diskOffset + valueOffset);
   }

   @Override
   protected boolean tryInPlaceLeafUpdate(Deque<PathEntry<V>> stack, LeafNode<V> oldLeaf,
         LeafNode<V> newLeaf) throws IOException {
      if (stack.isEmpty()) return false;

      if (newLeaf.length() < minNodeSize || newLeaf.length() > maxNodeSize) {
         return false;
      }

      PathEntry<V> parentEntry = stack.peek();
      Node<V> childNode = parentEntry.node().children[parentEntry.index()];
      if (!(childNode instanceof SoftNode<V> oldSoftNode)) {
         return false;
      }

      if (parentEntry.index() > 0) {
         byte[] oldLeft = oldLeaf.leftmostKey();
         byte[] newLeft = newLeaf.leftmostKey();
         if (!Arrays.equals(oldLeft, newLeft)) {
            return false;
         }
      }

      ByteBuffer data = serializeNode(newLeaf, serializer);
      NodeSpace newSpace = store.allocate((short) data.remaining());
      store.write(data, newSpace.offset);

      pendingFrees.add(new NodeSpace(oldSoftNode.diskOffset, oldSoftNode.occupiedSpace));

      SoftNode<V> newSoftNode = new SoftNode<>(newLeaf, newSpace.offset, newSpace.occupiedSpace, this);
      parentEntry.node().children[parentEntry.index()] = newSoftNode;

      Iterator<PathEntry<V>> it = stack.iterator();
      it.next();
      if (it.hasNext()) {
         PathEntry<V> grandparentEntry = it.next();
         SoftNode<V> parentSoftNode = (SoftNode<V>) grandparentEntry.node().children[grandparentEntry.index()];
         int refOffset = parentEntry.node().headerLength() + parentEntry.node().keyPartsLength()
               + parentEntry.index() * INNER_NODE_REFERENCE_SIZE;
         ByteBuffer refData = ByteBuffer.allocate(INNER_NODE_REFERENCE_SIZE);
         refData.putLong(newSpace.offset);
         refData.putShort(newSpace.occupiedSpace);
         refData.flip();
         store.write(refData, parentSoftNode.diskOffset + refOffset);
      }

      return true;
   }

   @Override
   public V put(byte[] key, V value) throws IOException {
      V old = super.put(key, value);
      afterMutation();
      return old;
   }

   @Override
   public V remove(byte[] key) throws IOException {
      V old = super.remove(key);
      afterMutation();
      return old;
   }

   @Override
   public void clear() {
      super.clear();
   }

   /**
    * Persists the tree to the store. All descendants should already be persisted
    * as SoftNodes (via afterMutation calls during normal operation). This method
    * serializes the root node itself and returns a descriptor that the caller can
    * write into the file header for {@link #loadTree} to find on restart.
    * <p>
    * The free-block state must be persisted separately by the caller via the
    * {@link NodeStore} implementation.
    *
    * @return the root's disk location, or null if the tree is empty
    */
   public NodeSpace saveTree() throws IOException {
      Node<V> r = getRoot();
      if (r instanceof LeafNode<V> leaf && leaf.values.length == 0) {
         return null;
      }
      if (r instanceof InnerNode<V> inner) {
         persistNewNodes(inner);
      }
      ByteBuffer data = serializeNode(r, serializer);
      NodeSpace space = store.allocate((short) data.remaining());
      store.write(data, space.offset);
      return space;
   }

   /**
    * Reconstructs the tree from a previously persisted root. For an inner root,
    * each child is created as a {@link SoftNode} pointing at its disk location —
    * no child data is loaded until first access.
    */
   public void loadTree(NodeSpace rootSpace) throws IOException {
      if (rootSpace == null) {
         return;
      }
      ByteBuffer data = store.read(rootSpace.offset, rootSpace.occupiedSpace);
      Node<V> root = deserializeNode(data);
      setRoot(root);
   }

   private void afterMutation() throws IOException {
      for (NodeSpace ns : pendingFrees) {
         store.free(ns.offset, ns.occupiedSpace);
      }
      pendingFrees.clear();
      Node<V> r = getRoot();
      if (r instanceof InnerNode<V> inner) {
         persistNewNodes(inner);
      }
   }

   private void persistNewNodes(InnerNode<V> inner) throws IOException {
      for (int i = 0; i < inner.children.length; i++) {
         Node<V> child = inner.children[i];
         if (child instanceof SoftNode) continue;
         if (child instanceof InnerNode<V> childInner) {
            persistNewNodes(childInner);
         }
         ByteBuffer data = serializeNode(child, serializer);
         NodeSpace space = store.allocate((short) data.remaining());
         store.write(data, space.offset);
         inner.children[i] = new SoftNode<>(child, space.offset, space.occupiedSpace, this);
      }
   }

   void clearSoftReferences() {
      Node<V> r = getRoot();
      if (r instanceof InnerNode<V> inner) {
         clearSoftReferencesRecursive(inner);
      }
   }

   private void clearSoftReferencesRecursive(InnerNode<V> inner) {
      for (Node<V> child : inner.children) {
         if (child instanceof SoftNode<V> soft) {
            soft.clearReference();
         }
      }
   }

   // --- Interfaces ---

   /**
    * Serializes and deserializes values stored in leaf nodes. Values are written
    * contiguously after the key-parts section of a serialized leaf node, so the
    * serialized size of every value must be fixed or self-describing.
    * <p>
    * <b>Important:</b> every value in a leaf must serialize to exactly
    * {@link #serializedSize} bytes. The tree relies on this for in-place
    * value overwrites — when a key's value is replaced, only that value's
    * bytes are rewritten at a calculated offset within the serialized node,
    * without re-serializing the entire node.
    *
    * @param <V> value type
    */
   public interface ValueSerializer<V> {
      /**
       * Writes the serialized form of {@code value} into {@code buffer} starting
       * at the buffer's current position. Must advance the position by exactly
       * {@link #serializedSize serializedSize(value)} bytes.
       */
      void write(V value, ByteBuffer buffer);

      /**
       * Reads a value from {@code buffer} starting at its current position.
       * Must advance the position by exactly the number of bytes that were
       * written by {@link #write} for this value.
       */
      V read(ByteBuffer buffer);

      /**
       * Returns the number of bytes that {@link #write} will produce for
       * {@code value}. Must be consistent with {@link #write} — the buffer
       * position must advance by exactly this amount.
       */
      int serializedSize(V value);
   }

   /**
    * Loads the B+ tree key for a given value during leaf node deserialization.
    * <p>
    * Keys are not stored in the serialized node format — only values are
    * persisted. When a leaf node is deserialized, each value is read via
    * {@link ValueSerializer#read} and then passed to this loader to reconstruct
    * the corresponding key. The returned key bytes are used for B+ tree lookups
    * and comparisons.
    * <p>
    * This callback is invoked once per value each time a leaf node is loaded
    * from disk (i.e., when a {@link SoftNode}'s soft reference has been cleared
    * and the node must be re-read).
    *
    * @param <V> value type
    */
   public interface KeyLoader<V> {
      /**
       * Returns the key bytes for the given value, or {@code null} if the
       * key cannot be loaded (e.g., the backing data file has been deleted).
       */
      byte[] loadKey(V value) throws IOException;
   }

   /**
    * Block-allocated storage backend for individual serialized nodes. Each node
    * is written independently at an allocated position and can be freed for reuse
    * when the node is replaced or deleted.
    */
   public interface NodeStore {
      /**
       * Allocates a block of at least {@code length} bytes and returns its
       * location. The returned {@link NodeSpace#occupiedSpace()} may be larger
       * than {@code length} if a suitable free block was reused.
       */
      NodeSpace allocate(short length);

      /**
       * Frees a previously allocated block for future reuse.
       */
      void free(long offset, short occupiedSpace);

      /**
       * Writes {@code data} (from position to limit) at the given offset.
       */
      void write(ByteBuffer data, long offset) throws IOException;

      /**
       * Reads {@code length} bytes from {@code offset}.
       */
      ByteBuffer read(long offset, int length) throws IOException;
   }

   public record NodeSpace(long offset, short occupiedSpace) { }

   // --- Serialization ---

   static <V> ByteBuffer serializeNode(Node<V> node, ValueSerializer<V> serializer) {
      int size;
      if (node instanceof LeafNode<V> leaf) {
         size = leafSerializedSize(serializer, leaf.values);
      } else {
         size = innerNodeSerializedSize((InnerNode<V>) node);
      }
      ByteBuffer buffer = ByteBuffer.allocate(size);
      if (node instanceof LeafNode<V> leaf) {
         buffer.putShort((short) 0);
         buffer.put(HAS_LEAVES);
         buffer.putShort((short) leaf.values.length);
         for (int i = 0; i < leaf.values.length; i++) {
            serializer.write(leaf.values[i], buffer);
         }
      } else if (node instanceof InnerNode<V> inner) {
         buffer.putShort((short) inner.prefix.length);
         buffer.put(inner.prefix);
         buffer.put(HAS_NODES);
         buffer.putShort((short) inner.keyParts.length);
         for (byte[] keyPart : inner.keyParts) {
            buffer.putShort((short) keyPart.length);
            buffer.put(keyPart);
         }
         for (Node<V> child : inner.children) {
            if (child instanceof SoftNode<V> soft) {
               buffer.putLong(soft.diskOffset);
               buffer.putShort(soft.occupiedSpace);
            } else {
               throw new IllegalStateException("Inner node child must be SoftNode during serialization, got: " + child.getClass().getSimpleName());
            }
         }
      }
      buffer.flip();
      return buffer;
   }

   private static <V> int innerNodeSerializedSize(InnerNode<V> inner) {
      int size = 2 + inner.prefix.length + 1 + 2;
      for (byte[] keyPart : inner.keyParts) {
         size += 2 + keyPart.length;
      }
      size += INNER_NODE_REFERENCE_SIZE * inner.children.length;
      return size;
   }

   private static <V> int leafSerializedSize(ValueSerializer<V> serializer, V[] values) {
      int size = INNER_NODE_HEADER_SIZE;
      for (V value : values) {
         size += serializer.serializedSize(value);
      }
      return size;
   }

   @SuppressWarnings("unchecked")
   Node<V> deserializeNode(ByteBuffer buffer) throws IOException {
      short prefixLen = buffer.getShort();
      byte[] prefix = new byte[prefixLen];
      buffer.get(prefix);
      byte flags = buffer.get();
      short count = buffer.getShort();
      if (flags == HAS_LEAVES) {
         V[] values = (V[]) new Object[count];
         byte[][] keys = new byte[count][];
         for (int i = 0; i < count; i++) {
            values[i] = serializer.read(buffer);
            keys[i] = keyLoader.loadKey(values[i]);
         }
         return new LeafNode<>(keys, values, INNER_NODE_HEADER_SIZE);
      } else {
         byte[][] keyParts = new byte[count][];
         for (int i = 0; i < count; i++) {
            short kpLen = buffer.getShort();
            keyParts[i] = new byte[kpLen];
            buffer.get(keyParts[i]);
         }
         int childCount = count + 1;
         Node<V>[] children = new Node[childCount];
         for (int i = 0; i < childCount; i++) {
            long offset = buffer.getLong();
            short occupiedSpace = buffer.getShort();
            children[i] = new SoftNode<>(offset, occupiedSpace, this);
         }
         return new InnerNode<>(prefix, keyParts, children);
      }
   }

   // --- SoftNode ---

   static final class SoftNode<V> extends Node<V> {
      private volatile SoftReference<Node<V>> reference;
      final long diskOffset;
      final short occupiedSpace;
      private final SoftBPlusTree<V> tree;
      private final ReentrantLock lock = new ReentrantLock();

      SoftNode(Node<V> actual, long diskOffset, short occupiedSpace, SoftBPlusTree<V> tree) {
         this.reference = new SoftReference<>(actual);
         this.diskOffset = diskOffset;
         this.occupiedSpace = occupiedSpace;
         this.tree = tree;
      }

      SoftNode(long diskOffset, short occupiedSpace, SoftBPlusTree<V> tree) {
         this.diskOffset = diskOffset;
         this.occupiedSpace = occupiedSpace;
         this.tree = tree;
      }

      @Override
      int getInsertionPoint(byte[] key) {
         try {
            return resolve().getInsertionPoint(key);
         } catch (IOException e) {
            throw new UncheckedIOException(e);
         }
      }

      @Override
      Node<V> resolve() throws IOException {
         Node<V> node;
         SoftReference<Node<V>> ref = this.reference;
         if (ref == null || (node = ref.get()) == null) {
            lock.lock();
            try {
               ref = this.reference;
               if (ref == null || (node = ref.get()) == null) {
                  ByteBuffer data = tree.store.read(diskOffset, occupiedSpace);
                  node = tree.deserializeNode(data);
                  this.reference = new SoftReference<>(node);
               }
            } finally {
               lock.unlock();
            }
         }
         return node;
      }

      @Override
      int length() {
         try {
            return resolve().length();
         } catch (IOException e) {
            throw new UncheckedIOException(e);
         }
      }

      @Override
      byte[] leftmostKey() {
         try {
            return resolve().leftmostKey();
         } catch (IOException e) {
            throw new UncheckedIOException(e);
         }
      }

      @Override
      byte[] rightmostKey() {
         try {
            return resolve().rightmostKey();
         } catch (IOException e) {
            throw new UncheckedIOException(e);
         }
      }

      void clearReference() {
         this.reference = null;
      }

      @Override
      List<Node<V>> split(int maxNodeSize) {
         try {
            return resolve().split(maxNodeSize);
         } catch (IOException e) {
            throw new UncheckedIOException(e);
         }
      }
   }
}
