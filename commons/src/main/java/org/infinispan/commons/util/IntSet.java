package org.infinispan.commons.util;

import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

/**
 * A set that represents primitive ints. This interface describes methods that can be used without having to box an int.
 * @author wburns
 * @since 9.2
 */
public interface IntSet extends Set<Integer> {

   /**
    * Adds the given int to this set and returns whether the int was present before
    * @param i the int value to add
    * @return whether this int was already present
    */
   boolean add(int i);

   /**
    * Adds or sets the int without returning whether it was previously set
    * @param i the value to make sure is in the set
    */
   void set(int i);

   /**
    * Removes, if present, the int from the set and returns if it was present or not
    * @param i the int to remove
    * @return whether the int was present in the set before it was removed
    */
   boolean remove(int i);

   /**
    * Whether this set contains the given int
    * @param i the int to check
    * @return if the set contains the int
    */
   boolean contains(int i);

   /**
    * Adds all ints from the provided set into this one
    * @param set the set of ints to add
    * @return if this set has a new int in it
    */
   boolean addAll(IntSet set);

   /**
    * Whether this set contains all ints in the given IntSet
    * @param set the set to check if all are present
    * @return if the set contains all the ints
    */
   boolean containsAll(IntSet set);

   /**
    * Removes all ints from this IntSet that are in the provided IntSet
    * @param set the ints to remove from this IntSet
    * @return if this set removed any ints
    */
   boolean removeAll(IntSet set);

   /**
    * Modifies this set to only remove all ints that are not present in the provided IntSet
    * @param c the ints this set should kep
    * @return if this set removed any ints
    */
   boolean retainAll(IntSet c);

   /**
    * A primtive iterator that allows iteration over the int values. This iterator supports removal if the set is
    * modifiable.
    * @return the iterator
    */
   PrimitiveIterator.OfInt iterator();

   /**
    * Performs the given action for each element of the {@code IntSet}
    * until all elements have been processed or the action throws an
    * exception.  Unless otherwise specified by the implementing class,
    * actions are performed in the order of iteration (if an iteration order
    * is specified).  Exceptions thrown by the action are relayed to the
    * caller.
    * @implSpec
    * <p>The default implementation behaves as if:
    * <pre>{@code
    *     iterator().forEachRemaining(consumer);
    * }</pre>
    * @param consumer The action to be performed for each element
    */
   default void forEach(IntConsumer consumer) {
      iterator().forEachRemaining(consumer);
   }

   /**
    * A stream of ints representing the data in this set
    * @return the stream
    */
   IntStream intStream();

   /**
    * Returns an array containing all of the elements in this set.
    * If this set makes any guarantees as to what order its elements
    * are returned by its iterator, this method must return the
    * elements in the same order.
    * @return this int set as an array
    */
   default int[] toIntArray() {
      int[] array = new int[size()];
      PrimitiveIterator.OfInt iter = iterator();
      int i = 0;
      while (iter.hasNext()) {
         array[i] = iter.next();
      }
      return array;
   }
}
