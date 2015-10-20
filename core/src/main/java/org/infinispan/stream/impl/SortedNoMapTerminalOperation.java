package org.infinispan.stream.impl;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.factories.ComponentRegistry;

import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * TODO:
 * A terminal operation for use with distributed stream
 * @param <Sorted> the type of the sorted result.  In this case it is also resulting values
 */
public interface SortedNoMapTerminalOperation<Sorted> extends SortedMapTerminalOperation<Sorted, Sorted> {
}
