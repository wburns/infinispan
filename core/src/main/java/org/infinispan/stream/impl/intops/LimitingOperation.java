package org.infinispan.stream.impl.intops;

/**
 * Interface to be implemented by operations that can limit output of the stream.  This is useful to apply
 * one or many {@link LimitableOperation} along with this limit operation both remotely and locally.
 */
public interface LimitingOperation {
    /**
     * The amount to limit the results by
     * @return the limit
     */
    long getLimit();
}
