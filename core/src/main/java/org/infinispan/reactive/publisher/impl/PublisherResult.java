package org.infinispan.reactive.publisher.impl;

import org.infinispan.commons.util.IntSet;

/**
 * @author wburns
 * @since 10.0
 */
public interface PublisherResult<R> {
   IntSet getSuspectedSegments();

   R getResult();
}
