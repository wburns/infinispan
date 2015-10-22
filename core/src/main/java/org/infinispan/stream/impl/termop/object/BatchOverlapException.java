package org.infinispan.stream.impl.termop.object;

import org.infinispan.commons.CacheException;

/**
 * Created by wburns on 10/22/15.
 */
public class BatchOverlapException extends CacheException {
   public BatchOverlapException(String message) {
      super(message);
   }
}