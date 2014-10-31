package org.infinispan.tasks;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedCallable;

/**
 * GlobalKeySetTask is a way for obtaining all of the keys
 * across a cluster.
 *
 * @author Manik Surtani
 * @author Tristan Tarrant
 * @since 5.3
 */
public class GlobalKeySetTask<K, V> {
   public static <K, V> Set<K> getGlobalKeySet(Cache<K, V> cache) throws InterruptedException, ExecutionException {
      return cache.keySet();
   }
}
