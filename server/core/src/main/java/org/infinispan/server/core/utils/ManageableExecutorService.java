package org.infinispan.server.core.utils;

import java.util.concurrent.ExecutorService;

import org.infinispan.jmx.annotations.MBean;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 */
@MBean(objectName = "WorkerExecutor")
public class ManageableExecutorService extends org.infinispan.executors.ManageableExecutorService<ExecutorService> {

   public ManageableExecutorService(ExecutorService threadPoolExecutor) {
      this.executor = threadPoolExecutor;
   }
}
