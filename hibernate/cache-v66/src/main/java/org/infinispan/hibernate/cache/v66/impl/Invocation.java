package org.infinispan.hibernate.cache.v66.impl;

import java.util.concurrent.CompletableFuture;

interface Invocation {
   CompletableFuture<?> invoke(boolean success);
}
