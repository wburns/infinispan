package org.infinispan.backpressure;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CopyOnWriteArraySet;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

@Scope(Scopes.GLOBAL)
public class BackpressureNotifierImpl implements BackpressureNotifier {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private final CopyOnWriteArraySet<BackpressureListener> listeners = new CopyOnWriteArraySet<>();

   @Override
   public void registerListener(BackpressureListener listener) {
      listeners.add(listener);
   }

   @Override
   public void unregisterListener(BackpressureInterceptor listener) {
      listeners.remove(listener);
   }

   @Override
   public void memberPressured(Address address) {
      listeners.forEach(l -> {
         log.tracef("Notifying listener %s that %s is pressured", l, address);
         l.memberPressured(address);
      });
   }

   @Override
   public void memberRelieved(Address address) {
      listeners.forEach(l -> {
         log.tracef("Notifying listener %s that %s is relieved", l, address);
         l.memberRelieved(address);
      });
   }
}
