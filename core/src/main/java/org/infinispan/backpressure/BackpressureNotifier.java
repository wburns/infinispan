package org.infinispan.backpressure;

import org.infinispan.remoting.transport.Address;

public interface BackpressureNotifier {
   void registerListener(BackpressureListener listener);

   void unregisterListener(BackpressureInterceptor listener);
   void memberPressured(Address address);
   void memberRelieved(Address address);

   interface BackpressureListener {
      void memberPressured(Address address);
      void memberRelieved(Address address);
   }
}
