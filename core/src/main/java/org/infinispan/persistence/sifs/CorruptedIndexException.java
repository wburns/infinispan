package org.infinispan.persistence.sifs;

class CorruptedIndexException extends IllegalStateException {
   CorruptedIndexException(String message) {
      super(message);
   }
}
