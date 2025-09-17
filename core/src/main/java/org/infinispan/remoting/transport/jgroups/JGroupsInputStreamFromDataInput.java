package org.infinispan.remoting.transport.jgroups;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class JGroupsInputStreamFromDataInput extends InputStream {
   private final DataInput dataInput;

   public JGroupsInputStreamFromDataInput(DataInput dataInput) {
      this.dataInput = dataInput;
   }

   @Override
   public int read() throws IOException {
      try {
         return dataInput.readByte() & 0xFF;
      } catch (EOFException e) {
         return -1;
      }
   }

   @Override
   public int read(byte[] b, int off, int len) throws IOException {
      // This will throw exception for EOF.. which means either a bug or the socket was closed early, in which
      // case propagation is fine
      dataInput.readFully(b, off, len);
      return len;
   }

   @Override
   public long skip(long n) throws IOException {
      return dataInput.skipBytes((int) (n & 0x7FFF_FFFF));
   }
}
