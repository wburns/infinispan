package org.infinispan.remoting.transport.jgroups;

import java.io.DataInput;
import java.io.IOException;

import org.jgroups.util.BaseDataInputStream;

public class JGroupsInputStreamFromDataInput extends BaseDataInputStream {
   private final DataInput dataInput;

   public JGroupsInputStreamFromDataInput(DataInput dataInput) {
      this.dataInput = dataInput;
   }

   @Override
   public int read() throws IOException {
      return dataInput.readByte();
   }

   @Override
   public int skipBytes(int n) throws IOException {
      return dataInput.skipBytes(n);
   }
}
