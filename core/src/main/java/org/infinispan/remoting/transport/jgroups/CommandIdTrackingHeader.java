package org.infinispan.remoting.transport.jgroups;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

import org.infinispan.commands.CommandInvocationId;
import org.jgroups.Header;
import org.jgroups.util.UUID;

public class CommandIdTrackingHeader extends Header {
   public static final short MAGIC_ID = 12411;
   private CommandInvocationId commandInvocationId;

   public CommandIdTrackingHeader() { }

   public CommandIdTrackingHeader(CommandInvocationId id) {
      this.commandInvocationId = id;
   }
   @Override
   public short getMagicId() {
      return MAGIC_ID;
   }

   @Override
   public Supplier<? extends Header> create() {
      return CommandIdTrackingHeader::new;
   }

   @Override
   public int serializedSize() {
      return 8 + ((JGroupsAddress) commandInvocationId.getAddress()).getJGroupsAddress().serializedSize();
   }

   @Override
   public void writeTo(DataOutput out) throws IOException {
      out.writeLong(commandInvocationId.getId());
      ((JGroupsAddress) commandInvocationId.getAddress()).getJGroupsAddress().writeTo(out);
   }

   @Override
   public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
      long id = in.readLong();
      UUID jgroupsAddress = new UUID();
      jgroupsAddress.readFrom(in);
      commandInvocationId = new CommandInvocationId(new JGroupsAddress(jgroupsAddress), id);
   }

   @Override
   public String toString() {
      return "[commandInvocationId=" + commandInvocationId + ']';
   }
}
