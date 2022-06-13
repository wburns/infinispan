package org.infinispan.client.hotrod.impl.protocol;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.impl.operations.AbstractKeyOperation;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.marshall.Marshaller;

import io.netty.buffer.ByteBuf;

/**
 * @since 14.0
 */
public class Codec40 extends Codec31 {
   @Override
   public HeaderParams writeHeader(ByteBuf buf, HeaderParams params) {
      return writeHeader(buf, params, HotRodConstants.VERSION_31);
   }

   @Override
   public Object returnPossiblePrevValue(Object key, ByteBuf buf, short status, DataFormat dataFormat, int flags,
         ClassAllowList allowList, Marshaller marshaller) {
      if (HotRodConstants.hasPrevious(status)) {
         return AbstractKeyOperation.readEntry(buf, key, dataFormat, allowList);
      }
      return null;
   }
}
