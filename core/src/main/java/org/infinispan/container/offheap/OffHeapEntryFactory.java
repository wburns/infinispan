package org.infinispan.container.offheap;

import java.io.IOException;

import org.infinispan.Version;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.TimeService;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * @author wburns
 * @since 9.0
 */
public class OffHeapEntryFactory {
   private final Marshaller marshaller;
   private final ByteBufAllocator allocator;
   private final TimeService timeService;
   private final InternalEntryFactory internalEntryFactory;

   // If custom than we just store the metadata as is (no other bits should be used)
   private static final byte CUSTOM = 1;
   // Version can be set with any combination of the following types
   private static final byte HAS_VERSION = 2;
   // Only one of the following should ever be set
   private static final byte IMMORTAL = 1 << 2;
   private static final byte MORTAL = 1 << 3;
   private static final byte TRANSIENT = 1 << 4;
   private static final byte TRANSIENTMORTAL = 1 << 5;

   public OffHeapEntryFactory(Marshaller marshaller, ByteBufAllocator allocator, TimeService timeService,
                              InternalEntryFactory internalEntryFactory) {
      this.marshaller = marshaller;
      this.allocator = allocator;
      this.timeService = timeService;
      this.internalEntryFactory = internalEntryFactory;
   }

   public ByteBuf create(WrappedBytes key, WrappedBytes value, Metadata metadata) {
      byte type;
      byte[] metadataBytes;
      if (metadata instanceof EmbeddedMetadata) {

         EntryVersion version = metadata.version();
         byte[] versionBytes;
         if (version != null) {
            type = HAS_VERSION;
            try {
               versionBytes = marshaller.objectToByteBuffer(version);
            } catch (IOException | InterruptedException e) {
               throw new CacheException(e);
            }
         } else {
            type = 0;
            versionBytes = new byte[0];
         }

         long lifespan = metadata.lifespan();
         long maxIdle = metadata.maxIdle();

         if (lifespan < 0 && maxIdle < 0) {
            type |= IMMORTAL;
            metadataBytes = versionBytes;
         } else if (lifespan > -1 && maxIdle < 0) {
            type |= MORTAL;
            metadataBytes = new byte[16 + versionBytes.length];
            putLong(metadataBytes, 0, lifespan);
            putLong(metadataBytes, 8, timeService.wallClockTime());
            System.arraycopy(metadataBytes, 0, versionBytes, 16, versionBytes.length);
         } else if (lifespan < 0 && maxIdle > -1) {
            type |= TRANSIENT;
            metadataBytes = new byte[16 + versionBytes.length];
            putLong(metadataBytes, 0, maxIdle);
            putLong(metadataBytes, 8, timeService.wallClockTime());
            System.arraycopy(metadataBytes, 0, versionBytes, 16, versionBytes.length);
         } else {
            type |= TRANSIENTMORTAL;
            metadataBytes = new byte[32 + versionBytes.length];
            putLong(metadataBytes, 0, maxIdle);
            putLong(metadataBytes, 8, lifespan);
            putLong(metadataBytes, 16, timeService.wallClockTime());
            putLong(metadataBytes, 24, timeService.wallClockTime());
            System.arraycopy(metadataBytes, 0, versionBytes, 16, versionBytes.length);
         }
      } else {
         type = CUSTOM;
         try {
            metadataBytes = marshaller.objectToByteBuffer(metadata);
         } catch (IOException | InterruptedException e) {
            throw new CacheException(e);
         }
      }
      int keySize = key.getLength();
      int valueSize = value.getLength();
      int metadataSize = metadataBytes.length;
      int totalSize = keySize + 1 + metadataSize + valueSize;

      ByteBuf buf = allocator.directBuffer(totalSize, totalSize);
      buf.writeBytes(key.getBytes(), key.backArrayOffset(), keySize);
      buf.writeByte(type);
      buf.writeBytes(metadataBytes);
      buf.writeBytes(value.getBytes(), value.backArrayOffset(), valueSize);
      // Set read index to the end of the key
      // Set write index to the end of the metadata
      buf.setIndex(keySize, keySize + 1 + metadataSize);
      return buf;
   }

   public InternalCacheEntry<WrappedByteArray, WrappedByteArray> fromBuffer(ByteBuf buf) {
      byte[] readerBytes = new byte[buf.readerIndex()];
      byte[] metadataBytes = new byte[buf.writerIndex() - buf.readerIndex()];
      byte[] valueBytes = new byte[buf.capacity() - buf.writerIndex()];
      // Read sequentially
      buf.getBytes(0, readerBytes);
      buf.getBytes(buf.readerIndex(), metadataBytes);
      buf.getBytes(buf.writerIndex(), valueBytes);

      byte metadataType = metadataBytes[0];
      Metadata metadata;
      // This is a custom metadata
      if ((metadataType & 1) == 1) {
         byte[] marshallerBytes = new byte[buf.writerIndex() - buf.readerIndex()];
         buf.getBytes(buf.readerIndex(), marshallerBytes);
         try {
            metadata = (Metadata) marshaller.objectFromByteBuffer(marshallerBytes);
         } catch (IOException | ClassNotFoundException e) {
            throw new CacheException(e);
         }
         return internalEntryFactory.create(new WrappedByteArray(readerBytes),
               new WrappedByteArray(valueBytes), metadata);
      } else {
         long lifespan;
         long maxIdle;
         long created;
         long lastUsed;
         int offset = 0;
         boolean hasVersion = (metadataType & 2) == 2;
         // Ignore CUSTOM and VERSION to find type
         switch (metadataType & 0xFC) {
            case IMMORTAL:
               lifespan = -1;
               maxIdle = -1;
               created = -1;
               lastUsed = -1;
               break;
            case MORTAL:
               maxIdle = -1;
               lifespan = getLong(metadataBytes, ++offset);
               created = getLong(metadataBytes, offset += 8);
               lastUsed = -1;
               break;
            case TRANSIENT:
               lifespan = -1;
               maxIdle = getLong(metadataBytes, ++offset);
               created = -1;
               lastUsed = getLong(metadataBytes, offset += 8);
               break;
            case TRANSIENTMORTAL:
               lifespan = getLong(metadataBytes, ++offset);
               maxIdle = getLong(metadataBytes, offset += 8);
               created = getLong(metadataBytes, offset += 8);
               lastUsed = getLong(metadataBytes, offset += 8);
               break;
            default:
               throw new IllegalArgumentException("Unsupported type: " + metadataType);
         }
         if (hasVersion) {
            try {
               EntryVersion version = (EntryVersion) marshaller.objectFromByteBuffer(metadataBytes, offset,
                     metadataBytes.length - offset);
               return internalEntryFactory.create(new WrappedByteArray(readerBytes),
                     new WrappedByteArray(valueBytes), version, created, lifespan, lastUsed, maxIdle);
            } catch (IOException | ClassNotFoundException e) {
               throw new CacheException(e);
            }
         } else {
            return internalEntryFactory.create(new WrappedByteArray(readerBytes),
                  new WrappedByteArray(valueBytes), (Metadata) null, created, lifespan, lastUsed, maxIdle);
         }
      }
   }

   static long getLong(byte[] b, int off) {
      return ((b[off + 7] & 0xFFL)      ) +
            ((b[off + 6] & 0xFFL) <<  8) +
            ((b[off + 5] & 0xFFL) << 16) +
            ((b[off + 4] & 0xFFL) << 24) +
            ((b[off + 3] & 0xFFL) << 32) +
            ((b[off + 2] & 0xFFL) << 40) +
            ((b[off + 1] & 0xFFL) << 48) +
            (((long) b[off])      << 56);
   }

   static void putLong(byte[] b, int off, long val) {
      b[off + 7] = (byte) (val       );
      b[off + 6] = (byte) (val >>>  8);
      b[off + 5] = (byte) (val >>> 16);
      b[off + 4] = (byte) (val >>> 24);
      b[off + 3] = (byte) (val >>> 32);
      b[off + 2] = (byte) (val >>> 40);
      b[off + 1] = (byte) (val >>> 48);
      b[off    ] = (byte) (val >>> 56);
   }
}
