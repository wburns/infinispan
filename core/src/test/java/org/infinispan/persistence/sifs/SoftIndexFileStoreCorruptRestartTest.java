package org.infinispan.persistence.sifs;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfigurationBuilder;
import org.infinispan.distribution.BaseDistStoreTest;
import org.infinispan.encoding.DataConversion;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.support.WaitDelegatingNonBlockingStore;
import org.infinispan.test.TestingUtil;
import org.infinispan.testing.Testing;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.sifs.SoftIndexFileStoreCorruptRestartTest")
public class SoftIndexFileStoreCorruptRestartTest extends BaseDistStoreTest<Integer, String, SoftIndexFileStoreRestartTest> {
   protected String tmpDirectory;

   {
      // We don't really need a cluster
      INIT_CLUSTER_SIZE = 1;
      l1CacheEnabled = false;
      segmented = true;
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new SoftIndexFileStoreCorruptRestartTest().cacheMode(CacheMode.LOCAL),
//            new SoftIndexFileStoreCorruptRestartTest().cacheMode(CacheMode.DIST_SYNC),
      };
   }

   @BeforeClass(alwaysRun = true)
   @Override
   public void createBeforeClass() throws Throwable {
      tmpDirectory = Testing.tmpDirectory(getClass());
      super.createBeforeClass();
   }


   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      super.destroy();
      Util.recursiveFileRemove(tmpDirectory);
   }

   @Override
   protected StoreConfigurationBuilder addStore(PersistenceConfigurationBuilder persistenceConfigurationBuilder, boolean shared) {
      // We don't support shared for SIFS
      assert !shared;
      return persistenceConfigurationBuilder.addSoftIndexFileStore()
            .maxFileSize(1000)
            .dataLocation(Paths.get(tmpDirectory, "data").toString())
            .indexLocation(Paths.get(tmpDirectory, "index").toString());
   }

   @Test(dataProvider = "booleans")
   public void testRestartWithEmptyDataFile(boolean deleteIndex) throws Throwable {
      cache(0, cacheName).put("some-value", "1");
      assertEquals(1, cache(0, cacheName).size());

      killMember(0, cacheName);

      if (deleteIndex) {
         // Delete the index which should force it to rebuild
         Util.recursiveFileRemove(Paths.get(tmpDirectory, "index"));
      }

      File file = Paths.get(tmpDirectory, "data", cacheName, "data", "ispn12.123").toFile();
      if (!file.createNewFile()) {
         fail("Unable to create file: " + file);
      }

      createCacheManagers();

      WaitDelegatingNonBlockingStore store = TestingUtil.getFirstStoreWait(cache(0, cacheName));

      Compactor compactor = TestingUtil.extractField(store.delegate(), "compactor");

      // Force compaction for the previous file
      CompletionStages.join(compactor.forceCompactionForAllNonLogFiles());

      assertEquals(1, cache(0, cacheName).size());
   }

   MarshallableEntry objectToMarshalledBuffer(Object obj) {
      DataConversion dc = cache(0, cacheName).getAdvancedCache().getKeyDataConversion();
      Object storedKey = dc.toStorage(obj);

      MarshallableEntryFactory mef = TestingUtil.extractComponent(cache(0, cacheName), MarshallableEntryFactory.class);

      return mef.create(storedKey);
   }

   @DataProvider(name = "booleans")
   Object[][] booleans() {
      return new Object[][]{
            {Boolean.TRUE}, {Boolean.FALSE}};
   }

   @Test(dataProvider = "booleans")
   public void testRestartWithPartialKeyWritten(boolean deleteIndex) throws Throwable {
      cache(0, cacheName).put("some-value", "1");
      assertEquals(1, cache(0, cacheName).size());

      org.infinispan.commons.io.ByteBuffer keyBytes = objectToMarshalledBuffer("key-1").getKeyBytes();

      ByteBuffer trimmedKey = ByteBuffer.wrap(keyBytes.getBuf(), 0, keyBytes.getLength() - 1);

      killMember(0, cacheName);

      if (deleteIndex) {
         // Delete the index which should force it to rebuild
         Util.recursiveFileRemove(Paths.get(tmpDirectory, "index"));
      }

      File file = Paths.get(tmpDirectory, "data", cacheName, "data", "ispn12.123").toFile();
      if (!file.createNewFile()) {
         fail("Unable to create file: " + file);
      }

      try (FileOutputStream stream = new FileOutputStream(file)) {
         FileChannel channel = stream.getChannel();
         ByteBuffer buffer = ByteBuffer.allocate(EntryHeader.HEADER_SIZE_11_0);
         EntryHeader.writeHeader(buffer, (short) (trimmedKey.remaining() + 2), (short) 0, 0, (short) 0, 20L, -1L);
         buffer.flip();
         int writeCount = channel.write(buffer);
         assertEquals(EntryHeader.HEADER_SIZE_11_0, writeCount);

         int expected = trimmedKey.remaining();
         assertEquals(expected, channel.write(trimmedKey));
      }

      createCacheManagers();

      WaitDelegatingNonBlockingStore store = TestingUtil.getFirstStoreWait(cache(0, cacheName));

      Compactor compactor = TestingUtil.extractField(store.delegate(), "compactor");

      // Force compaction for the previous file
      CompletionStages.join(compactor.forceCompactionForAllNonLogFiles());

      assertEquals(1, cache(0, cacheName).size());
   }

   @Test(dataProvider = "booleans")
   public void testRestartWithOnlyHeaderWritten(boolean deleteIndex) throws Throwable {
      cache(0, cacheName).put("some-value", "1");
      assertEquals(1, cache(0, cacheName).size());

      killMember(0, cacheName);

      if (deleteIndex) {
         // Delete the index which should force it to rebuild
         Util.recursiveFileRemove(Paths.get(tmpDirectory, "index"));
      }

      File file = Paths.get(tmpDirectory, "data", cacheName, "data", "ispn12.123").toFile();
      if (!file.createNewFile()) {
         fail("Unable to create file: " + file);
      }

      try (FileOutputStream stream = new FileOutputStream(file)) {
         FileChannel channel = stream.getChannel();
         ByteBuffer buffer = ByteBuffer.allocate(EntryHeader.HEADER_SIZE_11_0);
         EntryHeader.writeHeader(buffer, (short) 10, (short) 0, 100, (short) 0, 20L, -1L);
         buffer.flip();
         int writeCount = channel.write(buffer);
         assertEquals(EntryHeader.HEADER_SIZE_11_0, writeCount);
      }

      createCacheManagers();

      WaitDelegatingNonBlockingStore store = TestingUtil.getFirstStoreWait(cache(0, cacheName));

      Compactor compactor = TestingUtil.extractField(store.delegate(), "compactor");

      // Force compaction for the previous file
      CompletionStages.join(compactor.forceCompactionForAllNonLogFiles());

      assertEquals(1, cache(0, cacheName).size());
   }

   // Test for issue #17119 - SoftIndexFileStore reindex at runtime
   // This test writes entries until the log appender rolls to a new file, then truncates
   // the PREVIOUS log file to simulate index pointing to corrupted/missing data.
   // The test verifies that SIFS detects the corruption, invalidates the index entry,
   // logs a warning, and returns null (rather than throwing an exception).
   public void testRestartWithMissingNewestDataFile() throws Throwable {
      WaitDelegatingNonBlockingStore store = TestingUtil.getFirstStoreWait(cache(0, cacheName));
      NonBlockingSoftIndexFileStore sifsStore = (NonBlockingSoftIndexFileStore) store.delegate();
      LogAppender logAppender = TestingUtil.extractField(sifsStore, "logAppender");

      int keyCounter = 0;
      Integer previousLogFileId = null;
      Integer currentLogFileId = null;
      String keyInPreviousFile = null;
      Integer firstKeyInPreviousFile = null;

      // Write entries until the log appender rolls to a new file
      while (true) {
         // Get the current log file from the log appender
         FileProvider.Log currentLogFile = TestingUtil.extractField(logAppender, "logFile");
         currentLogFileId = currentLogFile != null ? currentLogFile.fileId : null;

         // Track the first key written to each log file
         if (currentLogFileId != null && !currentLogFileId.equals(previousLogFileId)) {
            if (previousLogFileId != null) {
               // The log appender has rolled to a new file
               // Use a key from the middle of the previous file, not the last one
               if (firstKeyInPreviousFile != null) {
                  int middleKey = firstKeyInPreviousFile + ((keyCounter - 1 - firstKeyInPreviousFile) / 2);
                  keyInPreviousFile = "key-" + middleKey;
               }
               break;
            }
            firstKeyInPreviousFile = keyCounter;
            previousLogFileId = currentLogFileId;
         }

         cache(0, cacheName).put("key-" + keyCounter, "value-" + keyCounter);
         keyCounter++;

         if (keyCounter > 10000) {
            fail("Failed to roll to a new log file after " + keyCounter + " entries");
         }
      }

      // Clear all entries from memory to force loading from store
      cache(0, cacheName).getAdvancedCache().getDataContainer().clear();

      // Truncate the PREVIOUS log file (the finalized one) WHILE THE CACHE IS STILL RUNNING
      // This simulates issue #17119 where the index points to a file that has been corrupted
      Path previousLogFilePath = Paths.get(tmpDirectory, "data", cacheName, "data",
            NonBlockingSoftIndexFileStore.PREFIX_LATEST + previousLogFileId);
      File previousLogFile = previousLogFilePath.toFile();

      if (!previousLogFile.exists()) {
         fail("Previous log file does not exist: " + previousLogFile);
      }

      long originalSize = previousLogFile.length();

      // Close the file handle to force re-reading from disk
      FileProvider fileProvider = TestingUtil.extractField(sifsStore, "fileProvider");

      // Get the file handle if it's open and close it to release the handle
      FileProvider.Handle handle = fileProvider.getFileIfOpen(previousLogFileId);
      if (handle != null) {
         handle.close();
      }

      // Truncate the file to just a few bytes WHILE THE CACHE IS STILL RUNNING
      // This will corrupt the data and cause the index to point to invalid locations
      try (RandomAccessFile raf = new RandomAccessFile(previousLogFile, "rw")) {
         raf.setLength(10); // Keep just 10 bytes, corrupt the rest
      }

      // Try to fetch the value that was in the truncated file WITHOUT RESTARTING
      // After the fix, SIFS should detect the corruption, log a warning, remove the index entry,
      // and return null instead of throwing an exception
      Object value = cache(0, cacheName).get(keyInPreviousFile);

      // The value should be null since the file was corrupted and the index entry should be removed
      assertNull("Expected null when loading key from truncated log file (was " +
            originalSize + " bytes, now 10)", value);

      // Verify the entry was actually removed from the index by checking the index directly
      // Use eventually() since the index update is asynchronous
      Index index = TestingUtil.extractField(sifsStore, "index");
      org.infinispan.commons.marshall.Marshaller marshaller = TestingUtil.extractField(sifsStore, "marshaller");
      org.infinispan.commons.io.ByteBuffer serializedKey = marshaller.objectToBuffer(keyInPreviousFile);
      int segmentId = cache(0, cacheName).getAdvancedCache().getDistributionManager() != null ?
            cache(0, cacheName).getAdvancedCache().getDistributionManager().getCacheTopology().getSegment(keyInPreviousFile) : 0;

      // Create final copies for lambda
      final String finalKey = keyInPreviousFile;
      final Index finalIndex = index;
      final org.infinispan.commons.io.ByteBuffer finalSerializedKey = serializedKey;
      final int finalSegmentId = segmentId;

      eventually("Entry position should be removed from index after corruption detected", () -> {
         try {
            EntryPosition positionAfter = finalIndex.getPosition(finalKey, finalSegmentId, finalSerializedKey);
            return positionAfter == null;
         } catch (Exception e) {
            return false;
         }
      });

      // Verify the entry is truly gone by trying to read it again
      // This should return null immediately without trying to read from the corrupted file
      Object value2 = cache(0, cacheName).get(keyInPreviousFile);
      assertNull("Entry should remain null after being removed from index", value2);
   }

   // Test for issue #17119 - iteration should handle corrupted entries gracefully
   // This test writes entries until the log appender rolls to a new file, then truncates
   // the PREVIOUS log file and performs iteration to verify corrupted entries are skipped
   // and removed from the index.
   public void testIterationWithCorruptedDataFile() throws Throwable {
      WaitDelegatingNonBlockingStore store = TestingUtil.getFirstStoreWait(cache(0, cacheName));
      NonBlockingSoftIndexFileStore sifsStore = (NonBlockingSoftIndexFileStore) store.delegate();
      LogAppender logAppender = TestingUtil.extractField(sifsStore, "logAppender");

      int keyCounter = 0;
      Integer previousLogFileId = null;
      Integer currentLogFileId = null;
      String keyInPreviousFile = null;
      Integer firstKeyInPreviousFile = null;

      // Write entries until the log appender rolls to a new file
      while (true) {
         // Get the current log file from the log appender
         FileProvider.Log currentLogFile = TestingUtil.extractField(logAppender, "logFile");
         currentLogFileId = currentLogFile != null ? currentLogFile.fileId : null;

         // Track the first key written to each log file
         if (currentLogFileId != null && !currentLogFileId.equals(previousLogFileId)) {
            if (previousLogFileId != null) {
               // The log appender has rolled to a new file
               // Use a key from the middle of the previous file, not the last one
               if (firstKeyInPreviousFile != null) {
                  int middleKey = firstKeyInPreviousFile + ((keyCounter - 1 - firstKeyInPreviousFile) / 2);
                  keyInPreviousFile = "key-" + middleKey;
               }
               break;
            }
            firstKeyInPreviousFile = keyCounter;
            previousLogFileId = currentLogFileId;
         }

         cache(0, cacheName).put("key-" + keyCounter, "value-" + keyCounter);
         keyCounter++;

         if (keyCounter > 10000) {
            fail("Failed to roll to a new log file after " + keyCounter + " entries");
         }
      }

      // Remember how many keys we have
      int totalKeys = keyCounter;

      // Clear all entries from memory to force loading from store
      cache(0, cacheName).getAdvancedCache().getDataContainer().clear();

      // Truncate the PREVIOUS log file (the finalized one) WHILE THE CACHE IS STILL RUNNING
      Path previousLogFilePath = Paths.get(tmpDirectory, "data", cacheName, "data",
            NonBlockingSoftIndexFileStore.PREFIX_LATEST + previousLogFileId);
      File previousLogFile = previousLogFilePath.toFile();

      if (!previousLogFile.exists()) {
         fail("Previous log file does not exist: " + previousLogFile);
      }

      long originalSize = previousLogFile.length();

      // Close the file handle to force re-reading from disk
      FileProvider fileProvider = TestingUtil.extractField(sifsStore, "fileProvider");

      // Get the file handle if it's open and close it to release the handle
      FileProvider.Handle handle = fileProvider.getFileIfOpen(previousLogFileId);
      if (handle != null) {
         handle.close();
      }

      // Truncate the file to just a few bytes WHILE THE CACHE IS STILL RUNNING
      try (RandomAccessFile raf = new RandomAccessFile(previousLogFile, "rw")) {
         raf.setLength(10); // Keep just 10 bytes, corrupt the rest
      }

      // Perform iteration - should skip the corrupted entry and continue with others
      int iteratedCount = 0;
      boolean foundCorruptedKey = false;
      for (Object key : cache(0, cacheName).keySet()) {
         iteratedCount++;
         if (key.equals(keyInPreviousFile)) {
            foundCorruptedKey = true;
         }
      }

      // The corrupted key should not be found during iteration
      assertNull("Corrupted key should not be returned during iteration, should be null",
            foundCorruptedKey ? "found" : null);

      // We should have iterated through all keys except the corrupted one(s)
      // Note: there might be multiple corrupted keys in the truncated file
      log.infof("Iterated %d keys out of %d total (file was %d bytes, now 10)",
            iteratedCount, totalKeys, originalSize);

      // Now try to access the corrupted key directly via load()
      // This should trigger index cleanup lazily
      Object value = cache(0, cacheName).get(keyInPreviousFile);
      assertNull("Corrupted key should return null when accessed directly", value);

      // Verify the index was cleaned up after the direct access
      Index index = TestingUtil.extractField(sifsStore, "index");
      org.infinispan.commons.marshall.Marshaller marshaller = TestingUtil.extractField(sifsStore, "marshaller");
      org.infinispan.commons.io.ByteBuffer serializedKey = marshaller.objectToBuffer(keyInPreviousFile);
      int segmentId = cache(0, cacheName).getAdvancedCache().getDistributionManager() != null ?
            cache(0, cacheName).getAdvancedCache().getDistributionManager().getCacheTopology().getSegment(keyInPreviousFile) : 0;

      final String finalKey = keyInPreviousFile;
      final Index finalIndex = index;
      final org.infinispan.commons.io.ByteBuffer finalSerializedKey = serializedKey;
      final int finalSegmentId = segmentId;

      eventually("Entry position should be removed from index after direct access", () -> {
         try {
            EntryPosition positionAfter = finalIndex.getPosition(finalKey, finalSegmentId, finalSerializedKey);
            return positionAfter == null;
         } catch (Exception e) {
            return false;
         }
      });
   }
}
