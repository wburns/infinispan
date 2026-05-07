package org.infinispan.persistence.sifs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.sifs.SIFSBenchmark")
public class SIFSBenchmark {

   private static final int ENTRY_COUNT = 250_000;
   private static final int VALUE_SIZE = 256;

   public void runBenchmark() throws Exception {
      Path tmpDir = Files.createTempDirectory("sifs-benchmark");
      String dataDir = tmpDir.resolve("data").toString();
      String indexDir = tmpDir.resolve("index").toString();

      try {
         GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
         gcb.globalState().enable().persistentLocation(tmpDir.toString());

         ConfigurationBuilder cb = new ConfigurationBuilder();
         cb.persistence()
               .addSoftIndexFileStore()
               .dataLocation(dataDir)
               .indexLocation(indexDir)
               .purgeOnStartup(true)
               .preload(false)
               .maxFileSize(64 * 1024 * 1024);

         EmbeddedCacheManager cm = new DefaultCacheManager(gcb.build());
         try {
            cm.defineConfiguration("benchmark", cb.build());
            Cache<String, byte[]> cache = cm.getCache("benchmark");
            byte[] value = new byte[VALUE_SIZE];
            for (int i = 0; i < VALUE_SIZE; i++) {
               value[i] = (byte) (i & 0xFF);
            }

            String keyPrefix = "k".repeat(117) + "-";
            long insertStart = System.nanoTime();
            for (int i = 0; i < ENTRY_COUNT; i++) {
               cache.put(String.format("%s%07d", keyPrefix, i), value);
            }
            long insertMs = (System.nanoTime() - insertStart) / 1_000_000;

            long stopStart = System.nanoTime();
            cm.stop();
            long stopMs = (System.nanoTime() - stopStart) / 1_000_000;

            long dataSize = dirSize(Path.of(dataDir));
            long indexSize = dirSize(Path.of(indexDir));

            System.out.println();
            System.out.println("========== SIFS Benchmark Results ==========");
            System.out.printf("Entries:     %,d%n", ENTRY_COUNT);
            System.out.printf("Key size:    %d bytes%n", (keyPrefix + "0000000").length());
            System.out.printf("Value size:  %d bytes%n", VALUE_SIZE);
            System.out.printf("Insert time: %,d ms%n", insertMs);
            System.out.printf("Stop time:   %,d ms%n", stopMs);
            System.out.printf("Data files:  %,d bytes (%.2f MB)%n", dataSize, dataSize / (1024.0 * 1024.0));
            System.out.printf("Index files: %,d bytes (%.2f MB)%n", indexSize, indexSize / (1024.0 * 1024.0));
            System.out.printf("Total:       %,d bytes (%.2f MB)%n", dataSize + indexSize, (dataSize + indexSize) / (1024.0 * 1024.0));
            System.out.println();

            Path indexPath = Path.of(indexDir);
            if (Files.exists(indexPath)) {
               try (Stream<Path> walk = Files.walk(indexPath)) {
                  List<Path> files = walk.filter(Files::isRegularFile).sorted().toList();
                  System.out.printf("Index file count: %d%n", files.size());
                  for (Path f : files) {
                     System.out.printf("  %-40s %,10d bytes%n",
                           indexPath.relativize(f), Files.size(f));
                  }
               }
            }
            System.out.println("============================================");
            System.out.println();
         } finally {
            try {
               cm.stop();
            } catch (Exception ignored) {
            }
         }
      } finally {
         Util.recursiveFileRemove(tmpDir.toFile());
      }
   }

   private static long dirSize(Path dir) throws IOException {
      if (!Files.exists(dir)) return 0;
      try (Stream<Path> walk = Files.walk(dir)) {
         return walk.filter(Files::isRegularFile)
               .mapToLong(p -> {
                  try {
                     return Files.size(p);
                  } catch (IOException e) {
                     return 0;
                  }
               })
               .sum();
      }
   }
}
