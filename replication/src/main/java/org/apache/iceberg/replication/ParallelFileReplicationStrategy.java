/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.replication;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * High-performance file replication strategy that uses parallel operations
 * and optimizations for large-scale replication workloads.
 */
public class ParallelFileReplicationStrategy extends HadoopFileReplicationStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(ParallelFileReplicationStrategy.class);

  private final int batchSize;
  private final ExecutorService dedicatedExecutor;
  private final long timeoutMs;

  public ParallelFileReplicationStrategy(Configuration conf, int maxParallelOperations,
                                         int batchSize, boolean compressionEnabled,
                                         long timeoutMs) {
    super(conf, maxParallelOperations, true);
    this.batchSize = batchSize;
    this.timeoutMs = timeoutMs;

    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("parallel-replication-%d")
        .setDaemon(true)
        .setPriority(Thread.NORM_PRIORITY)
        .build();

    this.dedicatedExecutor = Executors.newFixedThreadPool(maxParallelOperations, threadFactory);
  }

  @Override
  public void copyDataFiles(List<DataFile> dataFiles, String primaryTablePath,
                            String secondaryTablePath) throws ReplicationException {
    LOG.info("Copying {} data files in parallel (batch size: {})", dataFiles.size(), batchSize);

    long startTime = System.currentTimeMillis();

    try {
      if (dataFiles.size() <= batchSize) {
        // Small number of files, use parent implementation
        super.copyDataFiles(dataFiles, primaryTablePath, secondaryTablePath);
      } else {
        // Large number of files, process in batches
        copyDataFilesInBatches(dataFiles, primaryTablePath, secondaryTablePath);
      }

      long duration = System.currentTimeMillis() - startTime;
      LOG.info("Successfully copied {} data files in {}ms ({} files/sec)",
          dataFiles.size(), duration, (dataFiles.size() * 1000.0) / duration);

    } catch (Exception e) {
      throw new ReplicationException.FileOperationException(
          "Failed to copy data files in parallel", e);
    }
  }

  @Override
  public void copyManifestFiles(List<ManifestFile> manifestFiles, String primaryTablePath,
                                String secondaryTablePath) throws ReplicationException {
    LOG.info("Copying {} manifest files in parallel", manifestFiles.size());

    long startTime = System.currentTimeMillis();

    try {
      if (manifestFiles.size() <= batchSize) {
        // Small number of files, use parent implementation
        super.copyManifestFiles(manifestFiles, primaryTablePath, secondaryTablePath);
      } else {
        // Large number of files, process in batches
        copyManifestFilesInBatches(manifestFiles, primaryTablePath, secondaryTablePath);
      }

      long duration = System.currentTimeMillis() - startTime;
      LOG.info("Successfully copied {} manifest files in {}ms",
          manifestFiles.size(), duration);

    } catch (Exception e) {
      throw new ReplicationException.FileOperationException(
          "Failed to copy manifest files in parallel", e);
    }
  }

  private void copyDataFilesInBatches(List<DataFile> dataFiles, String primaryTablePath,
                                      String secondaryTablePath) throws ReplicationException {
    int totalFiles = dataFiles.size();
    int numBatches = (totalFiles + batchSize - 1) / batchSize;

    LOG.debug("Processing {} data files in {} batches", totalFiles, numBatches);

    CompletableFuture<?>[] batchFutures = new CompletableFuture[numBatches];

    for (int i = 0; i < numBatches; i++) {
      final int startIndex = i * batchSize;
      final int endIndex = Math.min(startIndex + batchSize, totalFiles);
      final List<DataFile> batch = dataFiles.subList(startIndex, endIndex);
      final int batchNumber = i + 1;

      batchFutures[i] = CompletableFuture.runAsync(() -> {
        try {
          LOG.debug("Processing batch {}/{} ({} files)", batchNumber, numBatches, batch.size());
          super.copyDataFiles(batch, primaryTablePath, secondaryTablePath);
        } catch (Exception e) {
          throw new RuntimeException("Failed to copy data file batch " + batchNumber, e);
        }
      }, dedicatedExecutor);
    }

    try {
      // Wait for all batches to complete with timeout
      CompletableFuture.allOf(batchFutures)
          .get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new ReplicationException.FileOperationException(
          "Failed to copy data files in batches", e);
    }
  }

  private void copyManifestFilesInBatches(List<ManifestFile> manifestFiles, String primaryTablePath,
                                          String secondaryTablePath) throws ReplicationException {
    int totalFiles = manifestFiles.size();
    int numBatches = (totalFiles + batchSize - 1) / batchSize;

    LOG.debug("Processing {} manifest files in {} batches", totalFiles, numBatches);

    CompletableFuture<?>[] batchFutures = new CompletableFuture[numBatches];

    for (int i = 0; i < numBatches; i++) {
      final int startIndex = i * batchSize;
      final int endIndex = Math.min(startIndex + batchSize, totalFiles);
      final List<ManifestFile> batch = manifestFiles.subList(startIndex, endIndex);
      final int batchNumber = i + 1;

      batchFutures[i] = CompletableFuture.runAsync(() -> {
        try {
          LOG.debug("Processing batch {}/{} ({} files)", batchNumber, numBatches, batch.size());
          super.copyManifestFiles(batch, primaryTablePath, secondaryTablePath);
        } catch (Exception e) {
          throw new RuntimeException("Failed to copy manifest file batch " + batchNumber, e);
        }
      }, dedicatedExecutor);
    }

    try {
      // Wait for all batches to complete with timeout
      CompletableFuture.allOf(batchFutures)
          .get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new ReplicationException.FileOperationException(
          "Failed to copy manifest files in batches", e);
    }
  }

  /**
   * Optimize file copying by skipping files that already exist with correct size.
   */
  public void copyDataFilesWithDeduplication(List<DataFile> dataFiles, String primaryTablePath,
                                             String secondaryTablePath) throws ReplicationException {
    LOG.info("Copying {} data files with deduplication", dataFiles.size());

    // Filter out files that already exist with correct size
    List<DataFile> filesToCopy = dataFiles.stream()
        .filter(file -> {
          try {
            String relativePath = getRelativePath(file.path().toString(), primaryTablePath);
            String targetPath = secondaryTablePath + "/" + relativePath;
            return !verifyFile(targetPath, file.fileSizeInBytes(), null);
          } catch (Exception e) {
            // If we can't verify, copy the file to be safe
            return true;
          }
        })
        .collect(java.util.stream.Collectors.toList());

    LOG.info("Filtered {} files for copying ({} already exist)",
        filesToCopy.size(), dataFiles.size() - filesToCopy.size());

    if (!filesToCopy.isEmpty()) {
      copyDataFiles(filesToCopy, primaryTablePath, secondaryTablePath);
    }
  }

  /**
   * Estimate replication time based on file sizes and historical performance.
   */
  public long estimateReplicationTimeMillis(List<DataFile> dataFiles) {
    long totalBytes = dataFiles.stream()
        .mapToLong(DataFile::fileSizeInBytes)
        .sum();

    // Assume average throughput of 100 MB/s (conservative estimate)
    long estimatedMs = (totalBytes / (100 * 1024 * 1024)) * 1000;

    // Add overhead for parallel operations
    long overhead = (dataFiles.size() / batchSize) * 1000L; // 1 second per batch

    return estimatedMs + overhead;
  }

  private String getRelativePath(String filePath, String basePath) {
    if (filePath.startsWith(basePath)) {
      String relative = filePath.substring(basePath.length());
      if (relative.startsWith("/")) {
        relative = relative.substring(1);
      }
      return relative;
    }
    // If file path doesn't start with base path, extract filename
    return filePath.substring(filePath.lastIndexOf('/') + 1);
  }

  @Override
  public void shutdown() {
    super.shutdown();
    if (dedicatedExecutor != null && !dedicatedExecutor.isShutdown()) {
      dedicatedExecutor.shutdown();
      try {
        if (!dedicatedExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
          dedicatedExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        dedicatedExecutor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Get statistics about the replication performance.
   */
  public static class ReplicationStats {
    private final long totalFiles;
    private final long totalBytes;
    private final long durationMs;
    private final long filesPerSecond;
    private final long bytesPerSecond;

    public ReplicationStats(long totalFiles, long totalBytes, long durationMs) {
      this.totalFiles = totalFiles;
      this.totalBytes = totalBytes;
      this.durationMs = durationMs;
      this.filesPerSecond = durationMs > 0 ? (totalFiles * 1000) / durationMs : 0;
      this.bytesPerSecond = durationMs > 0 ? (totalBytes * 1000) / durationMs : 0;
    }

    public long getTotalFiles() { return totalFiles; }
    public long getTotalBytes() { return totalBytes; }
    public long getDurationMs() { return durationMs; }
    public long getFilesPerSecond() { return filesPerSecond; }
    public long getBytesPerSecond() { return bytesPerSecond; }

    @Override
    public String toString() {
      return String.format(Locale.ROOT, "ReplicationStats{files=%d, bytes=%d, duration=%dms, throughput=%d files/s, %d MB/s}",
          totalFiles, totalBytes, durationMs, filesPerSecond, bytesPerSecond / (1024 * 1024));
    }
  }
}