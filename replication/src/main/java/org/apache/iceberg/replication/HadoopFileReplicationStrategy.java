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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.DataChecksum;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hadoop FileSystem-based implementation of FileReplicationStrategy.
 * Uses Hadoop FileSystem API to copy files between different storage systems.
 */
public class HadoopFileReplicationStrategy implements FileReplicationStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopFileReplicationStrategy.class);

  private final Configuration conf;
  private final ExecutorService executorService;
  private final boolean checksumEnabled;
  private final int maxParallelOperations;

  public HadoopFileReplicationStrategy(Configuration conf) {
    this(conf, Runtime.getRuntime().availableProcessors(), true);
  }

  public HadoopFileReplicationStrategy(Configuration conf, int maxParallelOperations,
                                       boolean checksumEnabled) {
    this.conf = conf;
    this.maxParallelOperations = maxParallelOperations;
    this.checksumEnabled = checksumEnabled;

    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("replication-worker-%d")
        .setDaemon(true)
        .build();
    this.executorService = Executors.newFixedThreadPool(maxParallelOperations, threadFactory);
  }

  @Override
  public void copyDataFiles(List<DataFile> dataFiles, String primaryTablePath,
                            String secondaryTablePath) throws ReplicationException {
    LOG.info("Copying {} data files", dataFiles.size());

    try {
      if (supportsParallelOperations() && dataFiles.size() > 1) {
        // Parallel copy for multiple files
        CompletableFuture<?>[] futures = dataFiles.stream()
            .map(dataFile -> CompletableFuture.runAsync(() -> {
              try {
                copyDataFile(dataFile, primaryTablePath, secondaryTablePath);
              } catch (Exception e) {
                throw new RuntimeException("Failed to copy data file: " + dataFile.path(), e);
              }
            }, executorService))
            .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures).join();
      } else {
        // Sequential copy
        for (DataFile dataFile : dataFiles) {
          copyDataFile(dataFile, primaryTablePath, secondaryTablePath);
        }
      }

      LOG.info("Successfully copied {} data files", dataFiles.size());
    } catch (Exception e) {
      throw new ReplicationException.FileOperationException(
          "Failed to copy data files", e);
    }
  }

  @Override
  public void copyDeleteFiles(List<DeleteFile> deleteFiles, String primaryTablePath,
                             String secondaryTablePath) throws ReplicationException {
    LOG.info("Copying {} delete files", deleteFiles.size());

    try {
      if (supportsParallelOperations() && deleteFiles.size() > 1) {
        // Parallel copy for multiple files
        CompletableFuture<?>[] futures = deleteFiles.stream()
            .map(deleteFile -> CompletableFuture.runAsync(() -> {
              try {
                copyDeleteFile(deleteFile, primaryTablePath, secondaryTablePath);
              } catch (Exception e) {
                throw new RuntimeException("Failed to copy delete file: " + deleteFile.path(), e);
              }
            }, executorService))
            .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures).join();
      } else {
        // Sequential copy
        for (DeleteFile deleteFile : deleteFiles) {
          copyDeleteFile(deleteFile, primaryTablePath, secondaryTablePath);
        }
      }

      LOG.info("Successfully copied {} delete files", deleteFiles.size());
    } catch (Exception e) {
      throw new ReplicationException.FileOperationException(
          "Failed to copy delete files", e);
    }
  }

  @Override
  public void copyManifestFiles(List<ManifestFile> manifestFiles, String primaryTablePath,
                                String secondaryTablePath) throws ReplicationException {
    LOG.info("Copying {} manifest files", manifestFiles.size());

    try {
      if (supportsParallelOperations() && manifestFiles.size() > 1) {
        // Parallel copy for multiple files
        CompletableFuture<?>[] futures = manifestFiles.stream()
            .map(manifestFile -> CompletableFuture.runAsync(() -> {
              try {
                copyManifestFile(manifestFile, primaryTablePath, secondaryTablePath);
              } catch (Exception e) {
                throw new RuntimeException("Failed to copy manifest file: " + manifestFile.path(), e);
              }
            }, executorService))
            .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures).join();
      } else {
        // Sequential copy
        for (ManifestFile manifestFile : manifestFiles) {
          copyManifestFile(manifestFile, primaryTablePath, secondaryTablePath);
        }
      }

      LOG.info("Successfully copied {} manifest files", manifestFiles.size());
    } catch (Exception e) {
      throw new ReplicationException.FileOperationException(
          "Failed to copy manifest files", e);
    }
  }

  @Override
  public void copyMetadataFiles(List<String> metadataFiles, String primaryTablePath,
                                String secondaryTablePath) throws ReplicationException {
    LOG.info("Copying {} metadata files", metadataFiles.size());

    try {
      for (String metadataFile : metadataFiles) {
        copyMetadataFile(metadataFile, primaryTablePath, secondaryTablePath);
      }

      LOG.info("Successfully copied {} metadata files", metadataFiles.size());
    } catch (Exception e) {
      throw new ReplicationException.FileOperationException(
          "Failed to copy metadata files", e);
    }
  }

  @Override
  public boolean verifyFile(String filePath, long expectedSize, String expectedChecksum)
      throws ReplicationException {
    try {
      Path path = new Path(filePath);
      FileSystem fs = path.getFileSystem(conf);

      if (!fs.exists(path)) {
        LOG.warn("File does not exist: {}", filePath);
        return false;
      }

      FileStatus status = fs.getFileStatus(path);
      if (status.getLen() != expectedSize) {
        LOG.warn("File size mismatch for {}: expected {}, actual {}",
            filePath, expectedSize, status.getLen());
        return false;
      }

      if (checksumEnabled && expectedChecksum != null) {
        // Note: Checksum verification implementation would depend on the file format
        // For now, we just check size
        LOG.debug("Checksum verification not implemented, using size check only");
      }

      return true;
    } catch (IOException e) {
      throw new ReplicationException.FileOperationException(
          "Failed to verify file: " + filePath, e);
    }
  }

  @Override
  public void deleteFiles(List<String> filePaths) throws ReplicationException {
    LOG.info("Deleting {} files", filePaths.size());

    try {
      for (String filePath : filePaths) {
        Path path = new Path(filePath);
        FileSystem fs = path.getFileSystem(conf);

        if (fs.exists(path)) {
          boolean deleted = fs.delete(path, false);
          if (!deleted) {
            LOG.warn("Failed to delete file: {}", filePath);
          } else {
            LOG.debug("Deleted file: {}", filePath);
          }
        }
      }
    } catch (IOException e) {
      throw new ReplicationException.FileOperationException(
          "Failed to delete files", e);
    }
  }

  @Override
  public boolean supportsParallelOperations() {
    return maxParallelOperations > 1;
  }

  @Override
  public int getMaxParallelOperations() {
    return maxParallelOperations;
  }

  private void copyDataFile(DataFile dataFile, String primaryTablePath,
                            String secondaryTablePath) throws IOException {
    String relativePath = getRelativePath(dataFile.path().toString(), primaryTablePath);
    Path sourcePath = new Path(dataFile.path().toString());
    Path targetPath = new Path(secondaryTablePath, relativePath);

    copyFile(sourcePath, targetPath, dataFile.fileSizeInBytes(), true);
  }

  private void copyDeleteFile(DeleteFile deleteFile, String primaryTablePath,
                             String secondaryTablePath) throws IOException {
    String relativePath = getRelativePath(deleteFile.path().toString(), primaryTablePath);
    Path sourcePath = new Path(deleteFile.path().toString());
    Path targetPath = new Path(secondaryTablePath, relativePath);

    copyFile(sourcePath, targetPath, deleteFile.fileSizeInBytes(), true);
  }

  private void copyManifestFile(ManifestFile manifestFile, String primaryTablePath,
                                String secondaryTablePath) throws IOException {
    String relativePath = getRelativePath(manifestFile.path(), primaryTablePath);
    Path sourcePath = new Path(manifestFile.path());
    Path targetPath = new Path(secondaryTablePath, relativePath);

    copyFile(sourcePath, targetPath, manifestFile.length(), true);
  }

  private void copyMetadataFile(String metadataFile, String primaryTablePath,
                                String secondaryTablePath) throws IOException {
    String relativePath = getRelativePath(metadataFile, primaryTablePath);
    Path sourcePath = new Path(metadataFile);
    Path targetPath = new Path(secondaryTablePath, relativePath);

    // Get file size from source
    FileSystem sourceFs = sourcePath.getFileSystem(conf);
    FileStatus sourceStatus = sourceFs.getFileStatus(sourcePath);

    copyFile(sourcePath, targetPath, sourceStatus.getLen(), false);
  }

  private void copyFile(Path sourcePath, Path targetPath, long expectedSize, boolean forceOverwrite) throws IOException {
    FileSystem sourceFs = sourcePath.getFileSystem(conf);
    FileSystem targetFs = targetPath.getFileSystem(conf);

    // Create target directory if it doesn't exist
    Path targetDir = targetPath.getParent();
    if (!targetFs.exists(targetDir)) {
      targetFs.mkdirs(targetDir);
    }

    // Check if we should overwrite existing file
    if (targetFs.exists(targetPath)) {
      if (forceOverwrite) {
        FileStatus existingStatus = targetFs.getFileStatus(targetPath);
        LOG.info("Force overwriting existing file: {} (current size: {}, expected: {})",
            targetPath, existingStatus.getLen(), expectedSize);
        boolean deleted = targetFs.delete(targetPath, false);
        LOG.info("File deletion result: {}", deleted);

        // Verify deletion
        if (targetFs.exists(targetPath)) {
          LOG.error("File still exists after deletion attempt!");
          throw new IOException("Failed to delete existing file: " + targetPath);
        }
        LOG.info("File successfully deleted, proceeding with copy");
      } else {
        FileStatus targetStatus = targetFs.getFileStatus(targetPath);
        if (targetStatus.getLen() == expectedSize) {
          LOG.debug("File already exists with correct size, skipping: {}", targetPath);
          return;
        } else {
          LOG.debug("File exists but size mismatch, re-copying: {}", targetPath);
          targetFs.delete(targetPath, false);
        }
      }
    }

    // Copy file
    LOG.info("Copying file from {} to {} (expected size: {})", sourcePath, targetPath, expectedSize);
    try {
      // Verify source file size first
      FileStatus sourceStatus = sourceFs.getFileStatus(sourcePath);
      LOG.info("Source file size: {}, expected: {}", sourceStatus.getLen(), expectedSize);

      if (forceOverwrite) {
        LOG.info("Using streaming copy for force overwrite");
        // For force overwrite, use direct streaming copy to ensure clean overwrite
        try (InputStream in = sourceFs.open(sourcePath);
             OutputStream out = targetFs.create(targetPath, true)) { // true = overwrite
          byte[] buffer = new byte[4096];
          int bytesRead;
          long totalBytes = 0;
          while ((bytesRead = in.read(buffer)) != -1) {
            out.write(buffer, 0, bytesRead);
            totalBytes += bytesRead;
          }
          LOG.info("Streamed {} bytes from source to target", totalBytes);
        }
      } else {
        LOG.info("Using FileUtil.copy for regular copy");
        // For regular copy, use FileUtil
        org.apache.hadoop.fs.FileUtil.copy(sourceFs, sourcePath, targetFs, targetPath, false, false, conf);
      }

      // Verify the copy
      FileStatus copiedStatus = targetFs.getFileStatus(targetPath);
      LOG.info("After copy - target file size: {}, expected: {}", copiedStatus.getLen(), expectedSize);
      if (copiedStatus.getLen() != expectedSize) {
        throw new IOException(String.format(Locale.ROOT,
            "Copied file size mismatch: expected %d, actual %d", expectedSize, copiedStatus.getLen()));
      }

      LOG.info("Successfully copied file: {} ({} bytes)", targetPath, expectedSize);
    } catch (IOException e) {
      // Clean up partial file on failure
      if (targetFs.exists(targetPath)) {
        targetFs.delete(targetPath, false);
      }
      throw new IOException("Failed to copy file from " + sourcePath + " to " + targetPath, e);
    }
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
    Path path = new Path(filePath);
    return path.getName();
  }

  public void shutdown() {
    if (executorService != null && !executorService.isShutdown()) {
      executorService.shutdown();
    }
  }
}