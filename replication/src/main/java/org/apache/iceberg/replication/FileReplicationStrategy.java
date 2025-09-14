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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;

/**
 * Strategy interface for replicating files between primary and secondary table locations.
 * Implementations handle the details of copying different types of files.
 */
public interface FileReplicationStrategy {

  /**
   * Copy data files from primary to secondary location.
   *
   * @param dataFiles list of data files to copy
   * @param primaryTablePath path of the primary table
   * @param secondaryTablePath path of the secondary table
   * @throws ReplicationException if file copying fails
   */
  void copyDataFiles(List<DataFile> dataFiles, String primaryTablePath, String secondaryTablePath)
      throws ReplicationException;

  /**
   * Copy delete files from primary to secondary location.
   *
   * @param deleteFiles list of delete files to copy
   * @param primaryTablePath path of the primary table
   * @param secondaryTablePath path of the secondary table
   * @throws ReplicationException if file copying fails
   */
  void copyDeleteFiles(List<DeleteFile> deleteFiles, String primaryTablePath, String secondaryTablePath)
      throws ReplicationException;

  /**
   * Copy manifest files from primary to secondary location.
   *
   * @param manifestFiles list of manifest files to copy
   * @param primaryTablePath path of the primary table
   * @param secondaryTablePath path of the secondary table
   * @throws ReplicationException if file copying fails
   */
  void copyManifestFiles(List<ManifestFile> manifestFiles, String primaryTablePath,
                         String secondaryTablePath) throws ReplicationException;

  /**
   * Copy metadata files from primary to secondary location.
   *
   * @param metadataFiles list of metadata file paths to copy
   * @param primaryTablePath path of the primary table
   * @param secondaryTablePath path of the secondary table
   * @throws ReplicationException if file copying fails
   */
  void copyMetadataFiles(List<String> metadataFiles, String primaryTablePath,
                         String secondaryTablePath) throws ReplicationException;

  /**
   * Verify that a file exists and has the expected size and checksum.
   *
   * @param filePath path to the file to verify
   * @param expectedSize expected file size in bytes
   * @param expectedChecksum expected file checksum (optional)
   * @return true if file is valid
   * @throws ReplicationException if verification fails
   */
  boolean verifyFile(String filePath, long expectedSize, String expectedChecksum)
      throws ReplicationException;

  /**
   * Delete files that should be removed during replication.
   *
   * @param filePaths list of file paths to delete
   * @throws ReplicationException if deletion fails
   */
  void deleteFiles(List<String> filePaths) throws ReplicationException;

  /**
   * Check if the strategy supports parallel file operations.
   *
   * @return true if parallel operations are supported
   */
  boolean supportsParallelOperations();

  /**
   * Get the maximum number of parallel operations supported.
   *
   * @return maximum parallel operations, or 1 if parallel operations not supported
   */
  int getMaxParallelOperations();
}