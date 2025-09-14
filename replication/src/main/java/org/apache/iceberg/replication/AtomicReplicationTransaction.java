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
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.iceberg.relocated.com.google.common.collect.Maps;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements atomic replication of commits from primary to secondary table.
 * Uses all-or-nothing semantics - if any part of replication fails,
 * the entire operation is rolled back.
 */
public class AtomicReplicationTransaction {

  private static final Logger LOG = LoggerFactory.getLogger(AtomicReplicationTransaction.class);

  private final Table primaryTable;
  private final Table secondaryTable;
  private final List<Snapshot> commitsToReplicate;
  private final FileReplicationStrategy fileReplicationStrategy;
  private final ReplicationMetadata replicationMetadata;

  // Tracking for rollback
  private final List<String> copiedFiles = Lists.newArrayList();
  private final List<Long> createdSnapshots = Lists.newArrayList();
  private Transaction secondaryTransaction;

  public AtomicReplicationTransaction(Table primaryTable, Table secondaryTable,
                                      List<Snapshot> commitsToReplicate,
                                      FileReplicationStrategy fileReplicationStrategy) {
    this.primaryTable = primaryTable;
    this.secondaryTable = secondaryTable;
    this.commitsToReplicate = commitsToReplicate;
    this.fileReplicationStrategy = fileReplicationStrategy;
    this.replicationMetadata = new ReplicationMetadata(secondaryTable);
  }

  /**
   * Execute the atomic replication transaction.
   * Either all commits are replicated successfully, or none are.
   *
   * @throws ReplicationException if replication fails
   */
  public void execute() throws ReplicationException {
    LOG.info("Starting atomic replication of {} commits", commitsToReplicate.size());

    if (commitsToReplicate.isEmpty()) {
      LOG.info("No commits to replicate");
      return;
    }

    try {
      beginReplication();

      for (Snapshot snapshot : commitsToReplicate) {
        replicateSnapshot(snapshot);
      }

      commitReplication();
      LOG.info("Successfully replicated {} commits", commitsToReplicate.size());

    } catch (Exception e) {
      LOG.error("Replication failed, rolling back", e);
      rollbackReplication();
      throw new ReplicationException.TransactionException("Replication failed and was rolled back", e);
    }
  }

  private void beginReplication() throws ReplicationException {
    LOG.debug("Beginning replication transaction");

    // Start transaction on secondary table
    secondaryTransaction = secondaryTable.newTransaction();

    // Update replication status
    replicationMetadata.setStatus(ReplicationStatus.RUNNING);
    replicationMetadata.setLastReplicationTimestamp(System.currentTimeMillis());
  }

  private void replicateSnapshot(Snapshot snapshot) throws ReplicationException {
    LOG.debug("Replicating snapshot: {}", snapshot.snapshotId());

    try {
      // 1. Replicate schema changes if any
      replicateSchemaChanges(snapshot);

      // 2. Replicate partition spec changes if any
      replicatePartitionSpecChanges(snapshot);

      // 3. Replicate sort order changes if any
      replicateSortOrderChanges(snapshot);

      // 4. Copy data files
      copyDataFiles(snapshot);

      // 5. Copy delete files
      copyDeleteFiles(snapshot);

      // 6. Copy manifest files
      copyManifestFiles(snapshot);

      // 7. Create snapshot in secondary table
      createSecondarySnapshot(snapshot);

      // 8. Update table properties
      replicateTableProperties(snapshot);

      createdSnapshots.add(snapshot.snapshotId());

      // Track replicated snapshot for idempotency
      trackReplicatedSnapshot(snapshot.snapshotId());

    } catch (Exception e) {
      throw new ReplicationException.TransactionException(
          "Failed to replicate snapshot: " + snapshot.snapshotId(), e);
    }
  }

  private void replicateSchemaChanges(Snapshot snapshot) throws ReplicationException {
    Schema primarySchema = primaryTable.schema();
    Schema secondarySchema = secondaryTable.schema();

    if (!primarySchema.asStruct().equals(secondarySchema.asStruct())) {
      LOG.debug("Replicating schema changes for snapshot: {}", snapshot.snapshotId());

      UpdateSchema updateSchema = secondaryTransaction.updateSchema();

      // For simplicity, we'll replace the entire schema
      // In a production implementation, you'd want to apply incremental changes
      updateSchema.unionByNameWith(primarySchema);
      updateSchema.commit();

      LOG.debug("Schema replicated successfully");
    }
  }

  private void replicatePartitionSpecChanges(Snapshot snapshot) throws ReplicationException {
    PartitionSpec primarySpec = primaryTable.spec();
    PartitionSpec secondarySpec = secondaryTable.spec();

    if (!primarySpec.equals(secondarySpec)) {
      LOG.debug("Replicating partition spec changes for snapshot: {}", snapshot.snapshotId());

      UpdatePartitionSpec updateSpec = secondaryTransaction.updateSpec();

      // For simplicity, replace with primary spec
      // In production, you'd apply incremental changes
      for (int i = 0; i < primarySpec.fields().size(); i++) {
        PartitionField field = primarySpec.fields().get(i);
        if (i >= secondarySpec.fields().size()) {
          String columnName = primaryTable.schema().findColumnName(field.sourceId());
          updateSpec.addField(field.name(), Expressions.transform(columnName, field.transform()));
        }
      }

      updateSpec.commit();
      LOG.debug("Partition spec replicated successfully");
    }
  }

  private void replicateSortOrderChanges(Snapshot snapshot) throws ReplicationException {
    SortOrder primarySortOrder = primaryTable.sortOrder();
    SortOrder secondarySortOrder = secondaryTable.sortOrder();

    if (!primarySortOrder.equals(secondarySortOrder)) {
      LOG.debug("Replicating sort order changes for snapshot: {}", snapshot.snapshotId());

      ReplaceSortOrder replaceSortOrder = secondaryTransaction.replaceSortOrder();

      // Replace with primary sort order
      primarySortOrder.fields().forEach(field -> {
        String columnName = primaryTable.schema().findColumnName(field.sourceId());
        if (field.direction() == org.apache.iceberg.SortDirection.ASC) {
          replaceSortOrder.asc(columnName);
        } else {
          replaceSortOrder.desc(columnName);
        }
      });

      replaceSortOrder.commit();
      LOG.debug("Sort order replicated successfully");
    }
  }

  private void copyDataFiles(Snapshot snapshot) throws ReplicationException {
    LOG.debug("Copying data files for snapshot: {}", snapshot.snapshotId());

    List<DataFile> dataFiles = Lists.newArrayList(snapshot.addedDataFiles(primaryTable.io()));
    if (!dataFiles.isEmpty()) {
      String primaryTablePath = primaryTable.location();
      String secondaryTablePath = secondaryTable.location();

      fileReplicationStrategy.copyDataFiles(dataFiles, primaryTablePath, secondaryTablePath);

      // Track copied files for potential rollback
      dataFiles.forEach(file -> copiedFiles.add(file.path().toString()));
    }

    LOG.debug("Copied {} data files", dataFiles.size());
  }

  private void copyDeleteFiles(Snapshot snapshot) throws ReplicationException {
    LOG.debug("Copying delete files for snapshot: {}", snapshot.snapshotId());

    List<DeleteFile> addedDeleteFiles = Lists.newArrayList(snapshot.addedDeleteFiles(primaryTable.io()));
    if (!addedDeleteFiles.isEmpty()) {
      String primaryTablePath = primaryTable.location();
      String secondaryTablePath = secondaryTable.location();

      fileReplicationStrategy.copyDeleteFiles(addedDeleteFiles, primaryTablePath, secondaryTablePath);

      // Track copied files for potential rollback
      addedDeleteFiles.forEach(file -> copiedFiles.add(file.path().toString()));
    }

    LOG.debug("Copied {} delete files", addedDeleteFiles.size());
  }

  private void copyManifestFiles(Snapshot snapshot) throws ReplicationException {
    LOG.debug("Copying manifest files for snapshot: {}", snapshot.snapshotId());

    List<ManifestFile> manifestFiles = snapshot.allManifests(primaryTable.io());
    if (!manifestFiles.isEmpty()) {
      String primaryTablePath = primaryTable.location();
      String secondaryTablePath = secondaryTable.location();

      fileReplicationStrategy.copyManifestFiles(manifestFiles, primaryTablePath, secondaryTablePath);

      // Track copied files for potential rollback
      manifestFiles.forEach(file -> copiedFiles.add(file.path()));
    }

    LOG.debug("Copied {} manifest files", manifestFiles.size());
  }

  private void createSecondarySnapshot(Snapshot snapshot) throws ReplicationException {
    LOG.debug("Creating snapshot in secondary table: {}", snapshot.snapshotId());

    try {
      // Detect operation type from snapshot summary
      String operationType = detectOperationType(snapshot);
      LOG.info("REPL_DEBUG: Detected operation type: {}", operationType);

      // Get the files that were added/removed in this snapshot
      List<DataFile> addedFiles = Lists.newArrayList(snapshot.addedDataFiles(primaryTable.io()));
      List<DataFile> deletedFiles = Lists.newArrayList(snapshot.removedDataFiles(primaryTable.io()));
      List<DeleteFile> addedDeleteFiles = Lists.newArrayList(snapshot.addedDeleteFiles(primaryTable.io()));
      List<DeleteFile> removedDeleteFiles = Lists.newArrayList(snapshot.removedDeleteFiles(primaryTable.io()));

      // Execute operation based on type
      switch (operationType.toLowerCase(Locale.ROOT)) {
        case "append":
          executeAppendOperation(addedFiles, addedDeleteFiles, snapshot);
          break;
        case "overwrite":
          executeOverwriteOperation(addedFiles, deletedFiles, addedDeleteFiles, removedDeleteFiles, snapshot);
          break;
        case "delete":
          executeDeleteOperation(deletedFiles, removedDeleteFiles, snapshot);
          break;
        default:
          // Fallback to legacy behavior for unknown operations
          LOG.warn("Unknown operation type '{}', falling back to legacy file-by-file approach", operationType);
          executeLegacyOperation(addedFiles, deletedFiles, addedDeleteFiles, removedDeleteFiles);
          break;
      }

    } catch (Exception e) {
      throw new ReplicationException.TransactionException(
          "Failed to create snapshot in secondary table", e);
    }
  }

  private String detectOperationType(Snapshot snapshot) {
    if (snapshot.summary() != null) {
      // Check for explicit operation type in summary
      String operation = snapshot.summary().get("operation");
      if (operation != null) {
        return operation;
      }
    }

    // Fallback to heuristic-based detection
    List<DataFile> addedFiles = Lists.newArrayList(snapshot.addedDataFiles(primaryTable.io()));
    List<DataFile> deletedFiles = Lists.newArrayList(snapshot.removedDataFiles(primaryTable.io()));

    if (!addedFiles.isEmpty() && !deletedFiles.isEmpty()) {
      return "overwrite";
    } else if (!addedFiles.isEmpty()) {
      return "append";
    } else if (!deletedFiles.isEmpty()) {
      return "delete";
    }

    return "unknown";
  }

  private void executeAppendOperation(List<DataFile> addedFiles, List<DeleteFile> addedDeleteFiles, Snapshot originalSnapshot) {
    if (!addedFiles.isEmpty() || !addedDeleteFiles.isEmpty()) {
      LOG.info("REPL_DEBUG: Executing append operation for {} data files and {} delete files",
               addedFiles.size(), addedDeleteFiles.size());
      AppendFiles append = secondaryTransaction.newAppend();
      addedFiles.forEach(append::appendFile);
      // Note: Delete files are typically managed through RowDelta or separate transactions
      // For now, we'll handle them through a separate append operation if needed
      if (!addedDeleteFiles.isEmpty()) {
        LOG.warn("REPL_DEBUG: Delete files found in append operation, handling separately");
        // TODO: Implement proper delete file handling via RowDelta
      }

      // Preserve original operation metadata
      preserveSnapshotSummary(append, originalSnapshot);

      append.commit();
      LOG.info("REPL_DEBUG: Append operation completed successfully");
    }
  }

  private void executeOverwriteOperation(List<DataFile> addedFiles, List<DataFile> deletedFiles,
                                        List<DeleteFile> addedDeleteFiles, List<DeleteFile> removedDeleteFiles, Snapshot originalSnapshot) {
    LOG.info("REPL_DEBUG: Executing overwrite operation for {} added, {} deleted data files and {} added, {} removed delete files",
             addedFiles.size(), deletedFiles.size(), addedDeleteFiles.size(), removedDeleteFiles.size());

    OverwriteFiles overwrite = secondaryTransaction.newOverwrite();

    // Add new files
    addedFiles.forEach(overwrite::addFile);
    // Note: Delete files need special handling in OverwriteFiles
    if (!addedDeleteFiles.isEmpty()) {
      LOG.warn("REPL_DEBUG: Delete files found in overwrite operation, need special handling");
      // TODO: Implement proper delete file handling
    }

    // Mark old files for deletion
    deletedFiles.forEach(overwrite::deleteFile);
    if (!removedDeleteFiles.isEmpty()) {
      LOG.warn("REPL_DEBUG: Removed delete files found, need special handling");
      // TODO: Implement proper delete file removal
    }

    // Preserve original operation metadata
    preserveSnapshotSummary(overwrite, originalSnapshot);

    overwrite.commit();
    LOG.info("REPL_DEBUG: Overwrite operation completed successfully");
  }

  private void executeDeleteOperation(List<DataFile> deletedFiles, List<DeleteFile> removedDeleteFiles, Snapshot originalSnapshot) {
    if (!deletedFiles.isEmpty() || !removedDeleteFiles.isEmpty()) {
      LOG.info("REPL_DEBUG: Executing delete operation for {} data files and {} delete files",
               deletedFiles.size(), removedDeleteFiles.size());
      DeleteFiles delete = secondaryTransaction.newDelete();
      deletedFiles.forEach(delete::deleteFile);
      if (!removedDeleteFiles.isEmpty()) {
        LOG.warn("REPL_DEBUG: Removed delete files found, need special handling for delete operation");
        // TODO: Implement proper delete file removal
      }

      // Preserve original operation metadata
      preserveSnapshotSummary(delete, originalSnapshot);

      delete.commit();
      LOG.info("REPL_DEBUG: Delete operation completed successfully");
    }
  }

  private void executeLegacyOperation(List<DataFile> addedFiles, List<DataFile> deletedFiles,
                                     List<DeleteFile> addedDeleteFiles, List<DeleteFile> removedDeleteFiles) {
    // This is the original file-by-file approach for backward compatibility
    if (!addedFiles.isEmpty()) {
      LOG.info("REPL_DEBUG: Legacy: Adding {} data files", addedFiles.size());
      AppendFiles append = secondaryTransaction.newAppend();
      addedFiles.forEach(append::appendFile);
      append.commit();
    }

    if (!addedDeleteFiles.isEmpty()) {
      LOG.warn("REPL_DEBUG: Legacy: Found {} delete files, skipping for now", addedDeleteFiles.size());
      // TODO: Implement proper delete file handling
    }

    if (!deletedFiles.isEmpty()) {
      LOG.info("REPL_DEBUG: Legacy: Removing {} data files", deletedFiles.size());
      DeleteFiles delete = secondaryTransaction.newDelete();
      deletedFiles.forEach(delete::deleteFile);
      delete.commit();
    }

    if (!removedDeleteFiles.isEmpty()) {
      LOG.warn("REPL_DEBUG: Legacy: Found {} removed delete files, skipping for now", removedDeleteFiles.size());
      // TODO: Implement proper delete file removal
    }
  }

  private void replicateTableProperties(Snapshot snapshot) throws ReplicationException {
    LOG.debug("Replicating table properties for snapshot: {}", snapshot.snapshotId());

    // Replicate non-replication-specific properties
    Set<String> replicationKeys = Sets.newHashSet(
        ReplicationConfiguration.REPLICATION_ENABLED,
        ReplicationConfiguration.SECONDARY_TABLE_PATH,
        ReplicationConfiguration.LAST_REPLICATED_SNAPSHOT_ID,
        ReplicationConfiguration.LAST_REPLICATION_TIMESTAMP,
        ReplicationConfiguration.REPLICATION_STATUS,
        ReplicationConfiguration.REPLICATED_SNAPSHOTS,
        ReplicationConfiguration.SOURCE_TABLE_UUID
    );

    boolean hasUpdates = false;
    UpdateProperties updateProperties = null;

    for (Map.Entry<String, String> entry : primaryTable.properties().entrySet()) {
      if (!replicationKeys.contains(entry.getKey())) {
        String currentValue = secondaryTable.properties().get(entry.getKey());
        if (!entry.getValue().equals(currentValue)) {
          if (updateProperties == null) {
            updateProperties = secondaryTransaction.updateProperties();
          }
          updateProperties.set(entry.getKey(), entry.getValue());
          hasUpdates = true;
        }
      }
    }

    if (hasUpdates && updateProperties != null) {
      updateProperties.commit();
      LOG.debug("Table properties replicated successfully");
    }
  }

  private void trackReplicatedSnapshot(long snapshotId) {
    // Get existing replicated snapshots
    String existingIds = secondaryTable.properties()
        .get(ReplicationConfiguration.REPLICATED_SNAPSHOTS);

    Set<String> replicatedIds = Sets.newHashSet();
    if (existingIds != null && !existingIds.trim().isEmpty()) {
      replicatedIds.addAll(Lists.newArrayList(existingIds.split(",")));
    }

    // Add new snapshot ID
    replicatedIds.add(String.valueOf(snapshotId));

    // Keep only recent snapshots to avoid property bloat (last 100)
    if (replicatedIds.size() > 100) {
      List<String> sortedIds = replicatedIds.stream()
          .sorted((a, b) -> Long.compare(Long.parseLong(b), Long.parseLong(a)))
          .collect(Collectors.toList());
      replicatedIds = Sets.newHashSet(sortedIds.subList(0, 100));
    }

    // Update secondary table properties in the transaction
    UpdateProperties updateProps = secondaryTransaction.updateProperties();
    updateProps.set(ReplicationConfiguration.REPLICATED_SNAPSHOTS,
                    String.join(",", replicatedIds));
    updateProps.commit();

    LOG.debug("Tracked replicated snapshot: {}", snapshotId);
  }

  private void preserveSnapshotSummary(Object operation, Snapshot originalSnapshot) {
    // Add replication markers to the operation - let Iceberg generate its own operational summary
    // to avoid conflicts with operational fields like "added-files-size"

    if (operation instanceof AppendFiles) {
      AppendFiles append = (AppendFiles) operation;
      append.set("replication.source-snapshot-id", String.valueOf(originalSnapshot.snapshotId()));
      append.set("replication.source-timestamp", String.valueOf(originalSnapshot.timestampMillis()));
      append.set("replication.replicated-at", String.valueOf(System.currentTimeMillis()));
    } else if (operation instanceof OverwriteFiles) {
      OverwriteFiles overwrite = (OverwriteFiles) operation;
      overwrite.set("replication.source-snapshot-id", String.valueOf(originalSnapshot.snapshotId()));
      overwrite.set("replication.source-timestamp", String.valueOf(originalSnapshot.timestampMillis()));
      overwrite.set("replication.replicated-at", String.valueOf(System.currentTimeMillis()));
    } else if (operation instanceof DeleteFiles) {
      DeleteFiles delete = (DeleteFiles) operation;
      delete.set("replication.source-snapshot-id", String.valueOf(originalSnapshot.snapshotId()));
      delete.set("replication.source-timestamp", String.valueOf(originalSnapshot.timestampMillis()));
      delete.set("replication.replicated-at", String.valueOf(System.currentTimeMillis()));
    }

    LOG.debug("Added replication markers to operation");
  }

  private void commitReplication() throws ReplicationException {
    LOG.info("REPL_DEBUG: Attempting to commit replication transaction");

    try {
      // Commit the transaction to make all changes visible
      LOG.info("REPL_DEBUG: About to call commitTransaction()");
      secondaryTransaction.commitTransaction();
      LOG.info("REPL_DEBUG: Successfully committed transaction");

      // Update replication metadata after transaction is committed
      if (!commitsToReplicate.isEmpty()) {
        Snapshot lastReplicated = commitsToReplicate.get(commitsToReplicate.size() - 1);
        replicationMetadata.setLastReplicatedSnapshotId(lastReplicated.snapshotId());
      }

      replicationMetadata.setStatus(ReplicationStatus.SUCCESS);
      replicationMetadata.commit();

      LOG.debug("Replication transaction committed successfully");

    } catch (Exception e) {
      throw new ReplicationException.TransactionException("Failed to commit replication", e);
    }
  }

  private void rollbackReplication() {
    LOG.warn("Rolling back replication transaction");

    try {
      // The transaction system will automatically handle rollback of
      // uncommitted changes when the transaction is abandoned

      // Clean up any copied files
      if (!copiedFiles.isEmpty()) {
        LOG.debug("Cleaning up {} copied files", copiedFiles.size());
        try {
          fileReplicationStrategy.deleteFiles(copiedFiles);
        } catch (Exception e) {
          LOG.error("Failed to clean up copied files during rollback", e);
        }
      }

      // Update replication status
      try {
        replicationMetadata.setStatus(ReplicationStatus.FAILED);
        replicationMetadata.commit();
      } catch (Exception e) {
        LOG.error("Failed to update replication status during rollback", e);
      }

    } catch (Exception e) {
      LOG.error("Error during rollback", e);
    }
  }
}