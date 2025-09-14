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

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of TableReplicationManager.
 * Provides basic replication functionality using Hadoop FileSystem for file operations.
 */
public class DefaultTableReplicationManager implements TableReplicationManager {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultTableReplicationManager.class);

  private final Table primaryTable;
  private Table secondaryTable;
  private FileReplicationStrategy fileReplicationStrategy;
  private CommitDistanceCalculator distanceCalculator;
  private Configuration configuration;
  private ReplicationStatus currentStatus = ReplicationStatus.DISABLED;

  public DefaultTableReplicationManager(Table primaryTable) {
    this.primaryTable = primaryTable;
  }

  @Override
  public void enableReplication(String secondaryTablePath, Configuration conf)
      throws ReplicationException {
    LOG.info("Enabling replication for table {} to secondary path: {}",
        primaryTable.name(), secondaryTablePath);

    try {
      this.configuration = conf;

      // Initialize file replication strategy
      this.fileReplicationStrategy = new HadoopFileReplicationStrategy(conf);

      // Get or create secondary table
      this.secondaryTable = getOrCreateSecondaryTable(secondaryTablePath);

      // Initialize distance calculator
      this.distanceCalculator = new CommitDistanceCalculator(primaryTable, secondaryTable);


      // Update primary table properties
      UpdateProperties updateProps = primaryTable.updateProperties();
      updateProps.set(TableProperties.REPLICATION_ENABLED, "true");
      updateProps.set(TableProperties.REPLICATION_TARGET, secondaryTablePath);
      updateProps.commit();

      this.currentStatus = ReplicationStatus.IDLE;

      LOG.info("Replication enabled successfully");

    } catch (Exception e) {
      this.currentStatus = ReplicationStatus.FAILED;
      throw new ReplicationException.ConfigurationException(
          "Failed to enable replication", e);
    }
  }

  @Override
  public void disableReplication() throws ReplicationException {
    LOG.info("Disabling replication for table: {}", primaryTable.name());

    try {
      // Update primary table properties
      UpdateProperties updateProps = primaryTable.updateProperties();
      updateProps.remove(TableProperties.REPLICATION_ENABLED);
      updateProps.remove(TableProperties.REPLICATION_TARGET);
      updateProps.commit();

      // Clean up resources
      if (fileReplicationStrategy instanceof HadoopFileReplicationStrategy) {
        ((HadoopFileReplicationStrategy) fileReplicationStrategy).shutdown();
      }

      this.currentStatus = ReplicationStatus.DISABLED;
      this.secondaryTable = null;
      this.fileReplicationStrategy = null;
      this.distanceCalculator = null;

      LOG.info("Replication disabled successfully");

    } catch (Exception e) {
      throw new ReplicationException.ConfigurationException(
          "Failed to disable replication", e);
    }
  }

  @Override
  public void startReplication() throws ReplicationException {
    if (!isReplicationEnabled()) {
      throw new ReplicationException.ConfigurationException("Replication is not enabled");
    }

    LOG.info("Starting replication for table: {}", primaryTable.name());

    try {
      this.currentStatus = ReplicationStatus.RUNNING;

      // Get commits that need to be replicated
      List<Snapshot> commitsToReplicate = distanceCalculator.getCommitsToReplicate();

      if (commitsToReplicate.isEmpty()) {
        LOG.info("No commits to replicate, tables are in sync");
        this.currentStatus = ReplicationStatus.SUCCESS;
        return;
      }

      LOG.info("Replicating {} commits", commitsToReplicate.size());

      // Execute atomic replication transaction
      AtomicReplicationTransaction transaction = new AtomicReplicationTransaction(
          primaryTable, secondaryTable, commitsToReplicate, fileReplicationStrategy);

      transaction.execute();

      // Refresh secondary table reference to see latest changes
      this.secondaryTable.refresh();

      // Recreate distance calculator with refreshed table
      this.distanceCalculator = new CommitDistanceCalculator(primaryTable, secondaryTable);

      this.currentStatus = ReplicationStatus.SUCCESS;
      LOG.info("Replication completed successfully");

    } catch (Exception e) {
      this.currentStatus = ReplicationStatus.FAILED;
      LOG.error("Replication failed", e);
      throw new ReplicationException("Replication failed", e);
    }
  }

  @Override
  public ReplicationStatus getReplicationStatus() {
    return currentStatus;
  }

  @Override
  public long getCommitDistance() throws ReplicationException {
    if (!isReplicationEnabled()) {
      throw new ReplicationException.ConfigurationException("Replication is not enabled");
    }

    try {
      return distanceCalculator.calculateDistance();
    } catch (Exception e) {
      throw new ReplicationException("Failed to calculate commit distance", e);
    }
  }

  @Override
  public boolean isReplicationEnabled() {
    String enabled = primaryTable.properties().get(TableProperties.REPLICATION_ENABLED);
    return "true".equals(enabled) && secondaryTable != null;
  }

  @Override
  public Table getSecondaryTable() {
    return secondaryTable;
  }

  @Override
  public void forceFullReplication() throws ReplicationException {
    if (!isReplicationEnabled()) {
      throw new ReplicationException.ConfigurationException("Replication is not enabled");
    }

    LOG.info("Starting force full replication for table: {}", primaryTable.name());

    try {
      this.currentStatus = ReplicationStatus.RUNNING;

      // Get all commits from primary table
      List<Snapshot> allCommits = distanceCalculator.getAllSnapshots(primaryTable);

      if (allCommits.isEmpty()) {
        LOG.info("No commits to replicate");
        this.currentStatus = ReplicationStatus.SUCCESS;
        return;
      }

      LOG.info("Force replicating {} commits", allCommits.size());

      // Execute atomic replication transaction
      AtomicReplicationTransaction transaction = new AtomicReplicationTransaction(
          primaryTable, secondaryTable, allCommits, fileReplicationStrategy);

      transaction.execute();

      this.currentStatus = ReplicationStatus.SUCCESS;
      LOG.info("Force full replication completed successfully");

    } catch (Exception e) {
      this.currentStatus = ReplicationStatus.FAILED;
      LOG.error("Force full replication failed", e);
      throw new ReplicationException("Force full replication failed", e);
    }
  }

  private Table getOrCreateSecondaryTable(String secondaryTablePath) throws ReplicationException {
    try {
      HadoopTables tables = new HadoopTables(configuration);

      // Try to load existing table
      try {
        Table existing = tables.load(secondaryTablePath);
        LOG.info("Loaded existing secondary table: {}", secondaryTablePath);
        return existing;
      } catch (Exception e) {
        // Table doesn't exist, create it
        LOG.info("Creating new secondary table: {}", secondaryTablePath, e);
        return tables.create(primaryTable.schema(), primaryTable.spec(),
            primaryTable.sortOrder(), primaryTable.properties(), secondaryTablePath);
      }

    } catch (Exception e) {
      throw new ReplicationException.ConfigurationException(
          "Failed to get or create secondary table", e);
    }
  }

  /**
   * Get replication lag in milliseconds.
   *
   * @return lag in milliseconds
   * @throws ReplicationException if lag cannot be calculated
   */
  public long getReplicationLagMillis() throws ReplicationException {
    if (!isReplicationEnabled()) {
      throw new ReplicationException.ConfigurationException("Replication is not enabled");
    }

    try {
      return distanceCalculator.getReplicationLagMillis();
    } catch (Exception e) {
      throw new ReplicationException("Failed to calculate replication lag", e);
    }
  }

  /**
   * Check if tables are in sync.
   *
   * @return true if tables are in sync
   * @throws ReplicationException if sync status cannot be determined
   */
  public boolean isInSync() throws ReplicationException {
    if (!isReplicationEnabled()) {
      return false;
    }

    try {
      return distanceCalculator.isInSync();
    } catch (Exception e) {
      throw new ReplicationException("Failed to check sync status", e);
    }
  }

  /**
   * Validate secondary table integrity.
   *
   * @return true if secondary table is valid
   * @throws ReplicationException if validation fails
   */
  public boolean validateSecondaryTable() throws ReplicationException {
    if (!isReplicationEnabled()) {
      throw new ReplicationException.ConfigurationException("Replication is not enabled");
    }

    try {
      return distanceCalculator.validateSecondarySnapshots();
    } catch (Exception e) {
      throw new ReplicationException("Failed to validate secondary table", e);
    }
  }
}