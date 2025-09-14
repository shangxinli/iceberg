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

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calculates the distance (number of commits) between primary and secondary tables.
 * Provides utilities for determining which commits need to be replicated.
 */
public class CommitDistanceCalculator {

  private static final Logger LOG = LoggerFactory.getLogger(CommitDistanceCalculator.class);

  private final Table primaryTable;
  private final Table secondaryTable;

  public CommitDistanceCalculator(Table primaryTable, Table secondaryTable) {
    this.primaryTable = primaryTable;
    this.secondaryTable = secondaryTable;
  }

  /**
   * Calculate the number of commits that secondary table is behind primary table.
   *
   * @return commit distance
   */
  public long calculateDistance() {
    List<Snapshot> commitsToReplicate = getCommitsToReplicate();
    return commitsToReplicate.size();
  }

  /**
   * Get the list of snapshots that need to be replicated from primary to secondary.
   * Returns snapshots in chronological order (oldest first).
   * Filters out already replicated snapshots for idempotency.
   *
   * @return ordered list of snapshots to replicate
   */
  public List<Snapshot> getCommitsToReplicate() {
    List<Snapshot> primarySnapshots = getAllSnapshots(primaryTable);
    List<Snapshot> secondarySnapshots = getAllSnapshots(secondaryTable);

    // Get list of already replicated snapshot IDs for idempotency
    Set<Long> replicatedSnapshotIds = getReplicatedSnapshotIds();

    if (primarySnapshots.isEmpty()) {
      LOG.debug("Primary table has no snapshots");
      return Lists.newArrayList();
    }

    if (secondarySnapshots.isEmpty()) {
      LOG.debug("Secondary table has no snapshots, need to replicate all {} primary snapshots",
          primarySnapshots.size());
      return primarySnapshots;
    }

    // Find last common snapshot
    Snapshot lastCommonSnapshot = findLastCommonSnapshot(primarySnapshots, secondarySnapshots);

    if (lastCommonSnapshot == null) {
      LOG.warn("No common snapshot found between primary and secondary tables. " +
               "This may indicate data corruption or divergence.");
      // Return all primary snapshots for full replication
      return primarySnapshots;
    }

    // Filter out already replicated snapshots for idempotency
    List<Snapshot> filteredPrimary = primarySnapshots.stream()
        .filter(snapshot -> !replicatedSnapshotIds.contains(snapshot.snapshotId()))
        .collect(Collectors.toList());

    LOG.debug("Filtered {} already replicated snapshots, {} remain",
              primarySnapshots.size() - filteredPrimary.size(), filteredPrimary.size());

    // Get commits in primary after last common snapshot
    List<Snapshot> commitsToReplicate = getCommitsAfter(filteredPrimary, lastCommonSnapshot);

    LOG.debug("Found {} commits to replicate after snapshot {}",
        commitsToReplicate.size(), lastCommonSnapshot.snapshotId());

    return commitsToReplicate;
  }

  /**
   * Check if secondary table is up-to-date with primary table.
   *
   * @return true if tables are in sync
   */
  public boolean isInSync() {
    return calculateDistance() == 0;
  }

  /**
   * Get the last common snapshot between primary and secondary tables.
   *
   * @return last common snapshot or null if no common snapshot found
   */
  public Snapshot getLastCommonSnapshot() {
    List<Snapshot> primarySnapshots = getAllSnapshots(primaryTable);
    List<Snapshot> secondarySnapshots = getAllSnapshots(secondaryTable);
    return findLastCommonSnapshot(primarySnapshots, secondarySnapshots);
  }

  public List<Snapshot> getAllSnapshots(Table table) {
    List<Snapshot> snapshots = Lists.newArrayList();
    for (Snapshot snapshot : table.snapshots()) {
      snapshots.add(snapshot);
    }

    // Sort by timestamp to ensure chronological order
    snapshots.sort(Comparator.comparing(Snapshot::timestampMillis));
    return snapshots;
  }

  private Snapshot findLastCommonSnapshot(List<Snapshot> primarySnapshots,
                                          List<Snapshot> secondarySnapshots) {
    if (primarySnapshots.isEmpty() || secondarySnapshots.isEmpty()) {
      return null;
    }

    // For replicated tables, we can't rely on snapshot IDs since replication creates new snapshots
    // Instead, we'll use timestamp-based matching with some tolerance and summary comparison

    // First try exact snapshot ID matching (for non-replicated scenarios)
    Set<Long> secondarySnapshotIds = secondarySnapshots.stream()
        .map(Snapshot::snapshotId)
        .collect(Collectors.toSet());

    List<Snapshot> reversedPrimary = Lists.reverse(primarySnapshots);
    for (Snapshot primarySnapshot : reversedPrimary) {
      if (secondarySnapshotIds.contains(primarySnapshot.snapshotId())) {
        LOG.debug("Found last common snapshot by ID: {}", primarySnapshot.snapshotId());
        return primarySnapshot;
      }
    }

    // If no exact ID match, try timestamp-based matching for replicated scenarios
    // Look for the latest primary snapshot that has a corresponding secondary snapshot
    // with similar timestamp and compatible summary
    for (Snapshot primarySnapshot : reversedPrimary) {
      for (Snapshot secondarySnapshot : Lists.reverse(secondarySnapshots)) {
        if (isLikelyReplicatedSnapshot(primarySnapshot, secondarySnapshot)) {
          LOG.debug("Found last common snapshot by timestamp match: primary {} -> secondary {}",
              primarySnapshot.snapshotId(), secondarySnapshot.snapshotId());
          return primarySnapshot;
        }
      }
    }

    LOG.debug("No common snapshot found");
    return null;
  }

  /**
   * Check if a secondary snapshot is likely a replica of a primary snapshot.
   * This uses timestamp proximity and summary comparison since snapshot IDs will differ.
   */
  private boolean isLikelyReplicatedSnapshot(Snapshot primarySnapshot, Snapshot secondarySnapshot) {
    // Check timestamp proximity (within reasonable replication window)
    long timeDiff = Math.abs(primarySnapshot.timestampMillis() - secondarySnapshot.timestampMillis());
    long maxTimeDiff = 30000; // 30 seconds tolerance for replication lag

    if (timeDiff > maxTimeDiff) {
      return false;
    }

    // Compare summaries if available
    if (primarySnapshot.summary() != null && secondarySnapshot.summary() != null) {
      // Check key summary fields that should match for replicated data
      String primaryRecords = primarySnapshot.summary().get("total-records");
      String secondaryRecords = secondarySnapshot.summary().get("total-records");
      String primaryFiles = primarySnapshot.summary().get("total-data-files");
      String secondaryFiles = secondarySnapshot.summary().get("total-data-files");

      if (primaryRecords != null && secondaryRecords != null && !primaryRecords.equals(secondaryRecords)) {
        return false;
      }
      if (primaryFiles != null && secondaryFiles != null && !primaryFiles.equals(secondaryFiles)) {
        return false;
      }
    }

    return true;
  }

  private List<Snapshot> getCommitsAfter(List<Snapshot> snapshots, Snapshot afterSnapshot) {
    if (afterSnapshot == null) {
      return snapshots;
    }

    List<Snapshot> result = Lists.newArrayList();
    boolean foundAfterSnapshot = false;

    for (Snapshot snapshot : snapshots) {
      if (foundAfterSnapshot) {
        result.add(snapshot);
      } else if (snapshot.snapshotId() == afterSnapshot.snapshotId()) {
        foundAfterSnapshot = true;
      }
    }

    return result;
  }

  /**
   * Validate that the secondary table has reasonable content compared to primary table.
   * This helps detect data corruption or divergence issues.
   * Note: Secondary snapshots will have different IDs than primary due to replication process.
   *
   * @return true if secondary table appears to be a valid replica
   */
  public boolean validateSecondarySnapshots() {
    List<Snapshot> primarySnapshots = getAllSnapshots(primaryTable);
    List<Snapshot> secondarySnapshots = getAllSnapshots(secondaryTable);

    // If primary is empty, secondary should be empty too
    if (primarySnapshots.isEmpty()) {
      return secondarySnapshots.isEmpty();
    }

    // If secondary is empty but primary isn't, it might be valid (not yet replicated)
    if (secondarySnapshots.isEmpty()) {
      LOG.debug("Secondary table is empty, primary has {} snapshots - may need replication",
          primarySnapshots.size());
      return true;
    }

    // Basic sanity checks:
    // 1. Secondary shouldn't have more snapshots than primary + reasonable buffer
    // 2. Schema should match between tables
    if (secondarySnapshots.size() > primarySnapshots.size() * 2) {
      LOG.error("Secondary table has {} snapshots but primary only has {} - possible divergence",
          secondarySnapshots.size(), primarySnapshots.size());
      return false;
    }

    // Check if schemas match (this is the most important validation)
    try {
      boolean schemasMatch = primaryTable.schema().asStruct().equals(secondaryTable.schema().asStruct());
      if (!schemasMatch) {
        LOG.error("Schema mismatch between primary and secondary tables");
        return false;
      }
    } catch (Exception e) {
      LOG.warn("Could not compare schemas", e);
      // Don't fail validation just because we can't compare schemas
    }

    LOG.debug("Secondary table validation passed: {} snapshots vs {} primary snapshots",
        secondarySnapshots.size(), primarySnapshots.size());
    return true;
  }

  /**
   * Get replication lag in milliseconds based on the oldest unreplicated commit.
   *
   * @return lag in milliseconds, or 0 if tables are in sync
   */
  public long getReplicationLagMillis() {
    List<Snapshot> commitsToReplicate = getCommitsToReplicate();
    if (commitsToReplicate.isEmpty()) {
      return 0;
    }

    // Get the oldest unreplicated commit
    Snapshot oldestUnreplicated = commitsToReplicate.get(0);
    long currentTime = System.currentTimeMillis();
    return currentTime - oldestUnreplicated.timestampMillis();
  }

  /**
   * Get set of already replicated snapshot IDs from secondary table properties.
   * Used for idempotency checking.
   *
   * @return set of replicated snapshot IDs
   */
  private Set<Long> getReplicatedSnapshotIds() {
    String replicatedSnapshotsStr = secondaryTable.properties()
        .get(ReplicationConfiguration.REPLICATED_SNAPSHOTS);

    if (replicatedSnapshotsStr == null || replicatedSnapshotsStr.trim().isEmpty()) {
      return Sets.newHashSet();
    }

    return Stream.of(replicatedSnapshotsStr.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .map(Long::parseLong)
        .collect(Collectors.toSet());
  }
}