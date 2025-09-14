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

import java.util.Map;

import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateProperties;

/**
 * Manages replication metadata stored in table properties.
 * Tracks the last replicated snapshot ID, replication timestamp, and status.
 */
public class ReplicationMetadata {

  private final Table table;
  private long lastReplicatedSnapshotId;
  private long lastReplicationTimestamp;
  private ReplicationStatus status;

  public ReplicationMetadata(Table table) {
    this.table = table;
    loadFromTableProperties();
  }

  /**
   * Load replication metadata from table properties.
   */
  private void loadFromTableProperties() {
    Map<String, String> properties = table.properties();

    // Load last replicated snapshot ID
    String lastSnapshotId = properties.get(ReplicationConfiguration.LAST_REPLICATED_SNAPSHOT_ID);
    this.lastReplicatedSnapshotId = lastSnapshotId != null ? Long.parseLong(lastSnapshotId) : 0L;

    // Load last replication timestamp
    String lastTimestamp = properties.get(ReplicationConfiguration.LAST_REPLICATION_TIMESTAMP);
    this.lastReplicationTimestamp = lastTimestamp != null ? Long.parseLong(lastTimestamp) : 0L;

    // Load replication status
    String statusStr = properties.get(ReplicationConfiguration.REPLICATION_STATUS);
    this.status = statusStr != null ? ReplicationStatus.valueOf(statusStr) : ReplicationStatus.IDLE;
  }

  /**
   * Commit the current metadata state to table properties.
   *
   * @throws ReplicationException if the update fails
   */
  public void commit() throws ReplicationException {
    try {
      UpdateProperties updateProperties = table.updateProperties();

      updateProperties.set(ReplicationConfiguration.LAST_REPLICATED_SNAPSHOT_ID,
          String.valueOf(lastReplicatedSnapshotId));
      updateProperties.set(ReplicationConfiguration.LAST_REPLICATION_TIMESTAMP,
          String.valueOf(lastReplicationTimestamp));
      updateProperties.set(ReplicationConfiguration.REPLICATION_STATUS, status.name());

      updateProperties.commit();
    } catch (Exception e) {
      throw new ReplicationException("Failed to update replication metadata", e);
    }
  }

  // Getters and setters

  public long getLastReplicatedSnapshotId() {
    return lastReplicatedSnapshotId;
  }

  public void setLastReplicatedSnapshotId(long lastReplicatedSnapshotId) {
    this.lastReplicatedSnapshotId = lastReplicatedSnapshotId;
  }

  public long getLastReplicationTimestamp() {
    return lastReplicationTimestamp;
  }

  public void setLastReplicationTimestamp(long lastReplicationTimestamp) {
    this.lastReplicationTimestamp = lastReplicationTimestamp;
  }

  public ReplicationStatus getStatus() {
    return status;
  }

  public void setStatus(ReplicationStatus status) {
    this.status = status;
  }

  /**
   * Check if this is the first replication for the table.
   *
   * @return true if no previous replication has been recorded
   */
  public boolean isFirstReplication() {
    return lastReplicatedSnapshotId == 0L;
  }

  /**
   * Get the age of the last replication in milliseconds.
   *
   * @return milliseconds since last replication, or -1 if never replicated
   */
  public long getReplicationAge() {
    if (lastReplicationTimestamp == 0L) {
      return -1L;
    }
    return System.currentTimeMillis() - lastReplicationTimestamp;
  }
}