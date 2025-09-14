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

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;

/**
 * Interface for managing table replication between primary and secondary data centers.
 * Provides functionality to enable, disable, and execute replication operations.
 */
public interface TableReplicationManager {

  /**
   * Enable replication for the table to a secondary location.
   *
   * @param secondaryTablePath the path where the secondary table should be created/maintained
   * @param conf Hadoop configuration for file system operations
   * @throws ReplicationException if replication cannot be enabled
   */
  void enableReplication(String secondaryTablePath, Configuration conf) throws ReplicationException;

  /**
   * Disable replication for the table.
   *
   * @throws ReplicationException if replication cannot be disabled
   */
  void disableReplication() throws ReplicationException;

  /**
   * Start the replication process to sync secondary table with primary.
   * This method implements atomic all-or-nothing semantics.
   *
   * @throws ReplicationException if replication fails
   */
  void startReplication() throws ReplicationException;

  /**
   * Get the current replication status.
   *
   * @return current replication status
   */
  ReplicationStatus getReplicationStatus();

  /**
   * Calculate the distance (number of commits) between primary and secondary tables.
   *
   * @return number of commits that secondary table is behind primary
   * @throws ReplicationException if distance cannot be calculated
   */
  long getCommitDistance() throws ReplicationException;

  /**
   * Check if replication is currently enabled for this table.
   *
   * @return true if replication is enabled, false otherwise
   */
  boolean isReplicationEnabled();

  /**
   * Get the secondary table instance if replication is enabled.
   *
   * @return secondary table instance or null if replication not enabled
   */
  Table getSecondaryTable();

  /**
   * Force a full replication, replicating all commits regardless of current state.
   * This is useful for recovery scenarios.
   *
   * @throws ReplicationException if full replication fails
   */
  void forceFullReplication() throws ReplicationException;
}