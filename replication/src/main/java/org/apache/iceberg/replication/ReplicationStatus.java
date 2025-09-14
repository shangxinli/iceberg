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

/**
 * Represents the current status of table replication.
 */
public enum ReplicationStatus {

  /** Replication is not configured for this table */
  DISABLED,

  /** Replication is configured but not currently running */
  IDLE,

  /** Replication is currently in progress */
  RUNNING,

  /** Last replication completed successfully */
  SUCCESS,

  /** Last replication failed */
  FAILED,

  /** Replication is in an unknown state */
  UNKNOWN;

  /**
   * Check if replication is currently active.
   *
   * @return true if replication is running
   */
  public boolean isActive() {
    return this == RUNNING;
  }

  /**
   * Check if replication is in a healthy state.
   *
   * @return true if replication is in a good state
   */
  public boolean isHealthy() {
    return this == SUCCESS || this == IDLE;
  }

  /**
   * Check if replication has failed.
   *
   * @return true if replication has failed
   */
  public boolean isFailed() {
    return this == FAILED;
  }
}