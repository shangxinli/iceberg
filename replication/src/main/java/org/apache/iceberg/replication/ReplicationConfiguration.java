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
 * Configuration constants for table replication functionality.
 */
public class ReplicationConfiguration {

  private ReplicationConfiguration() {}

  // Table properties for replication configuration
  public static final String REPLICATION_ENABLED = "iceberg.replication.enabled";
  public static final String SECONDARY_TABLE_PATH = "iceberg.replication.secondary.path";
  public static final String REPLICATION_MAX_RETRIES = "iceberg.replication.max-retries";
  public static final String REPLICATION_BATCH_SIZE = "iceberg.replication.batch-size";
  public static final String REPLICATION_TIMEOUT_MS = "iceberg.replication.timeout-ms";
  public static final String REPLICATION_THREAD_POOL_SIZE = "iceberg.replication.thread-pool-size";
  public static final String REPLICATION_CHECKSUM_ENABLED = "iceberg.replication.checksum.enabled";
  public static final String REPLICATION_COMPRESSION_ENABLED = "iceberg.replication.compression.enabled";

  // Replication metadata tracking
  public static final String LAST_REPLICATED_SNAPSHOT_ID = "iceberg.replication.last-snapshot-id";
  public static final String LAST_REPLICATION_TIMESTAMP = "iceberg.replication.last-timestamp";
  public static final String REPLICATION_STATUS = "iceberg.replication.status";
  public static final String REPLICATED_SNAPSHOTS = "iceberg.replication.snapshot-ids";
  public static final String SOURCE_TABLE_UUID = "iceberg.replication.source-table-uuid";

  // Default values
  public static final String DEFAULT_REPLICATION_ENABLED = "false";
  public static final String DEFAULT_MAX_RETRIES = "3";
  public static final String DEFAULT_BATCH_SIZE = "10";
  public static final String DEFAULT_TIMEOUT_MS = "300000"; // 5 minutes
  public static final String DEFAULT_THREAD_POOL_SIZE = "4";
  public static final String DEFAULT_CHECKSUM_ENABLED = "true";
  public static final String DEFAULT_COMPRESSION_ENABLED = "false";

  // Replication modes
  public static final String REPLICATION_MODE = "iceberg.replication.mode";
  public static final String REPLICATION_MODE_ASYNC = "async";
  public static final String REPLICATION_MODE_SYNC = "sync";
  public static final String DEFAULT_REPLICATION_MODE = REPLICATION_MODE_ASYNC;

  // File replication strategy
  public static final String FILE_REPLICATION_STRATEGY = "iceberg.replication.file.strategy";
  public static final String FILE_REPLICATION_STRATEGY_HADOOP = "hadoop";
  public static final String FILE_REPLICATION_STRATEGY_CLOUD = "cloud";
  public static final String DEFAULT_FILE_REPLICATION_STRATEGY = FILE_REPLICATION_STRATEGY_HADOOP;
}