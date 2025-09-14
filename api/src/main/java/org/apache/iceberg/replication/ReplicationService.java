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

/**
 * Service interface for table replication functionality.
 * This provides a clean abstraction for the core module to use replication
 * without direct dependencies on implementation classes.
 */
public interface ReplicationService {

  /**
   * Check if replication is enabled for the given table properties.
   *
   * @param tableProperties table properties to check
   * @return true if replication is enabled
   */
  boolean isReplicationEnabled(Map<String, String> tableProperties);

  /**
   * Trigger replication for the given table using the specified configuration.
   * This method is designed to never throw exceptions that could fail the main transaction.
   *
   * @param table the table to replicate
   * @param tableProperties table properties containing replication configuration
   * @param tableName table name for logging
   */
  void triggerReplication(Table table, Map<String, String> tableProperties, String tableName);
}