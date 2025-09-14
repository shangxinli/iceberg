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
import java.util.ServiceLoader;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating replication service instances.
 * Uses Java ServiceLoader pattern to allow pluggable replication implementations.
 */
public class ReplicationServiceFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationServiceFactory.class);

  private static volatile ReplicationService instance;

  /**
   * Get the replication service instance.
   * If no implementation is found via ServiceLoader, returns a no-op implementation.
   *
   * @return replication service instance
   */
  public static ReplicationService getInstance() {
    if (instance == null) {
      synchronized (ReplicationServiceFactory.class) {
        if (instance == null) {
          instance = loadReplicationService();
        }
      }
    }
    return instance;
  }

  private static ReplicationService loadReplicationService() {
    try {
      ServiceLoader<ReplicationService> serviceLoader = ServiceLoader.load(ReplicationService.class);
      for (ReplicationService service : serviceLoader) {
        LOG.info("Loaded replication service: {}", service.getClass().getName());
        return service;
      }
    } catch (Exception e) {
      LOG.warn("Failed to load replication service via ServiceLoader", e);
    }

    // Fallback to no-op implementation if no service is found
    LOG.debug("Using no-op replication service implementation");
    return new NoOpReplicationService();
  }

  /**
   * Reset the singleton instance (for testing purposes).
   */
  static void resetInstance() {
    instance = null;
  }

  /**
   * No-op implementation that does nothing when replication module is not present.
   */
  private static class NoOpReplicationService implements ReplicationService {
    @Override
    public boolean isReplicationEnabled(Map<String, String> tableProperties) {
      return false;
    }

    @Override
    public void triggerReplication(Table table, Map<String, String> tableProperties, String tableName) {
      // No-op
    }
  }
}