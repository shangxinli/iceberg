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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of ReplicationService that provides full replication functionality.
 * Uses proper thread management and error handling for production use.
 */
public class DefaultReplicationService implements ReplicationService {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultReplicationService.class);
  
  // Thread management
  private final ExecutorService replicationExecutor;
  private final ScheduledExecutorService retryExecutor;
  private final Map<String, CompletableFuture<Void>> activeReplications;
  private final AtomicInteger activeReplicationCount;
  private final int maxConcurrentReplications;
  private volatile boolean shutdown = false;

  // Retry tracking
  private final Map<String, Integer> retryCounters = new ConcurrentHashMap<>();
  private final int maxRetries;
  private final long initialRetryDelayMs;
  private final double retryBackoffMultiplier;

  public DefaultReplicationService() {
    this(Runtime.getRuntime().availableProcessors() * 2, 10);
  }

  public DefaultReplicationService(int threadPoolSize, int maxConcurrentReplications) {
    this(threadPoolSize, maxConcurrentReplications, 3, 1000L, 2.0);
  }

  public DefaultReplicationService(int threadPoolSize, int maxConcurrentReplications,
                                  int maxRetries, long initialRetryDelayMs, double retryBackoffMultiplier) {
    // Validate parameters
    if (threadPoolSize <= 0) {
      throw new IllegalArgumentException("Thread pool size must be positive, got: " + threadPoolSize);
    }
    if (maxConcurrentReplications <= 0) {
      throw new IllegalArgumentException("Max concurrent replications must be positive, got: " + maxConcurrentReplications);
    }
    if (maxRetries < 0) {
      throw new IllegalArgumentException("Max retries must be non-negative, got: " + maxRetries);
    }
    if (initialRetryDelayMs <= 0) {
      throw new IllegalArgumentException("Initial retry delay must be positive, got: " + initialRetryDelayMs);
    }
    if (retryBackoffMultiplier < 1.0) {
      throw new IllegalArgumentException("Retry backoff multiplier must be >= 1.0, got: " + retryBackoffMultiplier);
    }

    this.maxConcurrentReplications = maxConcurrentReplications;
    this.maxRetries = maxRetries;
    this.initialRetryDelayMs = initialRetryDelayMs;
    this.retryBackoffMultiplier = retryBackoffMultiplier;
    this.activeReplications = new ConcurrentHashMap<>();
    this.activeReplicationCount = new AtomicInteger(0);
    
    // Create thread factories with proper naming
    ThreadFactory replicationThreadFactory = new ThreadFactoryBuilder()
        .setNameFormat("replication-worker-%d")
        .setDaemon(false)
        .setPriority(Thread.NORM_PRIORITY)
        .setUncaughtExceptionHandler((t, e) -> LOG.error("Uncaught exception in replication thread: {}", t.getName(), e))
        .build();
        
    ThreadFactory retryThreadFactory = new ThreadFactoryBuilder()
        .setNameFormat("replication-retry-%d")
        .setDaemon(false)  // Changed from true to false to ensure proper shutdown
        .setPriority(Thread.NORM_PRIORITY)
        .setUncaughtExceptionHandler((t, e) -> LOG.error("Uncaught exception in retry thread: {}", t.getName(), e))
        .build();
    
    // Create thread pools
    this.replicationExecutor = Executors.newFixedThreadPool(threadPoolSize, replicationThreadFactory);
    this.retryExecutor = Executors.newScheduledThreadPool(2, retryThreadFactory);
    
    LOG.info("Initialized DefaultReplicationService with {} threads, max {} concurrent replications",
             threadPoolSize, maxConcurrentReplications);

    // Note: Shutdown hook removed due to error-prone checks
    // Applications should call shutdown() explicitly before exit
  }

  @Override
  public boolean isReplicationEnabled(Map<String, String> tableProperties) {
    return "true".equals(tableProperties.get(TableProperties.REPLICATION_ENABLED));
  }

  @Override
  public void triggerReplication(Table table, Map<String, String> tableProperties, String tableName) {
    if (!isReplicationEnabled(tableProperties)) {
      return;
    }

    if (shutdown) {
      LOG.warn("Replication service is shutting down, skipping replication for table: {}", tableName);
      return;
    }

    // Check if we're at capacity
    if (activeReplicationCount.get() >= maxConcurrentReplications) {
      LOG.warn("Maximum concurrent replications reached ({}), queuing replication for table: {}", 
               maxConcurrentReplications, tableName);
      // Schedule for later execution with backoff
      scheduleRetryWithBackoff(table, tableProperties, tableName);
      return;
    }

    // Cancel any existing replication for this table
    CompletableFuture<Void> existing = activeReplications.get(tableName);
    if (existing != null && !existing.isDone()) {
      LOG.info("Cancelling existing replication for table: {}", tableName);
      existing.cancel(true);
      // Clean up immediately to avoid count inconsistency
      activeReplications.remove(tableName);
      activeReplicationCount.decrementAndGet();
    }

    // Start new replication
    CompletableFuture<Void> future = CompletableFuture
        .runAsync(() -> doReplication(table, tableProperties, tableName), replicationExecutor)
        .exceptionally(throwable -> {
          LOG.error("Replication failed for table: {}", tableName, throwable);
          scheduleRetryWithBackoff(table, tableProperties, tableName);
          return null;
        })
        .whenComplete((result, throwable) -> {
          activeReplications.remove(tableName);
          activeReplicationCount.decrementAndGet();
          if (throwable == null) {
            LOG.info("Replication completed successfully for table: {}", tableName);
            // Reset retry counter on success
            retryCounters.remove(tableName);
          }
        });

    // Atomic update to avoid race conditions
    activeReplicationCount.incrementAndGet();
    activeReplications.put(tableName, future);
    
    String replicationMode = tableProperties.getOrDefault(
        TableProperties.REPLICATION_MODE, TableProperties.REPLICATION_MODE_DEFAULT);

    if (TableProperties.REPLICATION_MODE_SYNC.equals(replicationMode)) {
      // For sync mode, wait for completion but don't fail the main transaction
      try {
        future.get(5, TimeUnit.MINUTES); // 5 minute timeout
      } catch (Exception e) {
        LOG.error("Synchronous replication timed out or failed for table: {}, but primary transaction succeeded", 
                 tableName, e);
      }
    } else {
      LOG.debug("Started asynchronous replication for table: {}", tableName);
    }
  }

  private void doReplication(Table table, Map<String, String> tableProperties, String tableName) {
    try {
      LOG.debug("Starting replication for table: {}", tableName);

      // Create replication manager
      TableReplicationManager replicationManager = new DefaultTableReplicationManager(table);

      // Get Hadoop configuration
      Configuration conf = new Configuration();
      if (table.io() instanceof org.apache.iceberg.hadoop.HadoopFileIO) {
        conf = ((org.apache.iceberg.hadoop.HadoopFileIO) table.io()).conf();
      }

      // Configure replication targets
      configureReplicationTargets(replicationManager, tableProperties, conf);

      if (!replicationManager.isReplicationEnabled()) {
        LOG.debug("No replication targets configured for table: {}", tableName);
        return;
      }

      // Execute replication
      replicationManager.startReplication();
      LOG.info("Replication completed successfully for table: {}", tableName);

    } catch (ReplicationException e) {
      // Preserve specific replication exceptions for better error handling
      throw new RuntimeException("Replication failed for table: " + tableName, e);
    } catch (Exception e) {
      // Wrap unexpected exceptions for consistency
      throw new RuntimeException("Unexpected error during replication for table: " + tableName, e);
    }
  }

  private void scheduleRetryWithBackoff(Table table, Map<String, String> tableProperties, String tableName) {
    if (shutdown) {
      return;
    }

    int currentRetryCount = retryCounters.compute(tableName, (key, value) ->
        value == null ? 1 : value + 1);

    if (currentRetryCount > maxRetries) {
      LOG.error("Max retries ({}) exceeded for table: {}, giving up", maxRetries, tableName);
      retryCounters.remove(tableName);
      return;
    }

    // Calculate exponential backoff delay
    long delayMs = (long) (initialRetryDelayMs * Math.pow(retryBackoffMultiplier, currentRetryCount - 1));
    // Cap at 5 minutes to prevent excessive delays
    delayMs = Math.min(delayMs, 300_000L);

    LOG.info("Scheduling retry #{} for table: {} in {}ms", currentRetryCount, tableName, delayMs);

    @SuppressWarnings("FutureReturnValueIgnored")
    java.util.concurrent.ScheduledFuture<?> unused = retryExecutor.schedule(() -> {
      if (!shutdown) {
        try {
          triggerReplication(table, tableProperties, tableName);
        } catch (Exception e) {
          LOG.error("Retry #{} replication failed for table: {}", currentRetryCount, tableName, e);
        }
      }
    }, delayMs, TimeUnit.MILLISECONDS);
  }

  private void configureReplicationTargets(
      TableReplicationManager replicationManager,
      Map<String, String> tableProperties,
      Configuration conf) throws ReplicationException {

    // Check for multi-target configuration first
    String replicationTargets = tableProperties.get(TableProperties.REPLICATION_TARGETS);
    if (replicationTargets != null && !replicationTargets.trim().isEmpty()) {
      // Multi-target configuration
      Iterable<String> targets = Splitter.on(',').split(replicationTargets);
      for (String target : targets) {
        String trimmedTarget = target.trim();
        if (!trimmedTarget.isEmpty()) {
          replicationManager.enableReplication(trimmedTarget, conf);
          LOG.debug("Enabled replication to target: {}", trimmedTarget);
        }
      }
    } else {
      // Check for per-target configuration (write.replication.target.dc1.path, etc.)
      boolean foundTargetConfig = false;

      for (Map.Entry<String, String> entry : tableProperties.entrySet()) {
        String key = entry.getKey();
        if (key.startsWith(TableProperties.REPLICATION_TARGET_PREFIX) &&
            key.endsWith(TableProperties.REPLICATION_TARGET_PATH_SUFFIX)) {
          String targetId = key.substring(
              TableProperties.REPLICATION_TARGET_PREFIX.length(),
              key.length() - TableProperties.REPLICATION_TARGET_PATH_SUFFIX.length());

          String targetPath = entry.getValue();
          String targetEnabledKey = TableProperties.REPLICATION_TARGET_PREFIX + targetId +
                                   TableProperties.REPLICATION_TARGET_ENABLED_SUFFIX;
          String targetEnabled = tableProperties.getOrDefault(targetEnabledKey, "true");

          if ("true".equals(targetEnabled) && targetPath != null && !targetPath.trim().isEmpty()) {
            replicationManager.enableReplication(targetPath.trim(), conf);
            LOG.debug("Enabled replication to target {}: {}", targetId, targetPath);
            foundTargetConfig = true;
          }
        }
      }

      // Fallback to legacy single target configuration
      if (!foundTargetConfig) {
        String legacyTarget = tableProperties.get(TableProperties.REPLICATION_TARGET);
        if (legacyTarget != null && !legacyTarget.trim().isEmpty()) {
          replicationManager.enableReplication(legacyTarget.trim(), conf);
          LOG.debug("Enabled replication to legacy target: {}", legacyTarget);
        }
      }
    }
  }

  /**
   * Gracefully shutdown the replication service.
   * Waits for active replications to complete with a timeout.
   */
  public void shutdown() {
    shutdown(30, TimeUnit.SECONDS);
  }

  /**
   * Gracefully shutdown the replication service with custom timeout.
   * 
   * @param timeout the maximum time to wait for active replications to complete
   * @param unit the time unit of the timeout
   */
  public void shutdown(long timeout, TimeUnit unit) {
    LOG.info("Shutting down DefaultReplicationService...");
    shutdown = true;

    try {
      // Cancel all active replications
      for (CompletableFuture<Void> future : activeReplications.values()) {
        if (!future.isDone()) {
          future.cancel(true);
        }
      }
      // Clear the map to release references
      activeReplications.clear();
      activeReplicationCount.set(0);

      // Shutdown executors
      replicationExecutor.shutdown();
      retryExecutor.shutdown();

      // Wait for completion
      if (!replicationExecutor.awaitTermination(timeout, unit)) {
        LOG.warn("Replication executor did not terminate within timeout, forcing shutdown");
        replicationExecutor.shutdownNow();
      }

      if (!retryExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        LOG.warn("Retry executor did not terminate within timeout, forcing shutdown");
        retryExecutor.shutdownNow();
      }

      LOG.info("DefaultReplicationService shutdown completed");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Shutdown interrupted, forcing immediate shutdown", e);
      replicationExecutor.shutdownNow();
      retryExecutor.shutdownNow();
    }
  }

  /**
   * Get the number of active replications.
   * 
   * @return number of active replications
   */
  public int getActiveReplicationCount() {
    return activeReplicationCount.get();
  }

  /**
   * Get the maximum number of concurrent replications allowed.
   * 
   * @return maximum concurrent replications
   */
  public int getMaxConcurrentReplications() {
    return maxConcurrentReplications;
  }

  /**
   * Check if the service is shutting down.
   * 
   * @return true if shutting down, false otherwise
   */
  public boolean isShuttingDown() {
    return shutdown;
  }
}