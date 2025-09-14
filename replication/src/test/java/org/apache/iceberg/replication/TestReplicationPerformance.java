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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(ParameterizedTestExtension.class)
public class TestReplicationPerformance extends TestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationPerformance.class);

  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Lists.newArrayList(1, 2);
  }

  @TempDir
  private File tempDir;

  private Table primaryTable;
  private String primaryTablePath;
  private String secondaryTablePath;
  private DefaultTableReplicationManager manager;
  private Configuration conf;

  @BeforeEach
  public void setup() throws IOException, ReplicationException {
    conf = new Configuration();

    // Create primary table path
    File primaryDir = new File(tempDir, "primary_table");
    primaryDir.mkdirs();
    primaryTablePath = primaryDir.getAbsolutePath();

    // Create secondary table path
    File secondaryDir = new File(tempDir, "secondary_table");
    secondaryDir.mkdirs();
    secondaryTablePath = secondaryDir.getAbsolutePath();

    // Create primary table
    HadoopTables tables = new HadoopTables(conf);
    primaryTable = tables.create(SCHEMA, primaryTablePath);

    // Setup replication
    manager = new DefaultTableReplicationManager(primaryTable);
    manager.enableReplication(secondaryTablePath, conf);
  }

  @TestTemplate
  public void testSingleCommitReplicationPerformance() throws ReplicationException, IOException {
    // Add data
    DataFile fileA = createRealDataFile("single-commit-perf-a.parquet");
    primaryTable.newAppend().appendFile(fileA).commit();

    // Measure replication performance
    long startTime = System.currentTimeMillis();
    manager.startReplication();
    long duration = System.currentTimeMillis() - startTime;

    LOG.info("Single commit replication took {}ms", duration);

    // Verify replication succeeded
    assertThat(manager.getCommitDistance()).isEqualTo(0);
    assertThat(manager.getReplicationStatus()).isEqualTo(ReplicationStatus.SUCCESS);

    // Basic performance check - should complete within reasonable time
    assertThat(duration).isLessThan(10000); // 10 seconds
  }

  @TestTemplate
  public void testMultipleCommitReplicationPerformance() throws ReplicationException, IOException {
    // Create data files once
    DataFile fileA = createRealDataFile("multiple-commit-perf-a.parquet");
    DataFile fileB = createRealDataFile("multiple-commit-perf-b.parquet");
    DataFile fileC = createRealDataFile("multiple-commit-perf-c.parquet");

    // Add multiple commits
    int numCommits = 10;
    for (int i = 0; i < numCommits; i++) {
      if (i % 3 == 0) {
        primaryTable.newAppend().appendFile(fileA).commit();
      } else if (i % 3 == 1) {
        primaryTable.newAppend().appendFile(fileB).commit();
      } else {
        primaryTable.newAppend().appendFile(fileC).commit();
      }
    }

    assertThat(manager.getCommitDistance()).isEqualTo(numCommits);

    // Measure replication performance
    long startTime = System.currentTimeMillis();
    manager.startReplication();
    long duration = System.currentTimeMillis() - startTime;

    LOG.info("Replication of {} commits took {}ms ({}ms per commit)",
        numCommits, duration, duration / numCommits);

    // Verify replication succeeded
    assertThat(manager.getCommitDistance()).isEqualTo(0);
    assertThat(manager.getReplicationStatus()).isEqualTo(ReplicationStatus.SUCCESS);

    // Performance check - should complete within reasonable time
    assertThat(duration).isLessThan(30000); // 30 seconds
    assertThat(duration / numCommits).isLessThan(5000); // 5 seconds per commit
  }

  @TestTemplate
  public void testIncrementalReplicationPerformance() throws ReplicationException, IOException {
    // Setup initial state
    DataFile fileA = createRealDataFile("incremental-perf-a.parquet");
    DataFile fileB = createRealDataFile("incremental-perf-b.parquet");
    primaryTable.newAppend().appendFile(fileA).commit();
    manager.startReplication();

    // Measure incremental replication performance
    long totalTime = 0;
    int incrementalCommits = 5;

    for (int i = 0; i < incrementalCommits; i++) {
      // Add one commit
      primaryTable.newAppend().appendFile(fileB).commit();

      // Measure incremental replication
      long startTime = System.currentTimeMillis();
      manager.startReplication();
      long duration = System.currentTimeMillis() - startTime;

      totalTime += duration;
      LOG.info("Incremental replication {} took {}ms", i + 1, duration);

      // Verify state
      assertThat(manager.getCommitDistance()).isEqualTo(0);
    }

    long avgTime = totalTime / incrementalCommits;
    LOG.info("Average incremental replication time: {}ms", avgTime);

    // Incremental replication should be faster than full replication
    assertThat(avgTime).isLessThan(5000); // 5 seconds
  }

  @TestTemplate
  public void testReplicationLagMeasurement() throws ReplicationException, InterruptedException, IOException {
    // Add data
    DataFile fileA = createRealDataFile("lag-measurement-a.parquet");
    primaryTable.newAppend().appendFile(fileA).commit();

    // Wait to create lag
    Thread.sleep(100);

    // Measure lag before replication
    long lagBeforeMs = manager.getReplicationLagMillis();
    assertThat(lagBeforeMs).isGreaterThan(0);

    LOG.info("Replication lag before sync: {}ms", lagBeforeMs);

    // Replicate
    long startTime = System.currentTimeMillis();
    manager.startReplication();
    long replicationTime = System.currentTimeMillis() - startTime;

    // Measure lag after replication
    long lagAfterMs = manager.getReplicationLagMillis();
    assertThat(lagAfterMs).isEqualTo(0);

    LOG.info("Replication time: {}ms, lag after sync: {}ms", replicationTime, lagAfterMs);
  }

  @TestTemplate
  public void testConcurrentCommitPerformance() throws Exception {
    // This test measures performance when commits happen while replication is running

    // Add initial data
    DataFile fileA = createRealDataFile("concurrent-perf-a.parquet");
    DataFile fileB = createRealDataFile("concurrent-perf-b.parquet");
    DataFile fileC = createRealDataFile("concurrent-perf-c.parquet");
    primaryTable.newAppend().appendFile(fileA).commit();

    // Start replication in background
    Thread replicationThread = new Thread(() -> {
      try {
        manager.startReplication();
      } catch (ReplicationException e) {
        LOG.error("Background replication failed", e);
      }
    });

    long startTime = System.currentTimeMillis();
    replicationThread.start();

    // Add more commits while replication is running
    Thread.sleep(50); // Give replication a head start
    primaryTable.newAppend().appendFile(fileB).commit();
    primaryTable.newAppend().appendFile(fileC).commit();

    // Wait for background replication to complete
    replicationThread.join(10000); // 10 second timeout

    long totalTime = System.currentTimeMillis() - startTime;
    LOG.info("Concurrent operation completed in {}ms", totalTime);

    // Final replication to catch up
    manager.startReplication();

    // Verify final state
    assertThat(manager.getCommitDistance()).isEqualTo(0);
    assertThat(totalTime).isLessThan(15000); // 15 seconds
  }

  @TestTemplate
  public void testLargeTableReplicationEstimate() throws ReplicationException, IOException {
    // Create a scenario with multiple files to test estimation
    DataFile fileA = createRealDataFile("large-table-est-a.parquet");
    DataFile fileB = createRealDataFile("large-table-est-b.parquet");
    DataFile fileC = createRealDataFile("large-table-est-c.parquet");

    primaryTable.newAppend()
        .appendFile(fileA)
        .appendFile(fileB)
        .appendFile(fileC)
        .commit();

    primaryTable.newAppend()
        .appendFile(fileA)
        .appendFile(fileB)
        .commit();

    // This test mainly verifies that estimation doesn't crash
    // and provides reasonable bounds
    long distance = manager.getCommitDistance();
    long lagMs = manager.getReplicationLagMillis();

    LOG.info("Table has {} commits behind, lag: {}ms", distance, lagMs);

    assertThat(distance).isGreaterThan(0);
    assertThat(lagMs).isGreaterThanOrEqualTo(0);

    // Perform actual replication and measure
    long startTime = System.currentTimeMillis();
    manager.startReplication();
    long actualTime = System.currentTimeMillis() - startTime;

    LOG.info("Actual replication time: {}ms", actualTime);
    assertThat(actualTime).isLessThan(20000); // 20 seconds
  }

  @TestTemplate
  public void testReplicationStatusTransitions() throws ReplicationException, IOException {
    // Test that status transitions happen quickly and correctly

    // Initial status
    assertThat(manager.getReplicationStatus()).isEqualTo(ReplicationStatus.IDLE);

    // Add data
    DataFile fileA = createRealDataFile("status-transitions-a.parquet");
    primaryTable.newAppend().appendFile(fileA).commit();

    // Status should still be idle until replication starts
    assertThat(manager.getReplicationStatus()).isEqualTo(ReplicationStatus.IDLE);

    // Start replication and check final status
    manager.startReplication();

    // Should end up in success status
    assertThat(manager.getReplicationStatus()).isEqualTo(ReplicationStatus.SUCCESS);
  }

  @TestTemplate
  public void testValidationPerformance() throws ReplicationException, IOException {
    // Setup replicated data
    DataFile fileA = createRealDataFile("validation-perf-a.parquet");
    DataFile fileB = createRealDataFile("validation-perf-b.parquet");
    primaryTable.newAppend().appendFile(fileA).appendFile(fileB).commit();
    manager.startReplication();

    // Measure validation performance
    long startTime = System.currentTimeMillis();
    boolean isValid = manager.validateSecondaryTable();
    long validationTime = System.currentTimeMillis() - startTime;

    LOG.info("Table validation took {}ms", validationTime);

    assertThat(isValid).isTrue();
    assertThat(validationTime).isLessThan(5000); // 5 seconds

    // Measure sync check performance
    startTime = System.currentTimeMillis();
    boolean inSync = manager.isInSync();
    long syncCheckTime = System.currentTimeMillis() - startTime;

    LOG.info("Sync check took {}ms", syncCheckTime);

    assertThat(inSync).isTrue();
    assertThat(syncCheckTime).isLessThan(1000); // 1 second
  }

  /**
   * Create a real data file for testing.
   */
  private DataFile createRealDataFile(String filename) throws IOException {
    File dataFile = new File(tempDir, filename);
    Files.write(dataFile.toPath(), "test data for replication".getBytes());

    return DataFiles.builder(primaryTable.spec())
        .withPath(dataFile.getAbsolutePath())
        .withFileSizeInBytes(dataFile.length())
        .withRecordCount(1)
        .build();
  }
}