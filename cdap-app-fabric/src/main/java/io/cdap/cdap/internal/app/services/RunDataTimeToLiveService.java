/*
 * Copyright © 2023 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.internal.app.services;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data2.metadata.lineage.LineageTable;
import io.cdap.cdap.data2.metadata.lineage.field.FieldLineageTable;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.spi.data.transaction.TransactionException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service which periodically scans the database tables for records which should be deleted per the
 * global time to live value.
 *
 * <p>Does not run if no TTL is configured or a TTL of 0 is specified.
 * <p>
 *    This service would invoke other TTL based clean up services which implement the interface
 *    {@link CleanupService}. Those services would be called in sequence.
 * </p>
 */
public final class RunDataTimeToLiveService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(RunDataTimeToLiveService.class);

  private final TransactionRunner transactionRunner;
  private final boolean isEnabled;
  private final Duration ttlMaxAge;
  private final Duration checkFrequency;
  private final Duration initialDelay;
  private final Clock clock;

  private ScheduledExecutorService service;
  private List<CleanupService> cleanupServiceList;

  @Inject
  RunDataTimeToLiveService(CConfiguration cConf, TransactionRunner transactionRunner) {
    // Negative TTLs do not make sense, treat as 0.
    this.ttlMaxAge =
        Duration.ofDays(Math.max(cConf.getInt(Constants.AppFabric.RUN_DATA_CLEANUP_TTL_DAYS), 0));
    this.isEnabled = !this.ttlMaxAge.isZero();
    // Delay should be at least 1 hour to ensure it isn't infinitely running.
    this.checkFrequency =
        Duration.ofHours(
            Math.max(cConf.getInt(Constants.AppFabric.RUN_DATA_CLEANUP_TTL_FREQUENCY_HOURS), 1));
    // Negative delays do not make sense, treat as 0.
    this.initialDelay =
        Duration.ofMinutes(
            Math.max(
                cConf.getInt(Constants.AppFabric.RUN_DATA_CLEANUP_TTL_INITIAL_DELAY_MINUTES), 0));

    this.transactionRunner = transactionRunner;
    this.clock = Clock.systemUTC();
    this.cleanupServiceList = ImmutableList.of(
        new RunRecordsCleanupService(),
        new LineageCleanupService(),
        new FieldLineageCleanupService());
  }

  @Override
  protected void startUp() {
    if (!isEnabled) {
      LOG.info("No TTL configured, skipping starting RunDataTimeToLiveService");
      return;
    }

    service =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("Records TTL janitor").build());

    service.scheduleAtFixedRate(
        () -> doCleanup(),
        initialDelay.getSeconds(),
        checkFrequency.getSeconds(),
        TimeUnit.SECONDS);
  }

  @Override
  protected void shutDown() {
    if (!isEnabled) {
      // no-op because no services were started.
      return;
    }
    LOG.info("Stopping RunDataTimeToLiveService");

    service.shutdownNow();
  }

  private void doCleanup() {
    Instant endDate = Instant.now(clock).minus(ttlMaxAge);
    // Perform cleanup together for all services with a fixed end time. This is currently a
    // sequential call and may be executed parallelly in future if required.
    this.cleanupServiceList.forEach(service -> {
      long startTime = System.currentTimeMillis();
      service.doCleanup(endDate);
      double timeTaken = (System.currentTimeMillis() - startTime) / 1000.0;
      LOG.info("{} cleanup completed in {} seconds", service.getClass().getName(), timeTaken);
    });
  }

  private interface CleanupService {

    void doCleanup(Instant endDate);
  }

  private class RunRecordsCleanupService implements CleanupService {

    @Override
    public void doCleanup(Instant endDate) {
      LOG.info("Doing scheduled cleanup, deleting all run records before {}", endDate);

      try {
        transactionRunner.run(
            context -> {
              AppMetadataStore appMetadataStore = AppMetadataStore.create(context);

              appMetadataStore.deleteCompletedRunsStartedBefore(endDate);
            });
      } catch (TransactionException e) {
        LOG.error("Failed to clean up old run records", e);
      }
    }
  }

  private class LineageCleanupService implements CleanupService {

    @Override
    public void doCleanup(Instant endDate) {
      LOG.info("Doing scheduled cleanup, deleting all run lineage records before {}", endDate);

      try {
        transactionRunner.run(
            context -> {
              LineageTable lineageTable = LineageTable.create(context);

              lineageTable.deleteCompletedLineageRecordsStartedBefore(endDate);
            });
      } catch (TransactionException e) {
        LOG.error("Failed to clean up old lineage records", e);
      }
    }
  }


  private class FieldLineageCleanupService implements CleanupService {

    @Override
    public void doCleanup(Instant endDate) {
      LOG.info("Doing scheduled cleanup, deleting all field lineage records before {}", endDate);

      try {
        transactionRunner.run(
            context -> {
              FieldLineageTable fieldLineageTable = FieldLineageTable.create(context);

              fieldLineageTable.deleteFieldRecordsBefore(endDate);
            });
      } catch (TransactionException e) {
        LOG.error("Failed to clean up old field lineage records", e);
      }
    }
  }
}
