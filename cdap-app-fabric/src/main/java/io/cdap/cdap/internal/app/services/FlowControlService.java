/*
 * Copyright © 2022 Cask Data, Inc.
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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintain and provides total number of launching and running run-records. This class is used by
 * flow-control mechanism for launch requests.
 */
public class FlowControlService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(FlowControlService.class);

  private final MetricsCollectionService metricsCollectionService;
  private final TransactionRunner transactionRunner;

  /**
   * Monitors the program flow control.
   *
   * @param metricsCollectionService collect metrics
   */
  @Inject
  public FlowControlService(
      MetricsCollectionService metricsCollectionService,
      TransactionRunner transactionRunner) {
    this.metricsCollectionService = metricsCollectionService;
    this.transactionRunner = transactionRunner;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("FlowControlService started.");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("FlowControlService successfully shut down.");
  }

  /**
   * Add a new in-flight launch request and return total number of launching and running programs.
   *
   * @param programRunId run id associated with the launch request
   * @return total number of launching and running program runs.
   */
  public Counter addRequestAndGetCounter(ProgramRunId programRunId, ProgramOptions programOptions,
      ProgramDescriptor programDescriptor) throws Exception {
    if (RunIds.getTime(programRunId.getRun(), TimeUnit.MILLISECONDS) == -1) {
      throw new Exception("None time-based UUIDs are not supported");
    }

    Counter counter = TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);
      store.recordProgramPending(programRunId,
          programOptions.getArguments().asMap(),
          programOptions.getUserArguments().asMap(),
          programDescriptor.getArtifactId().toApiArtifactId());
      int launchingCount = store.getFlowControlLaunchingCount();
      int runningCount = store.getFlowControlRunningCount();
      return new Counter(launchingCount, runningCount);
    });
    LOG.info("Added request with runId {}.", programRunId);
    emitMetrics(Constants.Metrics.FlowControl.LAUNCHING_COUNT, counter.getLaunchingCount());

    LOG.info(
        "Counter has {} concurrent launching and {} running programs.",
        counter.getLaunchingCount(),
        counter.getRunningCount());
    return counter;
  }

  /**
   * Get total number of launching and running programs.
   *
   * @return Counter with total number of launching and running program runs.
   */
  public Counter getCounter() {
    return TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);
      return new Counter(store.getFlowControlLaunchingCount(), store.getFlowControlRunningCount());
    });
  }

  public void emitFlowControlMetrics() {
    Counter counter = getCounter();
    emitMetrics(Constants.Metrics.FlowControl.LAUNCHING_COUNT, counter.getLaunchingCount());
    emitMetrics(Constants.Metrics.FlowControl.RUNNING_COUNT, counter.getRunningCount());
  }

  private void emitMetrics(String metricName, long value) {
    LOG.trace("Setting metric {} to value {}", metricName, value);
    Map<String, String> tags = ImmutableMap.of(
        Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace()
    );
    metricsCollectionService.getContext(tags).gauge(metricName, value);
  }

  /**
   * Counts the concurrent program runs.
   */
  public class Counter {

    /**
     * Total number of launch requests that have been accepted but still missing in metadata store +
     * * total number of run records with {@link ProgramRunStatus#PENDING} status + total number of
     * run records with {@link ProgramRunStatus#STARTING} status.
     */
    private final int launchingCount;

    /**
     * Total number of run records with {@link ProgramRunStatus#RUNNING} status + Total number of run
     * records with {@link ProgramRunStatus#SUSPENDED} status + Total number of run records with
     * {@link ProgramRunStatus#RESUMING} status.
     */
    private final int runningCount;

    Counter(int launchingCount, int runningCount) {
      this.launchingCount = launchingCount;
      this.runningCount = runningCount;
    }

    public int getLaunchingCount() {
      return launchingCount;
    }

    public int getRunningCount() {
      return runningCount;
    }
  }
}
