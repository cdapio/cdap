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

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.Metrics.FlowControl;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.spi.data.transaction.TxCallable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintain and return total number of launching and running run-records. This class is used by
 * flow-control mechanism for launch requests. It also has a cleanup mechanism to automatically
 * remove old (i.e., configurable) entries from the counter as a safe-guard mechanism.
 */
public class RunRecordMonitorService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(RunRecordMonitorService.class);
  private final ProgramRuntimeService runtimeService;
  private final MetricsCollectionService metricsCollectionService;
  private final int maxConcurrentRuns;
  private final TransactionRunner transactionRunner;

  /**
   * Tracks the program runs.
   *
   * @param cConf configuration
   * @param runtimeService service to get info on programs
   * @param metricsCollectionService collect metrics
   */
  @Inject
  public RunRecordMonitorService(
      CConfiguration cConf,
      ProgramRuntimeService runtimeService,
      MetricsCollectionService metricsCollectionService,
      TransactionRunner transactionRunner) {
    this.runtimeService = runtimeService;
    this.metricsCollectionService = metricsCollectionService;
    this.maxConcurrentRuns = cConf.getInt(Constants.AppFabric.MAX_CONCURRENT_RUNS);
    this.transactionRunner = transactionRunner;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("RunRecordMonitorService started.");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("RunRecordMonitorService successfully shut down.");
  }

  /**
   * Add a new in-flight launch request and return total number of launching and running programs.
   *
   * @param programRunId run id associated with the launch request
   * @return total number of launching and running program runs.
   */
  public Counter addRequestAndGetCount(ProgramRunId programRunId) throws Exception {
    if (RunIds.getTime(programRunId.getRun(), TimeUnit.MILLISECONDS) == -1) {
      throw new Exception("None time-based UUIDs are not supported");
    }

    int launchingCount = addRequest(programRunId);
    int runningCount = getProgramsRunningCount();

    LOG.info(
        "Counter has {} concurrent launching and {} running programs.",
        launchingCount,
        runningCount);
    return new Counter(launchingCount, runningCount);
  }

  /**
   * Get imprecise (due to data races) total number of launching and running programs.
   *
   * @return total number of launching and running program runs.
   */
  public Counter getCount() {
    // TODO: Read from DB to return counter.
    // int launchingCount = launchingQueue.size();
    long applicationCount = TransactionRunners.run(transactionRunner,
        (TxCallable<Long>) context ->
            AppMetadataStore.create(context).getApplicationCount());
    int runningCount = getProgramsRunningCount();

    return new Counter(launchingCount, runningCount);


    return TransactionRunners.run(transactionRunner,
        (TxCallable<Counter>) context -> {
            int AppMetadataStore.create(context).getApplicationCount());

        });
  }

  /**
   * Add a new in-flight launch request.
   *
   * @param programRunId run id associated with the launch request
   *
   * @return Returns the count of launching programs.
   */
  public int addRequest(ProgramRunId programRunId) {
    int launchingCount = TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);
      store.recordProgramPending(programRunId);
      return store.countLaunchingRuns(null);
    });
    emitMetrics(Constants.Metrics.FlowControl.LAUNCHING_COUNT, launchingCount);
    LOG.info("Added request with runId {}.", programRunId);
    return launchingCount;
  }

  /**
   * Remove the request with the provided programRunId when the request is no longer launching.
   * I.e., not in-flight, not in {@link ProgramRunStatus#PENDING} and not in {@link
   * ProgramRunStatus#STARTING}
   *
   * @param programRunId of the request to be removed from launching queue.
   * @param emitRunningChange if true, also updates {@link
   *     Constants.Metrics.FlowControl#RUNNING_COUNT}
   */
  public void removeRequest(ProgramRunId programRunId, boolean emitRunningChange) {
    // TODO: See if this func can be refactored as it only emits metrics. Merge it with other
    //  functions if needed.
    emitLaunchingMetrics();
    if (emitRunningChange) {
      emitRunningMetrics();
    }
  }

  public void emitLaunchingMetrics(long value) {
    emitMetrics(Constants.Metrics.FlowControl.LAUNCHING_COUNT, value);
  }

  /**
   * Emit the {@link Constants.Metrics.FlowControl#LAUNCHING_COUNT} metric for runs.
   */
  public void emitLaunchingMetrics() {
    int launchingCount = TransactionRunners.run(transactionRunner, context -> {
      return AppMetadataStore.create(context).countLaunchingRuns(null);
    });
    emitMetrics(Constants.Metrics.FlowControl.LAUNCHING_COUNT, launchingCount);
  }


  /**
   * Emit the {@link Constants.Metrics.FlowControl#RUNNING_COUNT} metric for runs.
   */
  public void emitRunningMetrics() {
    emitMetrics(FlowControl.RUNNING_COUNT, getProgramsRunningCount());
  }

  private void emitMetrics(String metricName, long value) {
    LOG.trace("Setting metric {} to value {}", metricName, value);
    metricsCollectionService.getContext(Collections.emptyMap()).gauge(metricName, value);
  }

  /**
   * Returns the total number of programs in running state. The count includes batch (i.e., {@link
   * ProgramType#WORKFLOW}), streaming (i.e., {@link ProgramType#SPARK}) with no parent and
   * replication (i.e., {@link ProgramType#WORKER}) jobs.
   */
  private int getProgramsRunningCount() {
    List<ProgramRuntimeService.RuntimeInfo> list =
        runtimeService.listAll(
            ProgramType.WORKFLOW, ProgramType.WORKER, ProgramType.SPARK, ProgramType.MAPREDUCE);

    int launchingCount = launchingQueue.size();

    // We use program controllers (instead of querying metadata store) to count the total number of
    // programs in running state.
    // A program controller is created when a launch request is in the middle of starting state.
    // Therefore, the returning running count is NOT precise.
    int impreciseRunningCount =
        (int) list.stream()
                  .filter(r -> isRunning(r.getController().getState().getRunStatus()))
                  .count();

    if (maxConcurrentRuns < 0 || (launchingCount + impreciseRunningCount < maxConcurrentRuns)) {
      // It is safe to return the imprecise value since either flow control for runs is disabled
      // (i.e., -1) or flow control will not reject an incoming request yet.
      return impreciseRunningCount;
    }

    // Flow control is at the threshold. We return the precise count.
    return (int) list.stream()
                      .filter(
                          r ->
                              isRunning(r.getController().getState().getRunStatus())
                                  && !launchingQueue.contains(r.getController().getProgramRunId()))
                      .count();
  }

  private boolean isRunning(ProgramRunStatus status) {
    if (status == ProgramRunStatus.RUNNING
        || status == ProgramRunStatus.SUSPENDED
        || status == ProgramRunStatus.RESUMING) {
      return true;
    }

    return false;
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
