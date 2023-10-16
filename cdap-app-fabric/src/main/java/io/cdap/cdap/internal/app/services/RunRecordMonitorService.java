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

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintain and return total number of launching and running run-records. This class is used by
 * flow-control mechanism for launch requests. It also has a cleanup mechanism to automatically
 * remove old (i.e., configurable) entries from the counter as a safe-guard mechanism.
 */
public class RunRecordMonitorService extends AbstractScheduledService {

  private static final Logger LOG = LoggerFactory.getLogger(RunRecordMonitorService.class);

  /**
   * Contains ProgramRunIds of runs that have been accepted, but have not been added to metadata
   * store plus all run records with {@link ProgramRunStatus#PENDING} or {@link
   * ProgramRunStatus#STARTING} status.
   */
  private final BlockingQueue<ProgramRunId> launchingQueue;

  private final ProgramRuntimeService runtimeService;
  private final long ageThresholdSec;
  private final CConfiguration cConf;
  private final MetricsCollectionService metricsCollectionService;
  private final int maxConcurrentRuns;
  private ScheduledExecutorService executor;

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
      MetricsCollectionService metricsCollectionService) {
    this.cConf = cConf;
    this.runtimeService = runtimeService;
    this.metricsCollectionService = metricsCollectionService;

    this.launchingQueue =
        new PriorityBlockingQueue<>(
            128, Comparator.comparingLong(o -> RunIds.getTime(o.getRun(), TimeUnit.MILLISECONDS)));
    this.ageThresholdSec = cConf.getLong(Constants.AppFabric.MONITOR_RECORD_AGE_THRESHOLD_SECONDS);
    this.maxConcurrentRuns = cConf.getInt(Constants.AppFabric.MAX_CONCURRENT_RUNS);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("RunRecordMonitorService started.");
  }

  @Override
  protected void shutDown() throws Exception {
    if (executor != null) {
      executor.shutdownNow();
    }
    LOG.info("RunRecordMonitorService successfully shut down.");
  }

  @Override
  protected void runOneIteration() throws Exception {
    cleanupQueue();
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(
        0, cConf.getInt(Constants.AppFabric.MONITOR_CLEANUP_INTERVAL_SECONDS), TimeUnit.SECONDS);
  }

  @Override
  protected final ScheduledExecutorService executor() {
    executor =
        Executors.newSingleThreadScheduledExecutor(
            Threads.createDaemonThreadFactory("run-record-monitor-service-cleanup-scheduler"));
    return executor;
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
    int launchingCount = launchingQueue.size();
    int runningCount = getProgramsRunningCount();

    return new Counter(launchingCount, runningCount);
  }

  /**
   * Add a new in-flight launch request.
   *
   * @param programRunId run id associated with the launch request
   */
  public int addRequest(ProgramRunId programRunId) {
    int result;
    synchronized (launchingQueue) {
      launchingQueue.add(programRunId);
      result = launchingQueue.size();
    }
    emitMetrics(Constants.Metrics.FlowControl.LAUNCHING_COUNT, result);
    LOG.info("Added request with runId {}.", programRunId);
    return result;
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
    if (launchingQueue.remove(programRunId)) {
      LOG.info(
          "Removed request with runId {}. Counter has {} concurrent launching requests.",
          programRunId,
          launchingQueue.size());
      emitMetrics(Constants.Metrics.FlowControl.LAUNCHING_COUNT, launchingQueue.size());
    }

    if (emitRunningChange) {
      emitMetrics(Constants.Metrics.FlowControl.RUNNING_COUNT, getProgramsRunningCount());
    }
  }

  public void emitLaunchingMetrics(long value) {
    emitMetrics(Constants.Metrics.FlowControl.LAUNCHING_COUNT, value);
  }

  private void emitMetrics(String metricName, long value) {
    metricsCollectionService.getContext(Collections.emptyMap()).gauge(metricName, value);
  }

  private void cleanupQueue() {
    while (true) {
      ProgramRunId programRunId = launchingQueue.peek();
      if (programRunId == null
          || RunIds.getTime(programRunId.getRun(), TimeUnit.MILLISECONDS) + (ageThresholdSec * 1000)
              >= System.currentTimeMillis()) {
        // Queue is empty or queue head has not expired yet.
        break;
      }
      // Queue head might have already been removed. So instead of calling poll, we call remove.
      if (launchingQueue.remove(programRunId)) {
        LOG.info("Removing request with runId {} due to expired retention time.", programRunId);
        emitMetrics(Constants.Metrics.FlowControl.LAUNCHING_COUNT, launchingQueue.size());
      }
    }

    emitMetrics(Constants.Metrics.FlowControl.RUNNING_COUNT, getProgramsRunningCount());
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
