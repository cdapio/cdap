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
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Maintain and return total number of launching and running run-records.
 * This class is used by flow-control mechanism for launch requests.
 * It also has a cleanup mechanism to automatically remove old (i.e., configurable) entries from the counter as a
 * safe-guard mechanism.
 */
public class RunRecordMonitorService extends AbstractScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(RunRecordMonitorService.class);

  /**
   * Contains ProgramRunIds of runs that have been accepted, but have not been added to metadata store plus
   * all run records with {@link ProgramRunStatus#PENDING} or {@link ProgramRunStatus#STARTING} status.
   */
  private final BlockingQueue<ProgramRunId> launchingQueue;
  private final ProgramRuntimeService runtimeService;
  private final long ageThresholdSec;
  private final CConfiguration cConf;
  private ScheduledExecutorService executor;

  @Inject
  public RunRecordMonitorService(CConfiguration cConf, ProgramRuntimeService runtimeService) {
    this.runtimeService = runtimeService;
    this.cConf = cConf;

    launchingQueue = new PriorityBlockingQueue<>(128, Comparator.comparingLong(
      o -> RunIds.getTime(o.getRun(), TimeUnit.MILLISECONDS)));
    ageThresholdSec = cConf.getLong(Constants.AppFabric.MONITOR_RECORD_AGE_THRESHOLD_SECONDS);
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
    return Scheduler.newFixedRateSchedule(0,
                                          cConf.getInt(Constants.AppFabric.MONITOR_CLEANUP_INTERVAL_SECONDS),
                                          TimeUnit.SECONDS);
  }

  @Override
  protected final ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(
      Threads.createDaemonThreadFactory("run-record-monitor-service-cleanup-scheduler"));
    return executor;
  }

  /**
   * Add a new inflight launch request and return total number of launching and running program runs.
   *
   * @param programRunId run id associated with the launch request
   * @return total number of launching and running program runs.
   */
  public Count addRequestAndGetCount(ProgramRunId programRunId) throws Exception {
    if (RunIds.getTime(programRunId.getRun(), TimeUnit.MILLISECONDS) == -1) {
      throw new Exception("None time-based UUIDs are not supported");
    }

    int launchingCount;
    synchronized (this) {
      addRequest(programRunId);
      launchingCount = launchingQueue.size();
    }

    List<ProgramRuntimeService.RuntimeInfo> list = runtimeService
      .listAll(ProgramType.WORKFLOW,
               ProgramType.SERVICE,
               ProgramType.MAPREDUCE,
               ProgramType.SPARK,
               ProgramType.WORKER,
               ProgramType.CUSTOM_ACTION);

    int runsCount = (int)
      list.stream().filter(r -> isRunning(r.getController().getState().getRunStatus())).count()
      + launchingCount;

    LOG.info("Counter has {} concurrent launching and {} concurrent runs.", launchingCount, runsCount);
    return new Count(launchingCount, runsCount);
  }

  /**
   * Add a new inflight launch request.
   *
   * @param programRunId run id associated with the launch request
   */
  public void addRequest(ProgramRunId programRunId) {
    launchingQueue.add(programRunId);
    LOG.info("Added request with runId {}.", programRunId);
  }

  /**
   * Remove the request with the provided programRunId when the request is no longer lunching.
   * I.e., not in-flight, not {@link ProgramRunStatus#PENDING} and not {@link ProgramRunStatus#STARTING}
   *
   * @param programRunId
   */
  public void removeRequest(ProgramRunId programRunId) {
    if (launchingQueue.remove(programRunId)) {
      LOG.info("Removed request with runId {}. Counter has {} concurrent launching requests.",
               programRunId, launchingQueue.size());
    }
  }

  private void cleanupQueue() {
    while (true) {
      ProgramRunId programRunId = launchingQueue.peek();
      if (programRunId == null ||
        RunIds.getTime(programRunId.getRun(), TimeUnit.MILLISECONDS) + (ageThresholdSec * 1000) >=
          System.currentTimeMillis()) {
        //Queue is empty or queue head has not expired yet.
        return;
      }
      // Queue head might have already been removed. So instead of calling poll, we call remove.
      if (launchingQueue.remove(programRunId)) {
        LOG.info("Removing request with runId {} due to expired retention time.", programRunId);
      }

    }
  }

  private boolean isRunning(ProgramRunStatus status) {
    if (status == ProgramRunStatus.RUNNING
      || status == ProgramRunStatus.SUSPENDED
      || status == ProgramRunStatus.RESUMING) {
      return true;
    }

    return false;
  }

  class Count {
    /**
     * Total number of launch requests that have been accepted but still missing in metadata store +
     * total number of run records with {@link ProgramRunStatus#PENDING} status +
     * total number of run records with {@link ProgramRunStatus#STARTING} status
     */
    private final int launchingCount;

    /**
     * {@link this#launchingCount} +
     * Total number of run records with {@link ProgramRunStatus#RUNNING status +
     * Total number of run records with {@link ProgramRunStatus#SUSPENDED} status +
     * Total number of run records with {@link ProgramRunStatus#RESUMING} status
     */
    private final int runsCount;

    Count(int launchingCount, int runsCount) {
      this.launchingCount = launchingCount;
      this.runsCount = runsCount;
    }

    public int getLaunchingCount() {
      return launchingCount;
    }

    public int getRunsCount() {
      return runsCount;
    }
  }
}
