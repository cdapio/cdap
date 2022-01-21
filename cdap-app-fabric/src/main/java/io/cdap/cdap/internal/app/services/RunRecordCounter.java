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

import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

/**
 * Helper class to maintain and return total number of launching and running run-records.
 */
public class RunRecordCounter {
  private static final Logger LOG = LoggerFactory.getLogger(RunRecordCounter.class);

  /**
   * Contains ProgramRunIds of runs that have been accepted, but have not been added to metadata store plus
   * all run records with {@link ProgramRunStatus#PENDING} or {@link ProgramRunStatus#STARTING} status.
   */
  private final Set<ProgramRunId> launchingSet;
  private final ProgramRuntimeService runtimeService;

  public RunRecordCounter(ProgramRuntimeService runtimeService) {
    this.runtimeService = runtimeService;

    launchingSet = new ConcurrentHashSet<>();
  }

  /**
   * Add a new inflight launch request and return total number of launching and running program runs.
   *
   * @param programRunId run id associated with the launch request
   * @return total number of launching and running program runs.
   */
  public Count addRequestAndGetCount(ProgramRunId programRunId) {
    int launchingCount;
    synchronized (this) {
      addRequest(programRunId);
      launchingCount = launchingSet.size();
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
    launchingSet.add(programRunId);
    LOG.info("Added request with runId {}.", programRunId);
  }

  /**
   * Remove the request with the provided programRunId when the request is no longer lunching.
   * I.e., not in-flight, not {@link ProgramRunStatus#PENDING} and not {@link ProgramRunStatus#STARTING}
   *
   * @param programRunId
   */
  public void removeRequest(ProgramRunId programRunId) {
    if (launchingSet.contains(programRunId)) {
      launchingSet.remove(programRunId);
      LOG.info("Removed request with runId {}. Counter has {} concurrent launching requests.",
                programRunId, launchingSet.size());
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
