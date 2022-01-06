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

import java.util.List;
import java.util.Set;

/**
 * Helper class to maintain and return total number of launching and running run-records.
 */
public class RunRecordCounter {
  /**
   * Contains ProgramRunIds of runs that have been accepted, but have not been added to metadata store plus
   * all run records with {@link ProgramRunStatus#PENDING} status.
   */
  private final Set<ProgramRunId> inflightAndPendings;
  private final ProgramRuntimeService runtimeService;

  public RunRecordCounter(ProgramRuntimeService runtimeService) {
    this.runtimeService = runtimeService;

    inflightAndPendings = new ConcurrentHashSet<>();
  }

  /**
   * Add a new inflight launch request and return total number of launching and running program runs.
   *
   * @param programRunId run id associated with the launch request
   * @return total number of launching and running program runs.
   */
  public Count addRequestAndGetCount(ProgramRunId programRunId) {
    int inflightAndPendingCount;
    synchronized (this) {
      inflightAndPendings.add(programRunId);
      inflightAndPendingCount = inflightAndPendings.size();
    }

    List<ProgramRuntimeService.RuntimeInfo> list = runtimeService
      .listAll(ProgramType.WORKFLOW,
               ProgramType.SERVICE,
               ProgramType.MAPREDUCE,
               ProgramType.SPARK,
               ProgramType.WORKER,
               ProgramType.CUSTOM_ACTION);

    int launchingCount = (int)
      list.stream().filter(r -> r.getController().getState().getRunStatus() == ProgramRunStatus.STARTING).count()
      + inflightAndPendingCount;

    int runningCount = (int)
      list.stream().filter(r -> isRunning(r.getController().getState().getRunStatus())).count()
      + launchingCount;

    return new Count(launchingCount, runningCount);
  }

  public void removeRequest(ProgramRunId programRunId) {
    inflightAndPendings.remove(programRunId);
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
     * total number of run records with PENDING status +
     * total number of run records with STARTING status
     */
    int launchingCount;

    /**
     * Total number of run records with RUNNING status +
     * Total number of run records with SUSPENDED status +
     * Total number of run records with RESUMING status
     */
    int runningCount;

    Count(int launchingCount, int runningCount) {
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
