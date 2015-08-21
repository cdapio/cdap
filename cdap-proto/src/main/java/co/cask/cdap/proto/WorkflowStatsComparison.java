/*
 * Copyright © 2015 Cask Data, Inc.
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
package co.cask.cdap.proto;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Returns an object that has a map from WorkflowRunId to time and a list of metrics per ProgramRun
 * which are further divided into run that the action took in a specific workflow.
 */
public final class WorkflowStatsComparison {
  private final Map<String, Long> startTimes;
  private final Collection<ProgramNodes> programNodesList;

  public WorkflowStatsComparison(Map<String, Long> startTimes, Collection<ProgramNodes> programNodesList) {
    this.startTimes = startTimes;
    this.programNodesList = programNodesList;
  }

  public Map<String, Long> getStartTimes() {
    return startTimes;
  }

  public Collection<ProgramNodes> getProgramNodesList() {
    return programNodesList;
  }

  /**
   * Contains information about a Program and the statistics that correspond to it
   * divided per run of the workflow that the program ran in.
   */
  public static final class ProgramNodes {
    private final String programName;
    private final List<WorkflowProgramDetails> workflowProgramDetailsList;
    private final ProgramType programType;

    public ProgramNodes(String programName, ProgramType programType,
                        List<WorkflowProgramDetails> workflowProgramDetailsList) {
      this.programName = programName;
      this.programType = programType;
      this.workflowProgramDetailsList = workflowProgramDetailsList;
    }

    public void addWorkflowDetails(String workflowRunId, String programRunId, long programStartTime,
                                   Map<String, Long> metrics) {
      workflowProgramDetailsList.add(new WorkflowProgramDetails(workflowRunId, programRunId,
                                                                programStartTime, metrics));
    }

    public String getProgramName() {
      return programName;
    }

    public List<WorkflowProgramDetails> getWorkflowProgramDetailsList() {
      return workflowProgramDetailsList;
    }

    public ProgramType getProgramType() {
      return programType;
    }

    /**
     * Contains information of Workflow Runs and the metrics of the program associated with it.
     */
    public static final class WorkflowProgramDetails {
      private final String workflowRunId;
      private final String programRunId;
      private final long programRunStart;
      private final Map<String, Long> metrics;

      public WorkflowProgramDetails(String workflowRunId, String programRunId,
                                    long programRunStart, Map<String, Long> metrics) {
        this.workflowRunId = workflowRunId;
        this.metrics = metrics;
        this.programRunId = programRunId;
        this.programRunStart = programRunStart;
      }

      public String getProgramRunId() {
        return programRunId;
      }

      public long getProgramRunStart() {
        return programRunStart;
      }

      public String getWorkflowRunId() {
        return workflowRunId;
      }

      public Map<String, Long> getMetrics() {
        return metrics;
      }
    }
  }
}
