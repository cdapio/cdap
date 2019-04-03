/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.cdap.logging.context;

import co.cask.cdap.proto.ProgramType;

/**
 * Logging context to be used by programs running inside Workflow.
 */
public class WorkflowProgramLoggingContext extends WorkflowLoggingContext {
  public static final String TAG_WORKFLOW_MAP_REDUCE_ID = ".workflowMapReduceId";
  public static final String TAG_WORKFLOW_SPARK_ID = ".workflowSparkId";
  public static final String TAG_WORKFLOW_PROGRAM_RUN_ID = ".workflowProgramRunId";

  /**
   * Constructs logging context.
   *
   * @param namespaceId namespace id
   * @param applicationId application id
   * @param workflowId workflow id
   * @param runId run id of the application
   * @param programType type of the program. Currently only MapReduce and Spark is supported.
   * @param programName name of the program
   * @param programRunId run id of the program launched through workflow
   */
  public WorkflowProgramLoggingContext(String namespaceId, String applicationId, String workflowId, String runId,
                                       ProgramType programType, String programName, String programRunId) {
    super(namespaceId, applicationId, workflowId, runId);
    setSystemTag(TAG_WORKFLOW_PROGRAM_RUN_ID, programRunId);
    switch (programType) {
      case MAPREDUCE:
        setSystemTag(TAG_WORKFLOW_MAP_REDUCE_ID, programName);
        break;
      case SPARK:
        setSystemTag(TAG_WORKFLOW_SPARK_ID, programName);
        break;
      default:
        throw new IllegalArgumentException(String.format("Program type %s is not supported by Workflow.", programType));
    }
  }
}
