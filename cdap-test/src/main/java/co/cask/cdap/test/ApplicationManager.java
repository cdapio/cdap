/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.test;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;

import java.util.List;
import java.util.Map;

/**
 * Instance of this class is for managing deployed application.
 */
public interface ApplicationManager {

  /**
   * Returns a ProgramManager, without starting the program
   * @param flowName Name of the flow
   * @return A {@link FlowManager} for controlling the flow
   */
  FlowManager getFlowManager(String flowName);

  /**
   * Returns a ProgramManager, without starting the program
   * @param programName Name of the program
   * @return A {@link MapReduceManager} for controlling the mapreduce job
   */
  MapReduceManager getMapReduceManager(String programName);

  /**
   * Returns a ProgramManager, without starting the program
   * @param jobName Name of the job
   * @return A {@link SparkManager} for controlling the spark job
   */
  SparkManager getSparkManager(String jobName);

  /**
   * Returns a ProgramManager, without starting the program
   * @param workflowName Name of the workflow
   * @return A {@link WorkflowManager} for controlling the workflow
   */
  WorkflowManager getWorkflowManager(String workflowName);

  /**
   * Returns a ProgramManager, without starting the program
   * @param serviceName Name of the service
   * @return A {@link ServiceManager} for controlling the service
   */
  ServiceManager getServiceManager(String serviceName);

  /**
   * Returns a ProgramManager, without starting the program
   * @param workerName Name of the worker
   * @return A {@link WorkerManager} for controlling the worker
   */
  WorkerManager getWorkerManager(String workerName);

  /**
   * Stops all processors managed by this manager and clear all associated runtime metrics.
   */
  void stopAll();

  /**
   * Stops a particular program.
   * @param programId the program to stop
   */
  void stopProgram(Id.Program programId);

  /**
   * Starts a particular program.
   * @param programId the program to start
   */
  void startProgram(Id.Program programId);

  /**
   * Starts a particular program with arguments.
   * @param programId the program to start
   */
  void startProgram(Id.Program programId, Map<String, String> arguments);

  /**
   * Checks whether a particular program is running or not.
   * @param programId the program to check
   * @return true if the program is running; false otherwise.
   */
  boolean isRunning(Id.Program programId);

  /**
   * Gets the history of the program
   * @return list of {@link RunRecord} history
   */
  List<RunRecord> getHistory(Id.Program programId, ProgramRunStatus status);
}
