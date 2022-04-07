/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.test;

import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.PluginInstanceDetail;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramStatus;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ScheduleId;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Instance of this class is for managing deployed application.
 */
public interface ApplicationManager {

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
   * Returns the list of plugins used in the application.
   * @return list of plugins
   */
  List<PluginInstanceDetail> getPlugins();

  /**
   * Stops all processors managed by this manager and clear all associated runtime metrics.
   */
  void stopAll();

  /**
   * Stops a particular program.
   * @param programId the program to stop
   * @param gracefulShutdownSecs amount of seconds to wait for graceful shutdown before killing the run
   */
  void stopProgram(Id.Program programId, @Nullable String gracefulShutdownSecs);

  /**
   * Stops a particular program.
   * @param programId the program to stop
   * @param gracefulShutdownSecs amount of seconds to wait for graceful shutdown before killing the run
   */
  void stopProgram(ProgramId programId, @Nullable String gracefulShutdownSecs);

  /**
   * Wait for a given programId to have no running run record
   * @param programId the program id to wait on
   */
  void waitForStopped(ProgramId programId) throws Exception;

  /**
   * Starts a particular program.
   * @param programId the program to start
   */
  void startProgram(Id.Program programId);

  /**
   * Starts a particular program.
   * @param programId the program to start
   */
  void startProgram(ProgramId programId);

  /**
   * Starts a particular program with arguments.
   * @param programId the program to start
   */
  void startProgram(Id.Program programId, Map<String, String> arguments);

  /**
   * Starts a particular program with arguments.
   * @param programId the program to start
   */
  void startProgram(ProgramId programId, Map<String, String> arguments);

  /**
   * Checks whether a particular program is running or not.
   * @param programId the program to check
   * @return true if the program is running; false otherwise.
   */
  boolean isRunning(Id.Program programId);

  /**
   * Checks whether a particular program is in the {@link ProgramStatus#RUNNING} state.
   * @param programId the program to check
   * @return true if the program is running; false otherwise.
   */
  boolean isRunning(ProgramId programId);

  /**
   * Checks whether a particular program is in the {@link ProgramStatus#STOPPED} state.
   * @param programId the program to check
   * @return true if the program is running; false otherwise.
   */
  boolean isStopped(ProgramId programId);

  /**
   * Gets the history of the program
   * @return list of {@link RunRecord} history
   */
  List<RunRecord> getHistory(Id.Program programId, ProgramRunStatus status);

  /**
   * Gets the history of the program
   * @return list of {@link RunRecord} history
   */
  List<RunRecord> getHistory(ProgramId programId, ProgramRunStatus status);

  /**
   * Adds a schedule to the app.
   *
   * @param scheduleDetail the schedule to be added.
   */
  void addSchedule(ScheduleDetail scheduleDetail) throws Exception;

  /**
   * Enable a schedule in an app.
   *
   * @param scheduleId the id of the schedule to be enabled.
   */
  void enableSchedule(ScheduleId scheduleId) throws Exception;

  /**
   * Updates this application
   *
   * @param appRequest the {@link AppRequest} to update the application with
   */
  void update(AppRequest appRequest) throws Exception;

  /**
   * Deletes the application;
   */
  void delete() throws Exception;

  /**
   * Upgrades the application.
   *
   * @throws Exception
   */
  void upgrade() throws Exception;

  /**
   * Upgrades the application.
   *
   * @param artifactScopes Scopes in which to look for artifacts for upgrade. If null, then search in all scopes.
   * @param allowSnapshot Consider snapshot version of artifacts for upgrade or not.
   * @throws Exception
   */
  void upgrade(Set<String> artifactScopes, boolean allowSnapshot) throws Exception;

  /**
   * Returns the application's details.
   */
  ApplicationDetail getInfo() throws Exception;

  /**
   * Save runtime arguments for the specified program for all runs.
   *
   * @param programId the {@link ProgramId program} to save runtime arguments for
   * @param args the arguments to save
   */
  void setRuntimeArgs(ProgramId programId, Map<String, String> args) throws Exception;

  /**
   * Gets runtime arguments of the specified program.
   *
   * @param programId the {@link ProgramId program} to get runtime arguments for
   * @return args the arguments
   */
  Map<String, String> getRuntimeArgs(ProgramId programId) throws Exception;
}
