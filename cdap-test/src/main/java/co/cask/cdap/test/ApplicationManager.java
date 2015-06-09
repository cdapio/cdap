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

import java.util.Map;

/**
 * Instance of this class is for managing deployed application.
 */
public interface ApplicationManager {

  /**
   * Starts a flow.
   * @param flowName Name of the flow to start.
   * @return A {@link FlowManager} for controlling the started flow.
   *
   * @deprecated Use {@link FlowManager#start()}
   */
  @Deprecated
  FlowManager startFlow(String flowName);

  /**
   * Starts a flow.
   * @param flowName Name of the flow to start.
   * @param arguments Arguments to be passed while starting a flow.
   * @return A {@link FlowManager} for controlling the started flow.
   *
   * @deprecated Use {@link FlowManager#start(Map<String, String>)}
   */
  @Deprecated
  FlowManager startFlow(String flowName, Map<String, String> arguments);

  /**
   * Returns a ProgramManager, without starting the program
   * @param flowName Name of the flow
   * @return A {@link FlowManager} for controlling the flow
   */
  FlowManager getFlowManager(String flowName);

  /**
   * Starts a mapreduce job.
   * @param jobName Name of the mapreduce job to start.
   * @return A {@link MapReduceManager} for controlling the started mapreduce job.
   *
   * @deprecated Use {@link MapReduceManager#start()}
   */
  @Deprecated
  MapReduceManager startMapReduce(String jobName);

  /**
   * Starts a mapreduce job.
   * @param jobName Name of the mapreduce job to start.
   * @param arguments Arguments to be passed while starting a mapreduce.
   * @return A {@link MapReduceManager} for controlling the started mapreduce job.
   *
   * @deprecated Use {@link MapReduceManager#start(Map<String, String>)}
   */
  @Deprecated
  MapReduceManager startMapReduce(String jobName, Map<String, String> arguments);

  /**
   * Returns a ProgramManager, without starting the program
   * @param programName Name of the program
   * @return A {@link MapReduceManager} for controlling the mapreduce job
   */
  MapReduceManager getMapReduceManager(String programName);

  /**
   * Starts a Spark job.
   * @param jobName Name of the spark job to start.
   * @return A {@link SparkManager} for controlling the started spark job.
   *
   * @deprecated Use {@link SparkManager#start()}
   */
  @Deprecated
  SparkManager startSpark(String jobName);

  /**
   * Starts a Spark job.
   * @param jobName Name of the spark job to start.
   * @param arguments Arguments to be passed while starting a spark.
   * @return A {@link SparkManager} for controlling the started spark job.
   *
   * @deprecated Use {@link SparkManager#start(Map<String, String>)}
   */
  @Deprecated
  SparkManager startSpark(String jobName, Map<String, String> arguments);

  /**
   * Returns a ProgramManager, without starting the program
   * @param jobName Name of the job
   * @return A {@link SparkManager} for controlling the spark job
   */
  SparkManager getSparkManager(String jobName);

  /**
   * Starts a workflow.
   * @param workflowName Name of the workflow to start
   * @param arguments arguments to be passed to the workflow
   * @return {@link WorkflowManager} for controlling the started workflow.
   *
   * @deprecated Use {@link WorkflowManager#start(Map<String, String>)}
   */
  @Deprecated
  WorkflowManager startWorkflow(String workflowName, Map<String, String> arguments);

  /**
   * Returns a ProgramManager, without starting the program
   * @param workflowName Name of the workflow
   * @return A {@link WorkflowManager} for controlling the workflow
   */
  WorkflowManager getWorkflowManager(String workflowName);

  /**
   * Starts a service.
   * @param serviceName Name of the service to be started.
   * @return {@link ServiceManager} to control the running service.
   *
   * @deprecated Use {@link ServiceManager#start()}
   */
  @Deprecated
  ServiceManager startService(String serviceName);

  /**
   * Starts a service.
   * @param serviceName Name of the service to be started.
   * @param arguments Arguments to be passed for the service.
   * @return {@link ServiceManager} to control the running service.
   *
   * @deprecated Use {@link ServiceManager#start(Map<String, String>)}
   */
  @Deprecated
  ServiceManager startService(String serviceName, Map<String, String> arguments);

  /**
   * Returns a ProgramManager, without starting the program
   * @param serviceName Name of the service
   * @return A {@link ServiceManager} for controlling the service
   */
  ServiceManager getServiceManager(String serviceName);

  /**
   * Starts a worker.
   * @param workerName name of the worker to be started
   * @return {@link WorkerManager} to control the running worker
   *
   * @deprecated Use {@link WorkerManager#start()}
   */
  @Deprecated
  WorkerManager startWorker(String workerName);

  /**
   * Starts a worker.
   * @param workerName name of the worker to be started
   * @param arguments arguments to be passed to the worker
   * @return {@link WorkerManager} to control the running worker
   *
   * @deprecated Use {@link WorkerManager#start(Map<String, String>)}
   */
  @Deprecated
  WorkerManager startWorker(String workerName, Map<String, String> arguments);

  /**
   * Returns a ProgramManager, without starting the program
   * @param workerName Name of the worker
   * @return A {@link WorkerManager} for controlling the worker
   */
  WorkerManager getWorkerManager(String workerName);

  /**
   * Gets a {@link StreamWriter} for writing data to the given stream.
   * @param streamName Name of the stream to write to.
   * @return A {@link StreamWriter}.
   *
   * @deprecated use TestBase#getStreamManager(String streamName)
   */
  @Deprecated
  StreamWriter getStreamWriter(String streamName);

  /**
   * Gets an instance of {@link co.cask.cdap.api.dataset.Dataset} of the given dataset name.
   * Operations on the returned {@link co.cask.cdap.api.dataset.Dataset} always performed synchronously,
   * i.e. no support for multi-operations transaction.
   * @param dataSetName Name of the dataset to retrieve.
   * @param <T> Type of the dataset.
   * @return A {@link DataSetManager} instance.
   * @deprecated As of version 2.8.0, replaced by
   *             TestBase#getDataset(co.cask.cdap.proto.Id.Namespace, String)
   */
  @Deprecated
  <T> DataSetManager<T> getDataSet(String dataSetName) throws Exception;

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
}
