/*
 * Copyright © 2014 Cask Data, Inc.
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

import java.util.Map;

/**
 * Instance of this class is for managing deployed application.
 */
public interface ApplicationManager {

  /**
   * Starts a flow.
   * @param flowName Name of the flow to start.
   * @return A {@link co.cask.cdap.test.FlowManager} for controlling the started flow.
   */
  FlowManager startFlow(String flowName);

  /**
   * Starts a flow.
   * @param flowName Name of the flow to start.
   * @param arguments Arguments to be passed while starting a flow.
   * @return A {@link co.cask.cdap.test.FlowManager} for controlling the started flow.
   */
  FlowManager startFlow(String flowName, Map<String, String> arguments);

  /**
   * Starts a mapreduce job.
   * @param jobName Name of the mapreduce job to start.
   * @return A {@link co.cask.cdap.test.MapReduceManager} for controlling the started mapreduce job.
   */
  MapReduceManager startMapReduce(String jobName);

  /**
   * Starts a mapreduce job.
   * @param jobName Name of the mapreduce job to start.
   * @param arguments Arguments to be passed while starting a mapreduce.
   * @return A {@link co.cask.cdap.test.MapReduceManager} for controlling the started mapreduce job.
   */
  MapReduceManager startMapReduce(String jobName, Map<String, String> arguments);

  /**
   * Starts a Spark job.
   * @param jobName Name of the spark job to start.
   * @return A {@link SparkManager} for controlling the started spark job.
   */
  SparkManager startSpark(String jobName);

  /**
   * Starts a Spark job.
   * @param jobName Name of the spark job to start.
   * @param arguments Arguments to be passed while starting a spark.
   * @return A {@link SparkManager} for controlling the started spark job.
   */
  SparkManager startSpark(String jobName, Map<String, String> arguments);

  /**
   * Starts a procedure.
   * @param procedureName Name of the procedure to start.
   * @return A {@link co.cask.cdap.test.ProcedureManager} for controlling the started procedure.
   * @deprecated As of version 2.6.0,  replaced by {@link co.cask.cdap.api.service.Service}
   */
  @Deprecated
  ProcedureManager startProcedure(String procedureName);

  /**
   * Starts a procedure.
   * @param procedureName Name of the procedure to start.
   * @param arguments Arguments to be passed while starting a procedure.
   * @return A {@link co.cask.cdap.test.ProcedureManager} for controlling the started procedure.
   * @deprecated As of version 2.6.0,  replaced by {@link co.cask.cdap.api.service.Service}
   */
  @Deprecated
  ProcedureManager startProcedure(String procedureName, Map<String, String> arguments);

  /**
   * Gets a {@link co.cask.cdap.test.StreamWriter} for writing data to the given stream.
   * @param streamName Name of the stream to write to.
   * @return A {@link co.cask.cdap.test.StreamWriter}.
   */
  StreamWriter getStreamWriter(String streamName);

  /**
   * Gets an instance of {@link co.cask.cdap.api.dataset.Dataset} of the given dataset name.
   * Operations on the returned {@link co.cask.cdap.api.dataset.Dataset} always performed synchronously,
   * i.e. no support for multi-operations transaction.
   * @param dataSetName Name of the dataset to retrieve.
   * @param <T> Type of the dataset.
   * @return A {@link co.cask.cdap.test.DataSetManager} instance.
   */
  <T> DataSetManager<T> getDataSet(String dataSetName);

  /**
   * Stops all processors managed by this manager and clear all associated runtime metrics.
   */
  void stopAll();

  /**
   * Starts a workflow.
   * @param workflowName
   * @param arguments
   * @return {@link co.cask.cdap.test.WorkflowManager} for controlling the started workflow.
   */
  WorkflowManager startWorkflow(String workflowName, Map<String, String> arguments);

  /**
   * Starts a service.
   * @param serviceName Name of the service to be started.
   * @return {@link co.cask.cdap.test.ServiceManager} to control the running service.
   */
  ServiceManager startService(String serviceName);

  /**
   * Starts a service.
   * @param serviceName Name of the service to be started.
   * @param arguments Arguments to be passed for the service.
   * @return {@link co.cask.cdap.test.ServiceManager} to control the running service.
   */
  ServiceManager startService(String serviceName, Map<String, String> arguments);

}
