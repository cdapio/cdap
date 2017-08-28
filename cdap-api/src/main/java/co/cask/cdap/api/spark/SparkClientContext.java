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

package co.cask.cdap.api.spark;

import co.cask.cdap.api.ClientLocalizationContext;
import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.ProgramState;
import co.cask.cdap.api.Resources;
import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.api.SchedulableProgramContext;
import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.messaging.MessagingContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.workflow.WorkflowInfoProvider;

import java.net.URI;

/**
 * A context for a {@link Spark} program to interact with CDAP. This context object will be provided to
 * {@link Spark} program in the {@link ProgramLifecycle#initialize} call.
 */
@Beta
public interface SparkClientContext extends SchedulableProgramContext, RuntimeContext, DatasetContext,
  ClientLocalizationContext, Transactional, ServiceDiscoverer, PluginContext, WorkflowInfoProvider,
  SecureStore, MessagingContext {

  /**
   * @return The specification used to configure this {@link Spark} job instance.
   */
  SparkSpecification getSpecification();

  /**
   * Returns the logical start time of this Spark job. Logical start time is the time when this Spark
   * job is supposed to start if this job is started by the scheduler. Otherwise it would be the current time when the
   * job runs.
   *
   * @return Time in milliseconds since epoch time (00:00:00 January 1, 1970 UTC).
   */
  long getLogicalStartTime();

  /**
   * Returns a {@link Metrics} which can be used to emit custom metrics.
   *
   * @return a {@link Metrics} for the {@link Spark} program
   */
  Metrics getMetrics();

  /**
   * Sets the resources requirement for the Spark driver process.
   *
   * @param resources Resources that the driver should use
   */
  void setDriverResources(Resources resources);

  /**
   * Sets the resources, such as memory and virtual cores, to use for each executor process for the Spark program.
   *
   * @param resources Resources that each executor should use
   */
  void setExecutorResources(Resources resources);

  /**
   * Sets a
   * <a href="http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.SparkConf">SparkConf</a>
   * to be used for the Spark execution. Only configurations set inside the
   * {@link ProgramLifecycle#initialize} call will affect the Spark execution.
   *
   * @param <T> the SparkConf type
   */
  <T> void setSparkConf(T sparkConf);

  /**
   * Sets a python script to run using PySpark.
   *
   * @see #setPySparkScript(String, Iterable)
   */
  void setPySparkScript(String script, URI...additionalPythonFiles);

  /**
   * Sets a python script to run using PySpark. If this is set, it overrides whatever being set via
   * the {@link SparkConfigurer#setMainClassName(String)} and the python script is the one always get executed.
   *
   * @param script the python script to run using PySpark
   * @param additionalPythonFiles a list of addition python files to be included in the {@code PYTHONPATH}.
   *                              Each can be local by having {@code file} scheme or remote,
   *                              for example, by having {@code hdfs} or {@code http} scheme.
   *                              If the {@link URI} has no scheme, it will be default based on the execution
   *                              environment, which would be local file in local sandbox, and remote if running
   *                              in distributed mode. Note that in distributed mode, using {@code file} scheme
   *                              means the file has to be present on every node on the cluster, since the
   *                              spark program can be submitted from any node.
   */
  void setPySparkScript(String script, Iterable<URI> additionalPythonFiles);

  /**
   * Sets a location that points to a python script to run using PySpark.
   *
   * @see #setPySparkScript(URI, Iterable)
   */
  void setPySparkScript(URI scriptLocation, URI...additionalPythonFiles);

  /**
   * Sets a location that points to a python script to run using PySpark.
   * If this is set, it overrides whatever being set via the {@link SparkConfigurer#setMainClassName(String)}
   * and the python script is the one always get executed.
   *
   * @param scriptLocation location to the python script. It can be local by having {@code file} scheme or remote,
   *                       for example, by having {@code hdfs} or {@code http} scheme.
   *                       If the {@link URI} has no scheme, it will be default based on the execution
   *                       environment, which would be local file in local sandbox, and remote if running
   *                       in distributed mode. Note that in distributed mode, using {@code file} scheme
   *                       means the file has to be present on every node on the cluster, since the
   *                       spark program can be submitted from any node.
   * @param additionalPythonFiles a list of addition python files to be included in the {@code PYTHONPATH}.
   *                              Each location can be local or remote,
   *                              with the same definition as the {@code scriptLocation} parameter.
   */
  void setPySparkScript(URI scriptLocation, Iterable<URI> additionalPythonFiles);

  /**
   * Return the state of the Spark program.
   */
  ProgramState getState();
}
