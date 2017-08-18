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
   * Return the state of the Spark program.
   */
  ProgramState getState();
}
