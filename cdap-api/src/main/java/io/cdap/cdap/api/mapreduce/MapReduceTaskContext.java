/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package io.cdap.cdap.api.mapreduce;

import io.cdap.cdap.api.RuntimeContext;
import io.cdap.cdap.api.SchedulableProgramContext;
import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.api.TaskLocalizationContext;
import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.data.batch.InputContext;
import io.cdap.cdap.api.data.batch.PartitionedFileSetInputContext;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.workflow.Workflow;
import io.cdap.cdap.api.workflow.WorkflowInfo;
import io.cdap.cdap.api.workflow.WorkflowToken;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * MapReduce task execution context.
 *
 * @param <KEYOUT>   output key type
 * @param <VALUEOUT> output value type
 */
@Beta
public interface MapReduceTaskContext<KEYOUT, VALUEOUT> extends SchedulableProgramContext, RuntimeContext,
  DatasetContext, ServiceDiscoverer, PluginContext, TaskLocalizationContext, SecureStore {

  /**
   * Write key and value to the named output Dataset. This method must only be used if the MapReduce writes to
   * more than one output. If there is a single output, {@link #write(Object, Object)} must be used, or the
   * data may not be written correctly.
   *
   * @param namedOutput the name of the output Dataset
   * @param key         the key
   * @param value       the value
   */
  <K, V> void write(String namedOutput, K key, V value) throws IOException, InterruptedException;

  /**
   * Write key and value to the Hadoop context. This method must only be used if the MapReduce writes to a single
   * output. If there is more than one output, {@link #write(String, Object, Object)} must be used instead.
   *
   * @param key         the key
   * @param value       the value
   */
  void write(KEYOUT key, VALUEOUT value) throws IOException, InterruptedException;

  /**
   * @return The specification used to configure this {@link MapReduce} job instance.
   */
  MapReduceSpecification getSpecification();

  /**
   * Returns the logical start time of this MapReduce job. Logical start time is the time when this MapReduce
   * job is supposed to start if this job is started by the scheduler. Otherwise, it is the current time when the
   * job runs.
   *
   * @return Time in milliseconds since epoch time (00:00:00 January 1, 1970 UTC).
   */
  long getLogicalStartTime();

  /**
   * Returns the Hadoop context object for the task.
   */
  <T> T getHadoopContext();

  /**
   * @return the {@link WorkflowToken} associated with the current {@link Workflow},
   * if the {@link MapReduce} program is executed as a part of the Workflow; returns {@code null} otherwise.
   */
  @Nullable
  WorkflowToken getWorkflowToken();

  /**
   * @return information about the enclosing {@link Workflow} run, if this {@link MapReduce} program is executed
   * as a part of the Workflow; returns {@code null} otherwise.
   */
  @Nullable
  WorkflowInfo getWorkflowInfo();

  /**
   * Returns an {@link InputContext} of the input configured for this task. The returned object will be a
   * {@link PartitionedFileSetInputContext} if the input for this task was configured from the PartitionedFileSet APIs.
   * Returns null, if this task is a Reducer or no inputs were configured through CDAP APIs.
   */
  InputContext getInputContext();
}
