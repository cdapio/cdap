/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

import io.cdap.cdap.api.ClientLocalizationContext;
import io.cdap.cdap.api.ProgramState;
import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.RuntimeContext;
import io.cdap.cdap.api.SchedulableProgramContext;
import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.api.Transactional;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.lineage.field.LineageRecorder;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.metadata.MetadataWriter;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.workflow.WorkflowInfoProvider;

/**
 * MapReduce job execution context.
 */
public interface MapReduceContext extends SchedulableProgramContext, RuntimeContext, DatasetContext, ServiceDiscoverer,
  Transactional, PluginContext, ClientLocalizationContext, WorkflowInfoProvider, SecureStore, MessagingContext,
  LineageRecorder, MetadataReader, MetadataWriter {

  /**
   * @return The specification used to configure this {@link MapReduce} job instance.
   */
  MapReduceSpecification getSpecification();

  /**
   * Returns the logical start time of this MapReduce job. Logical start time is the time when this MapReduce
   * job is supposed to start if this job is started by the scheduler. Otherwise it would be the current time when the
   * job runs.
   *
   * @return Time in milliseconds since epoch time (00:00:00 January 1, 1970 UTC).
   */
  long getLogicalStartTime();

  /**
   * Returns the actual Hadoop job.
   */
  <T> T getHadoopJob();

  /**
   * Updates the input configuration of this MapReduce job to use the specified {@link Input}.
   * @param input the input to be used
   * @throws IllegalStateException if called after any setInput methods.
   */
  void addInput(Input input);

  /**
   * Updates the input configuration of this MapReduce job to use the specified {@link Input}.
   * @param input the input to be used
   * @param mapperCls the mapper class to be used for the input
   * @throws IllegalStateException if called after any setInput methods.
   */
  void addInput(Input input, Class<?> mapperCls);

  /**
   * Updates the output configuration of this MapReduce job to use the specified {@link Output}.
   * @param output the output to be used
   */
  void addOutput(Output output);

  /**
   * Overrides the resources, such as memory and virtual cores, to use for each mapper of this MapReduce job.
   *
   * @param resources Resources that each mapper should use.
   */
  void setMapperResources(Resources resources);

  /**
   * Override the resources, such as memory and virtual cores, to use for each reducer of this MapReduce job.
   *
   * @param resources Resources that each reducer should use.
   */
  void setReducerResources(Resources resources);

  /**
   * Return the state of the MapReduce program.
   */
  ProgramState getState();
}
