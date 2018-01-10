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

package co.cask.cdap.api.app;

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.mapreduce.MapReduce;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.api.worker.WorkerSpecification;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.internal.dataset.DatasetCreationSpec;
import co.cask.cdap.internal.schedule.ScheduleCreationSpec;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Application Specification used in core code.
 */
public interface ApplicationSpecification {

  /**
   * @return Name of the Application.
   */
  String getName();

  /**
   * @return Version of the Application.
   */
  String getAppVersion();

  /**
   * @return Configuration string used during the creation of the Application.
   */
  @Nullable
  String getConfiguration();

  /**
   * @return Description of the Application.
   */
  String getDescription();

  /**
   * @return ID of the artifact used to create the application.
   */
  ArtifactId getArtifactId();

  /**
   * @return An immutable {@link Map} from {@link Stream} name to {@link StreamSpecification}
   *         for {@link Stream}s configured for the Application.
   */
  Map<String, StreamSpecification> getStreams();

  /**
   * @return An immutable {@link Map} from {@link DatasetModule} name to {@link DatasetModule} class name for
   *         dataset modules configured for the Application.
   */
  Map<String, String> getDatasetModules();

  /**
   * @return An immutable {@link Map} from {@link Dataset} name to {@link DatasetCreationSpec} for {@link Dataset}s
   *         configured for the Application.
   */
  Map<String, DatasetCreationSpec> getDatasets();

  /**
   * @return An immutable {@link Map} from {@link Flow} name to {@link FlowSpecification}
   *         for {@link Flow}s configured for the Application.
   */
  Map<String, FlowSpecification> getFlows();

  /**
   * @return An immutable {@link Map} from {@link MapReduce} name to {@link MapReduceSpecification}
   *         for {@link MapReduce} jobs configured for the Application.
   */
  Map<String, MapReduceSpecification> getMapReduce();

  /**
   * @return An immutable {@link Map} from {@link Spark} name to
   * {@link SparkSpecification} for {@link Spark} jobs configured for the Application.
   */
  Map<String, SparkSpecification> getSpark();

  /**
   * @return An immutable {@link Map} from {@link Workflow} name to {@link WorkflowSpecification}
   *         for {@link Workflow}s configured for the Application.
   */
  Map<String, WorkflowSpecification> getWorkflows();

  /**
   * @return An immutable {@link Map} from service name to {@link ServiceSpecification}
   *         for services configured for the Application.
   */
  Map<String, ServiceSpecification> getServices();

  /**
   * @return An immutable {@link Map} from worker name to {@link WorkerSpecification}
   *         for workers configured for the Application.
   */
  Map<String, WorkerSpecification> getWorkers();

  /**
   * @return An immutable {@link Map} from Schedule name to {@link ScheduleCreationSpec}
   */
  Map<String, ScheduleCreationSpec> getProgramSchedules();

  /**
   * @return An immutable {@link Map} from plugin id to {@link Plugin}
   */
  Map<String, Plugin> getPlugins();
}
