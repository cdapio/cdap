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

package co.cask.cdap.app;

import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.app.ApplicationConfigurer;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.mapreduce.MapReduce;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.service.Service;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.api.worker.Worker;
import co.cask.cdap.api.worker.WorkerSpecification;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.internal.app.DefaultApplicationSpecification;
import co.cask.cdap.internal.app.DefaultPluginConfigurer;
import co.cask.cdap.internal.app.mapreduce.DefaultMapReduceConfigurer;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.flow.DefaultFlowConfigurer;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.app.services.DefaultServiceConfigurer;
import co.cask.cdap.internal.app.spark.DefaultSparkConfigurer;
import co.cask.cdap.internal.app.worker.DefaultWorkerConfigurer;
import co.cask.cdap.internal.app.workflow.DefaultWorkflowConfigurer;
import co.cask.cdap.internal.schedule.StreamSizeSchedule;
import co.cask.cdap.proto.Id;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * Default implementation of {@link ApplicationConfigurer}.
 */
public class DefaultAppConfigurer extends DefaultPluginConfigurer implements ApplicationConfigurer {
  private String name;
  private String description;
  private String configuration;
  private Id.Artifact artifactId;
  private ArtifactRepository artifactRepository;
  private PluginInstantiator pluginInstantiator;
  private final Map<String, FlowSpecification> flows = new HashMap<>();
  private final Map<String, MapReduceSpecification> mapReduces = new HashMap<>();
  private final Map<String, SparkSpecification> sparks = new HashMap<>();
  private final Map<String, WorkflowSpecification> workflows = new HashMap<>();
  private final Map<String, ServiceSpecification> services = new HashMap<>();
  private final Map<String, ScheduleSpecification> schedules = new HashMap<>();
  private final Map<String, WorkerSpecification> workers = new HashMap<>();

  // passed app to be used to resolve default name and description
  @VisibleForTesting
  public DefaultAppConfigurer(Id.Namespace namespace, Application app) {
    super(namespace, null, null, null);
    this.name = app.getClass().getSimpleName();
    this.description = "";
  }

  public DefaultAppConfigurer(Id.Namespace namespace, Id.Artifact artifactId, Application app, String configuration,
                              ArtifactRepository artifactRepository, PluginInstantiator pluginInstantiator) {
    super(namespace, artifactId, artifactRepository, pluginInstantiator);
    this.name = app.getClass().getSimpleName();
    this.description = "";
    this.configuration = configuration;
    this.artifactId = artifactId;
    this.artifactRepository = artifactRepository;
    this.pluginInstantiator = pluginInstantiator;
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

  @Override
  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public void addFlow(Flow flow) {
    Preconditions.checkArgument(flow != null, "Flow cannot be null.");
    DefaultFlowConfigurer configurer = new DefaultFlowConfigurer(flow);
    flow.configure(configurer);
    FlowSpecification spec = configurer.createSpecification();
    addStreams(configurer.getStreams());
    addDatasetModules(configurer.getDatasetModules());
    addDatasetSpecs(configurer.getDatasetSpecs());
    flows.put(spec.getName(), spec);
  }

  @Override
  public void addMapReduce(MapReduce mapReduce) {
    Preconditions.checkArgument(mapReduce != null, "MapReduce cannot be null.");
    DefaultMapReduceConfigurer configurer = new DefaultMapReduceConfigurer(mapReduce, deployNamespace, artifactId,
                                                                           artifactRepository,
                                                                           pluginInstantiator);
    mapReduce.configure(configurer);

    addStreams(configurer.getStreams());
    addDatasetModules(configurer.getDatasetModules());
    addDatasetSpecs(configurer.getDatasetSpecs());
    addPlugins(configurer.getPlugins());
    MapReduceSpecification spec = configurer.createSpecification();
    mapReduces.put(spec.getName(), spec);
  }

  @Override
  public void addSpark(Spark spark) {
    Preconditions.checkArgument(spark != null, "Spark cannot be null.");
    DefaultSparkConfigurer configurer = new DefaultSparkConfigurer(spark, deployNamespace, artifactId,
                                                                   artifactRepository,
                                                                   pluginInstantiator);
    spark.configure(configurer);

    addStreams(configurer.getStreams());
    addDatasetModules(configurer.getDatasetModules());
    addDatasetSpecs(configurer.getDatasetSpecs());
    addPlugins(configurer.getPlugins());
    SparkSpecification spec = configurer.createSpecification();
    sparks.put(spec.getName(), spec);
  }

  @Override
  public void addWorkflow(Workflow workflow) {
    Preconditions.checkArgument(workflow != null, "Workflow cannot be null.");
    DefaultWorkflowConfigurer configurer = new DefaultWorkflowConfigurer(workflow);
    workflow.configure(configurer);
    WorkflowSpecification spec = configurer.createSpecification();
    workflows.put(spec.getName(), spec);
  }

  public void addService(Service service) {
    Preconditions.checkArgument(service != null, "Service cannot be null.");
    DefaultServiceConfigurer configurer = new DefaultServiceConfigurer(service, deployNamespace, artifactId,
                                                                       artifactRepository, pluginInstantiator);
    service.configure(configurer);

    ServiceSpecification spec = configurer.createSpecification();
    addStreams(configurer.getStreams());
    addDatasetModules(configurer.getDatasetModules());
    addDatasetSpecs(configurer.getDatasetSpecs());
    addPlugins(configurer.getPlugins());
    services.put(spec.getName(), spec);
  }

  @Override
  public void addWorker(Worker worker) {
    Preconditions.checkArgument(worker != null, "Worker cannot be null.");
    DefaultWorkerConfigurer configurer = new DefaultWorkerConfigurer(worker, deployNamespace, artifactId,
                                                                     artifactRepository,
                                                                     pluginInstantiator);
    worker.configure(configurer);

    addStreams(configurer.getStreams());
    addDatasetModules(configurer.getDatasetModules());
    addDatasetSpecs(configurer.getDatasetSpecs());
    addPlugins(configurer.getPlugins());
    WorkerSpecification spec = configurer.createSpecification();
    workers.put(spec.getName(), spec);
  }

  @Override
  public void addSchedule(Schedule schedule, SchedulableProgramType programType, String programName,
                          Map<String, String> properties) {
    Preconditions.checkNotNull(schedule, "Schedule cannot be null.");
    Preconditions.checkNotNull(schedule.getName(), "Schedule name cannot be null.");
    Preconditions.checkArgument(!schedule.getName().isEmpty(), "Schedule name cannot be empty.");
    Preconditions.checkNotNull(programName, "Program name cannot be null.");
    Preconditions.checkArgument(!programName.isEmpty(), "Program name cannot be empty.");
    Preconditions.checkArgument(!schedules.containsKey(schedule.getName()), "Schedule with the name '" +
      schedule.getName()  + "' already exists.");
    if (schedule instanceof StreamSizeSchedule) {
      Preconditions.checkArgument(((StreamSizeSchedule) schedule).getDataTriggerMB() > 0,
                                  "Schedule data trigger must be greater than 0.");
    }

    ScheduleSpecification spec =
      new ScheduleSpecification(schedule, new ScheduleProgramInfo(programType, programName), properties);

    schedules.put(schedule.getName(), spec);
  }

  public ApplicationSpecification createSpecification() {
    // can be null only for apps before 3.2 that were not upgraded
    ArtifactId id = artifactId == null ? null :
      new ArtifactId(artifactId.getName(), artifactId.getVersion(),
                     artifactId.getNamespace().equals(Id.Namespace.SYSTEM) ? ArtifactScope.SYSTEM : ArtifactScope.USER);
    return new DefaultApplicationSpecification(name, description, configuration, id, getStreams(),
                                               getDatasetModules(), getDatasetSpecs(),
                                               flows, mapReduces, sparks, workflows, services,
                                               schedules, workers, getPlugins());
  }
}
