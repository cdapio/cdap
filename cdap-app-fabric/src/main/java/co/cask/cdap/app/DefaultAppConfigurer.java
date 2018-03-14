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

package co.cask.cdap.app;

import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.app.ApplicationConfigurer;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.app.ProgramType;
import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.mapreduce.MapReduce;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.schedule.ScheduleBuilder;
import co.cask.cdap.api.schedule.TriggerFactory;
import co.cask.cdap.api.service.Service;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.api.worker.Worker;
import co.cask.cdap.api.worker.WorkerSpecification;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.common.id.Id;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.internal.api.DefaultDatasetConfigurer;
import co.cask.cdap.internal.app.AbstractConfigurer;
import co.cask.cdap.internal.app.DefaultApplicationSpecification;
import co.cask.cdap.internal.app.mapreduce.DefaultMapReduceConfigurer;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.flow.DefaultFlowConfigurer;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.schedule.DefaultScheduleBuilder;
import co.cask.cdap.internal.app.runtime.schedule.trigger.DefaultTriggerFactory;
import co.cask.cdap.internal.app.services.DefaultServiceConfigurer;
import co.cask.cdap.internal.app.spark.DefaultSparkConfigurer;
import co.cask.cdap.internal.app.worker.DefaultWorkerConfigurer;
import co.cask.cdap.internal.app.workflow.DefaultWorkflowConfigurer;
import co.cask.cdap.internal.schedule.ScheduleCreationSpec;
import co.cask.cdap.proto.id.ApplicationId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link ApplicationConfigurer}.
 */
public class DefaultAppConfigurer extends AbstractConfigurer implements ApplicationConfigurer {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultAppConfigurer.class);

  private final ArtifactRepository artifactRepository;
  private final PluginInstantiator pluginInstantiator;
  private final Id.Artifact artifactId;
  private final String configuration;
  private final Map<String, FlowSpecification> flows = new HashMap<>();
  private final Map<String, MapReduceSpecification> mapReduces = new HashMap<>();
  private final Map<String, SparkSpecification> sparks = new HashMap<>();
  private final Map<String, WorkflowSpecification> workflows = new HashMap<>();
  private final Map<String, ServiceSpecification> services = new HashMap<>();
  private final Map<String, ScheduleCreationSpec> scheduleSpecs = new HashMap<>();
  private final Map<String, WorkerSpecification> workers = new HashMap<>();
  private final TriggerFactory triggerFactory;
  private String name;
  private String description;

  // passed app to be used to resolve default name and description
  @VisibleForTesting
  public DefaultAppConfigurer(Id.Namespace namespace, Id.Artifact artifactId, Application app) {
    this(namespace, artifactId, app, "", null, null);
  }

  public DefaultAppConfigurer(Id.Namespace namespace, Id.Artifact artifactId, Application app, String configuration,
                              @Nullable ArtifactRepository artifactRepository,
                              @Nullable PluginInstantiator pluginInstantiator) {
    super(namespace, artifactId, artifactRepository, pluginInstantiator);
    this.name = app.getClass().getSimpleName();
    this.description = "";
    this.configuration = configuration;
    this.artifactId = artifactId;
    this.artifactRepository = artifactRepository;
    this.pluginInstantiator = pluginInstantiator;
    this.triggerFactory = new DefaultTriggerFactory(namespace.toEntityId());
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
    addDatasets(configurer);
    flows.put(spec.getName(), spec);
  }

  @Override
  public void addMapReduce(MapReduce mapReduce) {
    Preconditions.checkArgument(mapReduce != null, "MapReduce cannot be null.");
    DefaultMapReduceConfigurer configurer = new DefaultMapReduceConfigurer(mapReduce, deployNamespace, artifactId,
                                                                           artifactRepository,
                                                                           pluginInstantiator);
    mapReduce.configure(configurer);
    addDatasetsAndPlugins(configurer);
    MapReduceSpecification spec = configurer.createSpecification();
    mapReduces.put(spec.getName(), spec);
  }

  @Override
  public void addSpark(Spark spark) {
    Preconditions.checkArgument(spark != null, "Spark cannot be null.");
    DefaultSparkConfigurer configurer = null;

    // It is a bit hacky here to look for the DefaultExtendedSparkConfigurer implementation through the
    // SparkRunnerClassloader directly (CDAP-11797)
    ClassLoader sparkRunnerClassLoader = ClassLoaders.findByName(
      spark.getClass().getClassLoader(), "co.cask.cdap.app.runtime.spark.classloader.SparkRunnerClassLoader");

    if (sparkRunnerClassLoader != null) {
      try {
        configurer = (DefaultSparkConfigurer) sparkRunnerClassLoader
          .loadClass("co.cask.cdap.app.deploy.spark.DefaultExtendedSparkConfigurer")
          .getConstructor(Spark.class, Id.Namespace.class, Id.Artifact.class,
                          ArtifactRepository.class, PluginInstantiator.class)
          .newInstance(spark, deployNamespace, artifactId, artifactRepository, pluginInstantiator);

      } catch (Exception e) {
        // Ignore it and the configurer will be defaulted to DefaultSparkConfigurer
        LOG.trace("No DefaultExtendedSparkConfigurer found. Fallback to DefaultSparkConfigurer.", e);
      }
    }

    if (configurer == null) {
      configurer = new DefaultSparkConfigurer(spark, deployNamespace, artifactId,
                                              artifactRepository, pluginInstantiator);
    }

    spark.configure(configurer);
    addDatasetsAndPlugins(configurer);
    SparkSpecification spec = configurer.createSpecification();
    sparks.put(spec.getName(), spec);
  }

  @Override
  public void addWorkflow(Workflow workflow) {
    Preconditions.checkArgument(workflow != null, "Workflow cannot be null.");
    DefaultWorkflowConfigurer configurer = new DefaultWorkflowConfigurer(workflow, this,
                                                                         deployNamespace, artifactId,
                                                                         artifactRepository, pluginInstantiator);
    workflow.configure(configurer);
    WorkflowSpecification spec = configurer.createSpecification();
    addDatasetsAndPlugins(configurer);
    workflows.put(spec.getName(), spec);
  }

  @Override
  public void addService(Service service) {
    Preconditions.checkArgument(service != null, "Service cannot be null.");
    DefaultServiceConfigurer configurer = new DefaultServiceConfigurer(service, deployNamespace, artifactId,
                                                                       artifactRepository, pluginInstantiator);
    service.configure(configurer);

    ServiceSpecification spec = configurer.createSpecification();
    addDatasetsAndPlugins(configurer);
    services.put(spec.getName(), spec);
  }

  @Override
  public void addWorker(Worker worker) {
    Preconditions.checkArgument(worker != null, "Worker cannot be null.");
    DefaultWorkerConfigurer configurer = new DefaultWorkerConfigurer(worker, deployNamespace, artifactId,
                                                                     artifactRepository,
                                                                     pluginInstantiator);
    worker.configure(configurer);
    addDatasetsAndPlugins(configurer);
    WorkerSpecification spec = configurer.createSpecification();
    workers.put(spec.getName(), spec);
  }

  private void doAddSchedule(ScheduleCreationSpec scheduleSpec) {
    // Schedules are unique by name so two different schedules with the same name cannot be scheduled
    String scheduleName = scheduleSpec.getName();
    Preconditions.checkArgument(null == scheduleSpecs.put(scheduleName, scheduleSpec),
                                "Duplicate schedule name for schedule: '%s'", scheduleName);
  }

  @Override
  public void schedule(ScheduleCreationSpec scheduleCreationSpec) {
    doAddSchedule(scheduleCreationSpec);
  }

  @Override
  public TriggerFactory getTriggerFactory() {
    return triggerFactory;
  }

  @Override
  public ScheduleBuilder buildSchedule(String scheduleName, ProgramType schedulableProgramType,
                                       String programName) {
    if (ProgramType.WORKFLOW != schedulableProgramType) {
      throw new IllegalArgumentException(String.format(
        "Cannot schedule program %s of type %s: Only workflows can be scheduled",
        programName, schedulableProgramType));
    }
    return new DefaultScheduleBuilder(scheduleName, programName, triggerFactory);
  }

  /**
   * Creates a {@link ApplicationSpecification} based on values in this configurer.
   *
   * @param applicationName if not null, the application name to use instead of using the one from this configurer
   * @return a new {@link ApplicationSpecification}.
   */
  public ApplicationSpecification createSpecification(@Nullable String applicationName) {
    // can be null only for apps before 3.2 that were not upgraded
    return createSpecification(applicationName, null);
  }

  public ApplicationSpecification createSpecification(@Nullable String applicationName,
                                                      @Nullable String applicationVersion) {
    // applicationName can be null only for apps before 3.2 that were not upgraded
    ArtifactScope scope = artifactId.getNamespace().equals(Id.Namespace.SYSTEM)
      ? ArtifactScope.SYSTEM : ArtifactScope.USER;
    ArtifactId artifactId = new ArtifactId(this.artifactId.getName(), this.artifactId.getVersion(), scope);

    String namespace = deployNamespace.toEntityId().getNamespace();
    String appName = applicationName == null ? name : applicationName;
    String appVersion = applicationVersion == null ? ApplicationId.DEFAULT_VERSION : applicationVersion;

    Map<String, ScheduleCreationSpec> builtScheduleSpecs = new HashMap<>();
    for (Map.Entry<String, ScheduleCreationSpec> entry : scheduleSpecs.entrySet()) {
      // If the ScheduleCreationSpec is really a builder, then build the ScheduleCreationSpec
      if (entry.getValue() instanceof DefaultScheduleBuilder.ScheduleCreationBuilder) {
        DefaultScheduleBuilder.ScheduleCreationBuilder builder =
          (DefaultScheduleBuilder.ScheduleCreationBuilder) entry.getValue();
        builtScheduleSpecs.put(entry.getKey(), builder.build(namespace, appName, appVersion));
      } else {
        builtScheduleSpecs.put(entry.getKey(), entry.getValue());
      }
    }

    return new DefaultApplicationSpecification(appName, appVersion, description,
                                               configuration, artifactId, getStreams(),
                                               getDatasetModules(), getDatasetSpecs(),
                                               flows, mapReduces, sparks, workflows, services,
                                               builtScheduleSpecs, workers, getPlugins());
  }

  private void addDatasetsAndPlugins(AbstractConfigurer configurer) {
    addDatasets(configurer);
    addPlugins(configurer.getPlugins());
  }

  private void addDatasets(DefaultDatasetConfigurer configurer) {
    addStreams(configurer.getStreams());
    addDatasetModules(configurer.getDatasetModules());
    addDatasetSpecs(configurer.getDatasetSpecs());
  }
}
