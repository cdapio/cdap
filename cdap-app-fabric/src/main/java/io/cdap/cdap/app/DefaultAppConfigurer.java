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

package io.cdap.cdap.app;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.api.app.ApplicationConfigurer;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.api.app.RuntimeConfigurer;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.mapreduce.MapReduce;
import io.cdap.cdap.api.mapreduce.MapReduceSpecification;
import io.cdap.cdap.api.metadata.Metadata;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.schedule.ScheduleBuilder;
import io.cdap.cdap.api.schedule.TriggerFactory;
import io.cdap.cdap.api.service.Service;
import io.cdap.cdap.api.service.ServiceSpecification;
import io.cdap.cdap.api.service.SystemServiceConfigurer;
import io.cdap.cdap.api.spark.Spark;
import io.cdap.cdap.api.spark.SparkSpecification;
import io.cdap.cdap.api.worker.Worker;
import io.cdap.cdap.api.worker.WorkerSpecification;
import io.cdap.cdap.api.workflow.Workflow;
import io.cdap.cdap.api.workflow.WorkflowSpecification;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.common.utils.ProjectInfo;
import io.cdap.cdap.internal.api.DefaultDatasetConfigurer;
import io.cdap.cdap.internal.app.AbstractConfigurer;
import io.cdap.cdap.internal.app.DefaultApplicationSpecification;
import io.cdap.cdap.internal.app.mapreduce.DefaultMapReduceConfigurer;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.internal.app.runtime.schedule.DefaultScheduleBuilder;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.DefaultTriggerFactory;
import io.cdap.cdap.internal.app.services.DefaultServiceConfigurer;
import io.cdap.cdap.internal.app.services.DefaultSystemTableConfigurer;
import io.cdap.cdap.internal.app.spark.DefaultSparkConfigurer;
import io.cdap.cdap.internal.app.worker.DefaultWorkerConfigurer;
import io.cdap.cdap.internal.app.workflow.DefaultWorkflowConfigurer;
import io.cdap.cdap.internal.schedule.ScheduleCreationSpec;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link ApplicationConfigurer}.
 */
public class DefaultAppConfigurer extends AbstractConfigurer implements ApplicationConfigurer {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultAppConfigurer.class);

  private final PluginFinder pluginFinder;
  private final PluginInstantiator pluginInstantiator;
  private final Id.Artifact artifactId;
  private final String configuration;
  private final Map<String, MapReduceSpecification> mapReduces = new HashMap<>();
  private final Map<String, SparkSpecification> sparks = new HashMap<>();
  private final Map<String, WorkflowSpecification> workflows = new HashMap<>();
  private final Map<String, ServiceSpecification> services = new HashMap<>();
  private final Map<String, ScheduleCreationSpec> scheduleSpecs = new HashMap<>();
  private final Map<String, WorkerSpecification> workers = new HashMap<>();
  private final Map<StructuredTableId, StructuredTableSpecification> systemTables = new HashMap<>();
  private final TriggerFactory triggerFactory;
  private final RuntimeConfigurer runtimeConfigurer;
  private String name;
  private Map<MetadataScope, Metadata> appMetadata;
  private String description;

  // passed app to be used to resolve default name and description
  @VisibleForTesting
  public DefaultAppConfigurer(Id.Namespace namespace, Id.Artifact artifactId, Application app) {
    this(namespace, artifactId, app, "", null, null, null);
  }

  public DefaultAppConfigurer(Id.Namespace namespace, Id.Artifact artifactId, Application app, String configuration,
                              @Nullable PluginFinder pluginFinder,
                              @Nullable PluginInstantiator pluginInstantiator,
                              @Nullable RuntimeConfigurer runtimeConfigurer) {
    super(namespace, artifactId, pluginFinder, pluginInstantiator);
    this.name = app.getClass().getSimpleName();
    this.description = "";
    this.configuration = configuration;
    this.artifactId = artifactId;
    this.pluginFinder = pluginFinder;
    this.pluginInstantiator = pluginInstantiator;
    this.appMetadata = new HashMap<>();
    this.triggerFactory = new DefaultTriggerFactory(namespace.toEntityId());
    this.runtimeConfigurer = runtimeConfigurer;
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
  public void addMapReduce(MapReduce mapReduce) {
    Preconditions.checkArgument(mapReduce != null, "MapReduce cannot be null.");
    DefaultMapReduceConfigurer configurer = new DefaultMapReduceConfigurer(mapReduce, deployNamespace, artifactId,
                                                                           pluginFinder,
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
      spark.getClass().getClassLoader(), "io.cdap.cdap.app.runtime.spark.classloader.SparkRunnerClassLoader");

    if (sparkRunnerClassLoader != null) {
      try {
        configurer = (DefaultSparkConfigurer) sparkRunnerClassLoader
          .loadClass("io.cdap.cdap.app.deploy.spark.DefaultExtendedSparkConfigurer")
          .getConstructor(Spark.class, Id.Namespace.class, Id.Artifact.class,
                          PluginFinder.class, PluginInstantiator.class)
          .newInstance(spark, deployNamespace, artifactId, pluginFinder, pluginInstantiator);

      } catch (Exception e) {
        // Ignore it and the configurer will be defaulted to DefaultSparkConfigurer
        LOG.trace("No DefaultExtendedSparkConfigurer found. Fallback to DefaultSparkConfigurer.", e);
      }
    }

    if (configurer == null) {
      configurer = new DefaultSparkConfigurer(spark, deployNamespace, artifactId,
                                              pluginFinder, pluginInstantiator);
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
                                                                         pluginFinder, pluginInstantiator);
    workflow.configure(configurer);
    WorkflowSpecification spec = configurer.createSpecification();
    addDatasetsAndPlugins(configurer);
    workflows.put(spec.getName(), spec);
  }

  @Override
  public void addService(Service service) {
    Preconditions.checkArgument(service != null, "Service cannot be null.");
    // check that a system service is only used in system namespace
    if (!deployNamespace.equals(Id.Namespace.fromEntityId(NamespaceId.SYSTEM))) {
      TypeToken<?> type = TypeToken.of(service.getClass()).resolveType(Service.class.getTypeParameters()[0]);
      if (SystemServiceConfigurer.class.isAssignableFrom(type.getRawType())) {
        throw new IllegalArgumentException(String.format(
          "Invalid service '%s'. Services can only use a SystemServiceConfigurer if the application is "
            + "deployed in the system namespace.", service.getClass().getSimpleName()));
      }
    }

    DefaultSystemTableConfigurer systemTableConfigurer = new DefaultSystemTableConfigurer();
    DefaultServiceConfigurer configurer = new DefaultServiceConfigurer(service, deployNamespace, artifactId,
                                                                       pluginFinder, pluginInstantiator,
                                                                       systemTableConfigurer);
    service.configure(configurer);

    ServiceSpecification spec = configurer.createSpecification();
    addDatasetsAndPlugins(configurer);
    addSystemTableSpecs(systemTableConfigurer.getTableSpecs());
    services.put(spec.getName(), spec);
  }

  @Override
  public void addWorker(Worker worker) {
    Preconditions.checkArgument(worker != null, "Worker cannot be null.");
    DefaultWorkerConfigurer configurer = new DefaultWorkerConfigurer(worker, deployNamespace, artifactId,
                                                                     pluginFinder,
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
  public void emitMetadata(Metadata metadata, MetadataScope scope) {
    Metadata scopeMetadata = appMetadata.computeIfAbsent(scope, s -> new Metadata(new HashMap<>(), new HashSet<>()));
    Map<String, String> properties = new HashMap<>(scopeMetadata.getProperties());
    properties.putAll(metadata.getProperties());
    Set<String> tags = new HashSet<>(scopeMetadata.getTags());
    tags.addAll(metadata.getTags());
    appMetadata.put(scope, new Metadata(properties, tags));
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

  @Override
  public RuntimeConfigurer getRuntimeConfigurer() {
    return runtimeConfigurer;
  }

  @Override
  public String getDeployedNamespace() {
    return deployNamespace.getId();
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

    return new DefaultApplicationSpecification(appName, appVersion, ProjectInfo.getVersion().toString(), description,
                                               configuration, artifactId,
                                               getDatasetModules(), getDatasetSpecs(),
                                               mapReduces, sparks, workflows, services,
                                               builtScheduleSpecs, workers, getPlugins());
  }

  public Collection<StructuredTableSpecification> getSystemTables() {
    return systemTables.values();
  }

  public Map<MetadataScope, Metadata> getMetadata() {
    return appMetadata;
  }

  /**
   * Adds the dataset and plugins at the application level so that they can be used by any program in the application.
   *
   * @param configurer the program's configurer
   */
  private void addDatasetsAndPlugins(AbstractConfigurer configurer) {
    addDatasets(configurer);
    addPlugins(configurer.getPlugins());
  }

  private void addDatasets(DefaultDatasetConfigurer configurer) {
    addDatasetModules(configurer.getDatasetModules());
    addDatasetSpecs(configurer.getDatasetSpecs());
  }

  private void addSystemTableSpecs(Collection<StructuredTableSpecification> specs) {
    for (StructuredTableSpecification spec : specs) {
      StructuredTableSpecification existing = systemTables.get(spec.getTableId());
      if (existing != null && !existing.equals(spec)) {
        throw new IllegalArgumentException(String.format(
          "System table '%s' was created more than once with different specifications.", spec.getTableId().getName()));
      }
      systemTables.put(spec.getTableId(), spec);
    }
  }
}
