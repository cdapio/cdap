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

package io.cdap.cdap.internal.app;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.mapreduce.MapReduceSpecification;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.service.ServiceSpecification;
import io.cdap.cdap.api.spark.SparkSpecification;
import io.cdap.cdap.api.worker.WorkerSpecification;
import io.cdap.cdap.api.workflow.WorkflowSpecification;
import io.cdap.cdap.internal.dataset.DatasetCreationSpec;
import io.cdap.cdap.internal.schedule.ScheduleCreationSpec;
import io.cdap.cdap.proto.id.ApplicationId;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 *
 */
public final class DefaultApplicationSpecification implements ApplicationSpecification {

  private final String name;
  private final String appVersion;
  private final String appCDAPVersion;
  private final String description;
  private final String configuration;
  private final ArtifactId artifactId;
  private final Map<String, String> datasetModules;
  private final Map<String, DatasetCreationSpec> datasetInstances;
  private final Map<String, MapReduceSpecification> mapReduces;
  private final Map<String, SparkSpecification> sparks;
  private final Map<String, WorkflowSpecification> workflows;
  private final Map<String, ServiceSpecification> services;
  private final Map<String, ScheduleCreationSpec> programSchedules;
  private final Map<String, WorkerSpecification> workers;
  private final Map<String, Plugin> plugins;

  public DefaultApplicationSpecification(String name, String description, String configuration,
                                         ArtifactId artifactId,
                                         Map<String, String> datasetModules,
                                         Map<String, DatasetCreationSpec> datasetInstances,
                                         Map<String, MapReduceSpecification> mapReduces,
                                         Map<String, SparkSpecification> sparks,
                                         Map<String, WorkflowSpecification> workflows,
                                         Map<String, ServiceSpecification> services,
                                         Map<String, ScheduleCreationSpec> programSchedules,
                                         Map<String, WorkerSpecification> workers,
                                         Map<String, Plugin> plugins, String appCDAPVersion) {
    this(name, ApplicationId.DEFAULT_VERSION, appCDAPVersion, description, configuration, artifactId, datasetModules,
         datasetInstances, mapReduces, sparks, workflows, services, programSchedules, workers,
         plugins);
  }

  public DefaultApplicationSpecification(String name, String appVersion, String appCDAPVersion, String description,
                                         String configuration,
                                         ArtifactId artifactId,
                                         Map<String, String> datasetModules,
                                         Map<String, DatasetCreationSpec> datasetInstances,
                                         Map<String, MapReduceSpecification> mapReduces,
                                         Map<String, SparkSpecification> sparks,
                                         Map<String, WorkflowSpecification> workflows,
                                         Map<String, ServiceSpecification> services,
                                         Map<String, ScheduleCreationSpec> programSchedules,
                                         Map<String, WorkerSpecification> workers,
                                         Map<String, Plugin> plugins) {
    this.name = name;
    this.appVersion = appVersion;
    this.appCDAPVersion = appCDAPVersion;
    this.description = description;
    this.configuration = configuration;
    this.artifactId = artifactId;
    this.datasetModules = ImmutableMap.copyOf(datasetModules);
    this.datasetInstances = ImmutableMap.copyOf(datasetInstances);
    this.mapReduces = ImmutableMap.copyOf(mapReduces);
    this.sparks = ImmutableMap.copyOf(sparks);
    this.workflows = ImmutableMap.copyOf(workflows);
    this.services = ImmutableMap.copyOf(services);
    this.programSchedules = ImmutableMap.copyOf(programSchedules);
    this.workers = ImmutableMap.copyOf(workers);
    this.plugins = ImmutableMap.copyOf(plugins);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getAppVersion() {
    return appVersion;
  }

  @Override
  public String getAppCDAPVersion() {
    return appCDAPVersion;
  }

  @Nullable
  @Override
  public String getConfiguration() {
    return configuration;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public ArtifactId getArtifactId() {
    return artifactId;
  }

  @Override
  public Map<String, String> getDatasetModules() {
    return datasetModules;
  }

  @Override
  public Map<String, DatasetCreationSpec> getDatasets() {
    return datasetInstances;
  }

  @Override
  public Map<String, MapReduceSpecification> getMapReduce() {
    return mapReduces;
  }

  @Override
  public Map<String, SparkSpecification> getSpark() {
    return sparks;
  }

  @Override
  public Map<String, WorkflowSpecification> getWorkflows() {
    return workflows;
  }

  @Override
  public Map<String, ServiceSpecification> getServices() {
    return services;
  }

  @Override
  public Map<String, WorkerSpecification> getWorkers() {
    return workers;
  }

  @Override
  public Map<String, ScheduleCreationSpec> getProgramSchedules() {
    return programSchedules;
  }

  @Override
  public Map<String, Plugin> getPlugins() {
    return plugins;
  }

  @Override
  public Set<String> getProgramsByType(ProgramType programType) {
    switch (programType) {
      case SPARK:
        return sparks.keySet();
      case MAPREDUCE:
        return mapReduces.keySet();
      case WORKER:
        return workers.keySet();
      case SERVICE:
        return services.keySet();
      case WORKFLOW:
        return workflows.keySet();
      default:
        return ImmutableSet.of();
    }
  }
}
