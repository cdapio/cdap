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

package co.cask.cdap.internal.app;

import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.procedure.ProcedureSpecification;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.api.worker.WorkerSpecification;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.data.dataset.DatasetCreationSpec;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 *
 */
public final class DefaultApplicationSpecification implements ApplicationSpecification {

  private final String name;
  private final String description;
  private final Map<String, StreamSpecification> streams;
  private final Map<String, String> datasetModules;
  private final Map<String, DatasetCreationSpec> datasetInstances;
  private final Map<String, FlowSpecification> flows;
  private final Map<String, ProcedureSpecification> procedures;
  private final Map<String, MapReduceSpecification> mapReduces;
  private final Map<String, SparkSpecification> sparks;
  private final Map<String, WorkflowSpecification> workflows;
  private final Map<String, ServiceSpecification> services;
  private final Map<String, ScheduleSpecification> schedules;
  private final Map<String, WorkerSpecification> workers;

  public DefaultApplicationSpecification(String name, String description,
                                         Map<String, StreamSpecification> streams,
                                         Map<String, String> datasetModules,
                                         Map<String, DatasetCreationSpec> datasetInstances,
                                         Map<String, FlowSpecification> flows,
                                         Map<String, ProcedureSpecification> procedures,
                                         Map<String, MapReduceSpecification> mapReduces,
                                         Map<String, SparkSpecification> sparks,
                                         Map<String, WorkflowSpecification> workflows,
                                         Map<String, ServiceSpecification> services,
                                         Map<String, ScheduleSpecification> schedules,
                                         Map<String, WorkerSpecification> workers) {
    this.name = name;
    this.description = description;
    this.streams = ImmutableMap.copyOf(streams);
    this.datasetModules = ImmutableMap.copyOf(datasetModules);
    this.datasetInstances = ImmutableMap.copyOf(datasetInstances);
    this.flows = ImmutableMap.copyOf(flows);
    this.procedures = ImmutableMap.copyOf(procedures);
    this.mapReduces = ImmutableMap.copyOf(mapReduces);
    this.sparks = ImmutableMap.copyOf(sparks);
    this.workflows = ImmutableMap.copyOf(workflows);
    this.services = ImmutableMap.copyOf(services);
    this.schedules = ImmutableMap.copyOf(schedules);
    this.workers = ImmutableMap.copyOf(workers);
  }

  public static DefaultApplicationSpecification from(ApplicationSpecification spec) {
    return new DefaultApplicationSpecification(spec.getName(), spec.getDescription(),
                                               spec.getStreams(),
                                               spec.getDatasetModules(), spec.getDatasets(),
                                               spec.getFlows(), spec.getProcedures(),
                                               spec.getMapReduce(), spec.getSpark(), spec.getWorkflows(),
                                               spec.getServices(), spec.getSchedules(), spec.getWorkers());
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public Map<String, StreamSpecification> getStreams() {
    return streams;
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
  public Map<String, FlowSpecification> getFlows() {
    return flows;
  }

  /*
  * @deprecated As of version 2.6.0, replaced by {@link co.cask.cdap.api.service.Service}
  */
  @Deprecated
  @Override
  public Map<String, ProcedureSpecification> getProcedures() {
    return procedures;
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

  public Map<String, ServiceSpecification> getServices() {
    return services;
  }

  @Override
  public Map<String, WorkerSpecification> getWorkers() {
    return workers;
  }

  @Override
  public Map<String, ScheduleSpecification> getSchedules() {
    return schedules;
  }
}
