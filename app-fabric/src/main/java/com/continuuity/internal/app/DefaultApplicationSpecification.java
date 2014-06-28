package com.continuuity.internal.app;

import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.api.service.ServiceSpecification;
import com.continuuity.api.workflow.WorkflowSpecification;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.data.dataset.DatasetCreationSpec;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 *
 */
public final class DefaultApplicationSpecification implements ApplicationSpecification {

  private final String name;
  private final String description;
  private final Map<String, StreamSpecification> streams;
  private final Map<String, DataSetSpecification> datasets;
  private final Map<String, String> datasetModules;
  private final Map<String, DatasetCreationSpec> datasetInstances;
  private final Map<String, FlowSpecification> flows;
  private final Map<String, ProcedureSpecification> procedures;
  private final Map<String, MapReduceSpecification> mapReduces;
  private final Map<String, WorkflowSpecification> workflows;
  private final Map<String, ServiceSpecification> services;


  public DefaultApplicationSpecification(String name, String description,
                                         Map<String, StreamSpecification> streams,
                                         Map<String, DataSetSpecification> datasets,
                                         Map<String, FlowSpecification> flows,
                                         Map<String, ProcedureSpecification> procedures,
                                         Map<String, MapReduceSpecification> mapReduces,
                                         Map<String, WorkflowSpecification> workflows) {
    this(name, description, streams, datasets,
         Maps.<String, String>newHashMap(),
         Maps.<String, DatasetCreationSpec>newHashMap(),
         flows, procedures, mapReduces, workflows, Maps.<String, ServiceSpecification>newHashMap());

  }

  public DefaultApplicationSpecification(String name, String description,
                                         Map<String, StreamSpecification> streams,
                                         Map<String, DataSetSpecification> datasets,
                                         Map<String, String> datasetModules,
                                         Map<String, DatasetCreationSpec> datasetInstances,
                                         Map<String, FlowSpecification> flows,
                                         Map<String, ProcedureSpecification> procedures,
                                         Map<String, MapReduceSpecification> mapReduces,
                                         Map<String, WorkflowSpecification> workflows,
                                         Map<String, ServiceSpecification> services) {
    this.name = name;
    this.description = description;
    this.streams = ImmutableMap.copyOf(streams);
    this.datasets = ImmutableMap.copyOf(datasets);
    this.datasetModules = ImmutableMap.copyOf(datasetModules);
    this.datasetInstances = ImmutableMap.copyOf(datasetInstances);
    this.flows = ImmutableMap.copyOf(flows);
    this.procedures = ImmutableMap.copyOf(procedures);
    this.mapReduces = ImmutableMap.copyOf(mapReduces);
    this.workflows = ImmutableMap.copyOf(workflows);
    this.services = ImmutableMap.copyOf(services);
  }

  public static DefaultApplicationSpecification from(com.continuuity.api.ApplicationSpecification spec) {
    return new DefaultApplicationSpecification(spec.getName(), spec.getDescription(),
                                               spec.getStreams(), spec.getDataSets(),
                                               spec.getFlows(), spec.getProcedures(),
                                               spec.getMapReduce(), spec.getWorkflows());
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
  public Map<String, DataSetSpecification> getDataSets() {
    return datasets;
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

  @Override
  public Map<String, ProcedureSpecification> getProcedures() {
    return procedures;
  }

  @Override
  public Map<String, MapReduceSpecification> getMapReduce() {
    return mapReduces;
  }

  @Override
  public Map<String, WorkflowSpecification> getWorkflows() {
    return workflows;
  }

  public Map<String, ServiceSpecification> getServices() {
    return services;
  }
}
