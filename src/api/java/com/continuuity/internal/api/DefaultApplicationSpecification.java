package com.continuuity.internal.api;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 *
 */
public final class DefaultApplicationSpecification implements ApplicationSpecification {

  private final String name;
  private final String description;
  private final Map<String, StreamSpecification> streams;
  private final Map<String, DataSetSpecification> datasets;
  private final Map<String, FlowSpecification> flows;
  private final Map<String, ProcedureSpecification> procedures;

  public DefaultApplicationSpecification(String name, String description,
                                         Map<String, StreamSpecification> streams,
                                         Map<String, DataSetSpecification> datasets,
                                         Map<String, FlowSpecification> flows,
                                         Map<String, ProcedureSpecification> procedures) {
    this.name = name;
    this.description = description;
    this.streams = ImmutableMap.copyOf(streams);
    this.datasets = ImmutableMap.copyOf(datasets);
    this.flows = ImmutableMap.copyOf(flows);
    this.procedures = ImmutableMap.copyOf(procedures);
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
  public Map<String, FlowSpecification> getFlows() {
    return flows;
  }

  @Override
  public Map<String, ProcedureSpecification> getProcedures() {
    return procedures;
  }
}
