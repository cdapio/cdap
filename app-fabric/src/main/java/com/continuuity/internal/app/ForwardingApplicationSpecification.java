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

import java.util.Map;

/**
 *
 */
public abstract class ForwardingApplicationSpecification implements ApplicationSpecification {

  private final ApplicationSpecification delegate;

  protected ForwardingApplicationSpecification(ApplicationSpecification delegate) {
    this.delegate = delegate;
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public String getDescription() {
    return delegate.getDescription();
  }

  @Override
  public Map<String, StreamSpecification> getStreams() {
    return delegate.getStreams();
  }

  @Override
  public Map<String, DataSetSpecification> getDataSets() {
    return delegate.getDataSets();
  }

  @Override
  public Map<String, FlowSpecification> getFlows() {
    return delegate.getFlows();
  }

  @Override
  public Map<String, ProcedureSpecification> getProcedures() {
    return delegate.getProcedures();
  }

  @Override
  public Map<String, MapReduceSpecification> getMapReduce() {
    return delegate.getMapReduce();
  }

  @Override
  public Map<String, WorkflowSpecification> getWorkflows() {
    return delegate.getWorkflows();
  }

  @Override
  public Map<String, String> getDatasetModules() {
    return delegate.getDatasetModules();
  }

  @Override
  public Map<String, DatasetCreationSpec> getDatasets() {
    return delegate.getDatasets();
  }

  @Override
  public Map<String, ServiceSpecification> getServices() {
    return delegate.getServices();
  }
}
