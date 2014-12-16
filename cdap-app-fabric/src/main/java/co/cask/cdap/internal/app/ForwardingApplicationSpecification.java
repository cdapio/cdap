/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.data.dataset.DatasetCreationSpec;

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
  public Map<String, FlowSpecification> getFlows() {
    return delegate.getFlows();
  }

  /*
  * @deprecated As of version 2.6.0,  replaced by {@link co.cask.cdap.api.service.Service}
   */
  @Deprecated
  @Override
  public Map<String, ProcedureSpecification> getProcedures() {
    return delegate.getProcedures();
  }

  @Override
  public Map<String, MapReduceSpecification> getMapReduce() {
    return delegate.getMapReduce();
  }

  @Override
  public Map<String, SparkSpecification> getSpark() {
    return delegate.getSpark();
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
