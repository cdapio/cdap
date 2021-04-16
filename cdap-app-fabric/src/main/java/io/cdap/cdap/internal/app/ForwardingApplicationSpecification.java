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

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

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
  public String getAppVersion() {
    return delegate.getAppVersion();
  }

  @Override
  public String getAppCDAPVersion() {
    return delegate.getAppCDAPVersion();
  }

  @Nullable
  @Override
  public String getConfiguration() {
    return delegate.getConfiguration();
  }

  @Override
  public String getDescription() {
    return delegate.getDescription();
  }

  @Override
  public ArtifactId getArtifactId() {
    return delegate.getArtifactId();
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

  @Override
  public Map<String, WorkerSpecification> getWorkers() {
    return delegate.getWorkers();
  }

  @Override
  public Map<String, ScheduleCreationSpec> getProgramSchedules() {
    return delegate.getProgramSchedules();
  }

  @Override
  public Map<String, Plugin> getPlugins() {
    return delegate.getPlugins();
  }

  @Override
  public Set<String> getProgramsByType(ProgramType programType) {
    return delegate.getProgramsByType(programType);
  }
}
