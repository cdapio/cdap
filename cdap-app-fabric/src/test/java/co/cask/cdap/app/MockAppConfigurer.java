/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.api.app.ProgramType;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.mapreduce.MapReduce;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.plugin.PluginSelector;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.ScheduleBuilder;
import co.cask.cdap.api.schedule.TriggerFactory;
import co.cask.cdap.api.service.Service;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.worker.Worker;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.internal.app.runtime.schedule.DefaultScheduleBuilder;
import co.cask.cdap.internal.app.runtime.schedule.trigger.DefaultTriggerFactory;
import co.cask.cdap.internal.schedule.ScheduleCreationSpec;
import co.cask.cdap.proto.id.NamespaceId;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Used in unit-test and integration-test simply to get the name of the Application.
 */
public final class MockAppConfigurer implements ApplicationConfigurer {

  private static final String ERROR_MSG = "Applications that use plugins cannot be deployed/created using " +
    "deployApplication(Id.Namespace namespace, Class<? extends Application> applicationClz) method." +
    "Instead use addAppArtifact, addPluginArtifact and " +
    "deployApplication(Id.Artifact artifactId, AppRequest appRequest) method.";

  private String name;

  public MockAppConfigurer(Application app) {
    this.name = app.getClass().getSimpleName();
  }

  public String getName() {
    return name;
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

  @Override
  public void setDescription(String description) {

  }

  @Override
  public void addFlow(Flow flow) {

  }

  @Override
  public void addMapReduce(MapReduce mapReduce) {

  }

  @Override
  public void addSpark(Spark spark) {

  }

  @Override
  public void addWorkflow(Workflow workflow) {

  }

  @Override
  public void addService(Service service) {

  }

  @Override
  public void addWorker(Worker worker) {

  }

  @Override
  public void addSchedule(Schedule schedule, SchedulableProgramType programType, String programName,
                          Map<String, String> properties) {

  }

  @Override
  public void schedule(ScheduleCreationSpec programSchedule) {

  }

  @Override
  public TriggerFactory getTriggerFactory() {
    // the result of this won't actually be used, but the returned object will have its methods called
    return new DefaultTriggerFactory(NamespaceId.DEFAULT);
  }

  @Override
  public ScheduleBuilder buildSchedule(String scheduleName, ProgramType programType,
                                       String workflowName) {
    // the result of this won't actually be used, but the returned object will have its methods called
    return new DefaultScheduleBuilder(scheduleName, workflowName, getTriggerFactory());
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties) {
    throw new UnsupportedOperationException(ERROR_MSG);
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties,
                         PluginSelector selector) {
    throw new UnsupportedOperationException(ERROR_MSG);
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId,
                                     PluginProperties properties) {
    throw new UnsupportedOperationException(ERROR_MSG);
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId, PluginProperties properties,
                                     PluginSelector selector) {
    throw new UnsupportedOperationException(ERROR_MSG);
  }

  @Override
  public void addStream(Stream stream) {

  }

  @Override
  public void addStream(String streamName) {

  }

  @Override
  public void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass) {

  }

  @Override
  public void addDatasetType(Class<? extends Dataset> datasetClass) {

  }

  @Override
  public void createDataset(String datasetName, String typeName, DatasetProperties properties) {

  }

  @Override
  public void createDataset(String datasetName, String typeName) {

  }

  @Override
  public void createDataset(String datasetName, Class<? extends Dataset> datasetClass, DatasetProperties props) {

  }

  @Override
  public void createDataset(String datasetName, Class<? extends Dataset> datasetClass) {

  }
}
