/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.api.app.ApplicationConfigurer;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.mapreduce.MapReduce;
import io.cdap.cdap.api.metadata.Metadata;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.api.schedule.ScheduleBuilder;
import io.cdap.cdap.api.schedule.TriggerFactory;
import io.cdap.cdap.api.service.Service;
import io.cdap.cdap.api.spark.Spark;
import io.cdap.cdap.api.worker.Worker;
import io.cdap.cdap.api.workflow.Workflow;
import io.cdap.cdap.internal.app.runtime.schedule.DefaultScheduleBuilder;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.DefaultTriggerFactory;
import io.cdap.cdap.internal.schedule.ScheduleCreationSpec;
import io.cdap.cdap.proto.id.NamespaceId;

import java.util.Collections;
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
  public void schedule(ScheduleCreationSpec programSchedule) {

  }

  @Override
  public void emitMetadata(Metadata metadata, MetadataScope scope) {

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
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties,
                         PluginSelector selector) {
    throw new UnsupportedOperationException(ERROR_MSG);
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId, PluginProperties properties,
                                     PluginSelector selector) {
    throw new UnsupportedOperationException(ERROR_MSG);
  }

  @Override
  public Map<String, String> evaluateMacros(Map<String, String> properties, MacroEvaluator evaluator,
                                            MacroParserOptions options) throws InvalidMacroException {
    return properties;
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

  @Override
  public Map<String, String> getFeatureFlags() {
    return Collections.emptyMap();
  }
}
