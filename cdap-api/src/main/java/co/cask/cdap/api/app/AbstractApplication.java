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

package co.cask.cdap.api.app;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.mapreduce.MapReduce;
import co.cask.cdap.api.procedure.Procedure;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.service.BasicService;
import co.cask.cdap.api.service.Service;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.workflow.Workflow;

import java.util.HashMap;
import java.util.Map;

/**
 * A support class for {@link Application Applications} which reduces repetition and results in
 * a more readable configuration.
 *
 * <p>
 * Implement the {@link #configure()} method to define your application.
 * </p>
 * 
 * @see co.cask.cdap.api.app
 */
public abstract class AbstractApplication implements Application {
  private ApplicationContext context;
  private ApplicationConfigurer configurer;

  /**
   * Override this method to declare and configure the application.
   */
  public abstract void configure();

  @Override
  public final void configure(ApplicationConfigurer configurer, ApplicationContext context) {
    this.context = context;
    this.configurer = configurer;

    configure();
  }

  /**
   * @return The {@link ApplicationConfigurer} used to configure the {@link Application}
   */
  protected ApplicationConfigurer getConfigurer() {
    return configurer;
  }

  /**
   * @return The {@link ApplicationContext} of the {@link Application}
   */
  protected final ApplicationContext getContext() {
    return context;
  }

  /**
   * @see ApplicationConfigurer#setName(String)
   */
  protected void setName(String name) {
    configurer.setName(name);
  }

  /**
   * @see ApplicationConfigurer#setDescription(String)
   */
  protected void setDescription(String description) {
    configurer.setDescription(description);
  }

  /**
   * @see ApplicationConfigurer#addStream(Stream)
   */
  protected void addStream(Stream stream) {
    configurer.addStream(stream);
  }

  /**
   * @see ApplicationConfigurer#addDatasetModule(String, Class)
   */
  @Beta
  protected void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass) {
    configurer.addDatasetModule(moduleName, moduleClass);
  }

  /**
   * @see ApplicationConfigurer#addDatasetType(Class)
   */
  @Beta
  protected void addDatasetType(Class<? extends Dataset> datasetClass) {
    configurer.addDatasetType(datasetClass);
  }

  /**
   * Calls {@link ApplicationConfigurer#createDataset(String, String, DatasetProperties)}, passing empty properties.
   *
   * @see ApplicationConfigurer#createDataset(String, String, DatasetProperties)
   */
  @Beta
  protected void createDataset(String datasetName, String typeName) {
    configurer.createDataset(datasetName, typeName, DatasetProperties.EMPTY);
  }

  /**
   * Calls {@link ApplicationConfigurer#createDataset(String, String, DatasetProperties)}, passing the type name and
   * properties.
   *
   * @see ApplicationConfigurer#createDataset(String, String, co.cask.cdap.api.dataset.DatasetProperties)
   */
  @Beta
  protected void createDataset(String datasetName, String typeName, DatasetProperties properties) {
    configurer.createDataset(datasetName, typeName, properties);
  }

  /**
   * Calls {@link ApplicationConfigurer#createDataset(String, String, DatasetProperties)}, passing the dataset class
   * and properties.
   *
   * @see ApplicationConfigurer#createDataset(String, Class, co.cask.cdap.api.dataset.DatasetProperties)
   */
  protected void createDataset(String datasetName,
                               Class<? extends Dataset> datasetClass,
                               DatasetProperties properties) {
    configurer.createDataset(datasetName, datasetClass, properties);
  }

  /**
   * Calls {@link ApplicationConfigurer#createDataset(String, Class, DatasetProperties)}, passing empty properties.
   *
   * @see ApplicationConfigurer#createDataset(String, Class, DatasetProperties)
   */
  protected void createDataset(String datasetName,
                               Class<? extends Dataset> datasetClass) {
    configurer.createDataset(datasetName, datasetClass, DatasetProperties.EMPTY);
  }

  /**
   * @see ApplicationConfigurer#addFlow(Flow)
   */
  protected void addFlow(Flow flow) {
    configurer.addFlow(flow);
  }

  /**
   * @see ApplicationConfigurer#addProcedure(Procedure)
   * @deprecated As of version 2.6.0,  replaced by {@link co.cask.cdap.api.service.Service}
   */
  @Deprecated
  protected void addProcedure(Procedure procedure) {
    configurer.addProcedure(procedure);
  }

  /**
   * @see ApplicationConfigurer#addProcedure(Procedure, int)
   * @deprecated As of version 2.6.0, replaced by {@link co.cask.cdap.api.service.Service}
   */
  @Deprecated
  protected void addProcedure(Procedure procedure, int instances) {
    configurer.addProcedure(procedure, instances);
  }

  /**
   * @see ApplicationConfigurer#addMapReduce(MapReduce)
   */
  protected void addMapReduce(MapReduce mapReduce) {
    configurer.addMapReduce(mapReduce);
  }

  /**
   * @see ApplicationConfigurer#addSpark(Spark)
   */
  protected void addSpark(Spark spark) {
    configurer.addSpark(spark);
  }

  /**
   * @see ApplicationConfigurer#addWorkflow(Workflow)
   */
  protected void addWorkflow(Workflow workflow) {
    configurer.addWorkflow(workflow);
  }

  /**
   * @see ApplicationConfigurer#addService(Service)
   */
  protected void addService(Service service) {
    configurer.addService(service);
  }

  /**
   * Adds a {@link Service} that consists of the given {@link HttpServiceHandler}.
   *
   * @param name Name of the Service
   * @param handler handler for the Service
   * @param handlers more handlers for the Service
   */
  protected void addService(String name, HttpServiceHandler handler, HttpServiceHandler...handlers) {
    configurer.addService(new BasicService(name, handler, handlers));
  }

  /**
   * Schedules the specified {@link Workflow}
   * @param schedule the schedule to be added for the Workflow
   * @param workflowName the name of the Workflow
   */
  protected void scheduleWorkflow(Schedule schedule, String workflowName) {
    scheduleWorkflow(schedule, workflowName, new HashMap<String, String>());
  }

  /**
   * Schedule the specified {@link Workflow}
   * @param scheduleName the name of the Schedule
   * @param cronTab the crontab entry for the Schedule
   * @param workflowName the name of the Workflow
   */
  protected void scheduleWorkflow(String scheduleName, String cronTab, String workflowName) {
    String scheduleDescription = scheduleName + " with crontab " + cronTab;
    scheduleWorkflow(new Schedule(scheduleName, scheduleDescription, cronTab), workflowName,
                     new HashMap<String, String>());
  }

  /**
   * Schedule the specified {@link Workflow}
   * @param scheduleName the name of the Schedule
   * @param cronTab the crontab entry for the Schedule
   * @param workflowName the name of the Workflow
   * @param properties properties to be added for the Schedule
   */
  protected void scheduleWorkflow(String scheduleName, String cronTab, String workflowName,
                                  Map<String, String> properties) {
    String scheduleDescription = scheduleName + " with crontab " + cronTab;
    scheduleWorkflow(new Schedule(scheduleName, scheduleDescription, cronTab), workflowName, properties);
  }

  /**
   * Schedule the specified {@link Workflow}
   * @param schedule the schedule to be added for the Workflow
   * @param workflowName the name of the Workflow
   * @param properties properties to be added for the Schedule
   */
  protected void scheduleWorkflow(Schedule schedule, String workflowName, Map<String, String> properties) {
    configurer.addSchedule(schedule, SchedulableProgramType.WORKFLOW, workflowName, properties);
  }
}
