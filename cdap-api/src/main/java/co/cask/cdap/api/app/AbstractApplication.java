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

import co.cask.cdap.api.Config;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.mapreduce.MapReduce;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.ScheduleBuilder;
import co.cask.cdap.api.service.BasicService;
import co.cask.cdap.api.service.Service;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.worker.Worker;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.internal.api.AbstractPluginConfigurable;
import co.cask.cdap.internal.schedule.ScheduleCreationSpec;

import java.util.Collections;
import java.util.Map;

/**
 * A support class for {@link Application Applications} which reduces repetition and results in
 * a more readable configuration.
 *
 * <p>
 * Implement the {@link #configure()} method to define your application.
 * </p>
 *
 * @param <T> {@link Config} config class that represents the configuration of the Application.
 * @see co.cask.cdap.api.app
 */
public abstract class AbstractApplication<T extends Config> extends AbstractPluginConfigurable<ApplicationConfigurer>
  implements Application<T> {
  private ApplicationContext context;
  private ApplicationConfigurer configurer;

  /**
   * Override this method to declare and configure the application.
   */
  public abstract void configure();

  @Override
  public final void configure(ApplicationConfigurer configurer, ApplicationContext<T> context) {
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
  protected final ApplicationContext<T> getContext() {
    return context;
  }

  /**
   * Get the configuration object.
   *
   * @return application configuration provided during application creation
   */
  protected T getConfig() {
    return getContext().getConfig();
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
   * @see ApplicationConfigurer#addFlow(Flow)
   */
  protected void addFlow(Flow flow) {
    configurer.addFlow(flow);
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
   * @see ApplicationConfigurer#addWorker(Worker)
   */
  protected void addWorker(Worker worker) {
    configurer.addWorker(worker);
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
   * @deprecated Deprecated as of 4.2.0. Please use {@link #schedule(ScheduleCreationSpec)} instead.
   */
  @Deprecated
  protected void scheduleWorkflow(Schedule schedule, String workflowName) {
    scheduleWorkflow(schedule, workflowName, Collections.<String, String>emptyMap());
  }


  /**
   * Schedule the specified {@link Workflow}
   * @param schedule the schedule to be added for the Workflow
   * @param workflowName the name of the Workflow
   * @param properties properties to be added for the Schedule
   * @deprecated Deprecated as of 4.2.0. Please use {@link #schedule(ScheduleCreationSpec)} instead.
   */
  @Deprecated
  protected void scheduleWorkflow(Schedule schedule, String workflowName, Map<String, String> properties) {
    configurer.addSchedule(schedule, SchedulableProgramType.WORKFLOW, workflowName, properties);
  }

  /**
   * Get a ScheduleBuilder for the specified program.
   * @param scheduleName the name of the schedule
   * @param programType the type of the program; currently, only ProgramType.WORKFLOW can be scheduled
   * @param programName the name of the program
   *
   * @return The {@link ScheduleBuilder} used to build the schedule
   */
  protected ScheduleBuilder buildSchedule(String scheduleName, ProgramType programType,
                                          String programName) {
    return configurer.buildSchedule(scheduleName, programType, programName);
  }

  /**
   * Schedules a program, using the given scheduleCreationSpec.
   *
   * @param scheduleCreationSpec defines the schedule. Can be built using the builder obtained
   *                             from {@link #buildSchedule(String, ProgramType, String)}
   */
  protected void schedule(ScheduleCreationSpec scheduleCreationSpec) {
    configurer.schedule(scheduleCreationSpec);
  }
}
