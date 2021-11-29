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

package io.cdap.cdap.api.app;

import io.cdap.cdap.api.DatasetConfigurer;
import io.cdap.cdap.api.mapreduce.MapReduce;
import io.cdap.cdap.api.metadata.Metadata;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.schedule.ScheduleBuilder;
import io.cdap.cdap.api.schedule.TriggerFactory;
import io.cdap.cdap.api.service.Service;
import io.cdap.cdap.api.spark.Spark;
import io.cdap.cdap.api.worker.Worker;
import io.cdap.cdap.api.workflow.Workflow;
import io.cdap.cdap.internal.schedule.ScheduleCreationSpec;

import javax.annotation.Nullable;

/**
 * Configures a CDAP Application.
 */
public interface ApplicationConfigurer extends PluginConfigurer, DatasetConfigurer {

  /**
   * Sets the name of the Application.
   *
   * @param name name
   */
  void setName(String name);

  /**
   * Sets the description of the Application.
   *
   * @param description description
   */
  void setDescription(String description);

  /**
   * Adds a {@link MapReduce MapReduce job} to the Application. Use it when you need to re-use existing MapReduce jobs
   * that rely on Hadoop MapReduce APIs.
   *
   * @param mapReduce The {@link MapReduce MapReduce job} to include in the Application
   */
  void addMapReduce(MapReduce mapReduce);

  /**
   * Adds a {@link Spark} job to the Application.
   *
   * @param spark The {@link Spark} job to include in the Application
   */
  void addSpark(Spark spark);

  /**
   * Adds a {@link Workflow} to the Application.
   *
   * @param workflow The {@link Workflow} to include in the Application
   */
  void addWorkflow(Workflow workflow);

  /**
   * Adds a custom {@link Service} to the Application.
   *
   * @param service The service to include in the Application
   */
  void addService(Service service);

  /**
   * Adds a {@link Worker} to the Application.
   *
   * @param worker The worker to include in the Application
   */
  void addWorker(Worker worker);

  /**
   * Get a ScheduleBuilder for the specified program.
   * @param scheduleName the name of the schedule
   * @param programType the type of the program; currently, only ProgramType.WORKFLOW can be scheduled
   * @param programName the name of the program
   *
   * @return The {@link ScheduleBuilder} used to build the schedule
   */
  ScheduleBuilder buildSchedule(String scheduleName, ProgramType programType,
                                String programName);

  /**
   * Schedules a program, using the given scheduleCreationSpec.
   *
   * @param scheduleCreationSpec defines the schedule.
   */
  void schedule(ScheduleCreationSpec scheduleCreationSpec);

  /**
   * Emit the given {@link Metadata} for the application in the given scope.
   * Note the tags and properties emitted in SYSTEM scope will get overridden by the platform system metadata if
   * the tags or property keys are same.
   *
   * @param metadata the metadata to emit
   */
  default void emitMetadata(Metadata metadata, MetadataScope scope) {
    throw new UnsupportedOperationException("Emitting metadata for applications is not supported.");
  }

  /**
   * Get a TriggerFactory to get triggers.
   *
   * @return The {@link TriggerFactory} used to get triggers
   */
  TriggerFactory getTriggerFactory();

  /**
   * Return the runtime configurer that contains the runtime arguments and provides access for other runtime
   * functionalities. This is used for the app to provide additional information for the newly generated app spec
   * before each program run. This method will return null when the app initially gets deployed.
   *
   * @return the runtime configurer, or null if this is the initial deploy time.
   */
  @Nullable
  default RuntimeConfigurer getRuntimeConfigurer() {
    return null;
  }

  /**
   * Return the namespace the app is deployed.
   *
   * @return the namespace the app is deployed
   */
  default String getDeployedNamespace() {
    throw new UnsupportedOperationException("Getting deployed namespace is not supported");
  }
}
