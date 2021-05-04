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
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.schedule.ScheduleBuilder;
import io.cdap.cdap.api.schedule.TriggerFactory;
import io.cdap.cdap.api.service.Service;
import io.cdap.cdap.api.spark.Spark;
import io.cdap.cdap.api.worker.Worker;
import io.cdap.cdap.api.workflow.Workflow;
import io.cdap.cdap.internal.schedule.ScheduleCreationSpec;

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
   * Get the application name, this will be the application name provided when deploying the application, or
   * the name set in {@link #setName(String)}. If it is not provided in either way, it will be the simple class name
   * of the app.
   *
   * @return the application name
   */
  String getName();

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
   * Emit the given {@link Metadata} for the given metadata entity. If the given metadata entity already exists,
   * existing tags and properties will be updated.
   *
   * @param metadataEntity the entity to emit metadata
   * @param metadata the metadata to emit
   */
  void emitMetadata(MetadataEntity metadataEntity, Metadata metadata);

  /**
   * Get a TriggerFactory to get triggers.
   *
   * @return The {@link TriggerFactory} used to get triggers
   */
  TriggerFactory getTriggerFactory();
}
