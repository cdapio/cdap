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

package co.cask.cdap.api.workflow;

import java.util.Map;

/**
 * This abstract class provides a default implementation of {@link Workflow} methods for easy extension.
 */
public abstract class AbstractWorkflow implements Workflow {

  private WorkflowConfigurer configurer;

  @Override
  public final void configure(WorkflowConfigurer configurer) {
    this.configurer = configurer;
    configure();
  }

  /**
   * Override this method to configure this {@link Workflow} job.
   */
  protected void configure() {
    // Default no-op
  }

  /**
   * Returns the {@link WorkflowConfigurer}, only available at configuration time.
   */
  protected final WorkflowConfigurer getConfigurer() {
    return configurer;
  }

  /**
   * Sets the name of the {@link Workflow}.
   */
  protected final void setName(String name) {
    configurer.setName(name);
  }

  /**
   * Sets the description of the {@link Workflow}.
   */
  protected final void setDescription(String description) {
    configurer.setDescription(description);
  }

  /**
   * Sets a set of properties that will be available through the {@link WorkflowSpecification#getProperties()}
   * at runtime.
   *
   * @param properties the properties to set
   */
  protected final void setProperties(Map<String, String> properties) {
    configurer.setProperties(properties);
  }

  /**
   * Adds custom action to the {@link Workflow}
   * @param action the action to be added
   */
  protected final void addAction(WorkflowAction action) {
    configurer.addAction(action);
  }

  /**
   * Adds MapReduce program to the {@link Workflow}
   * @param mapReduce the name of MapReduce program to be added
   */
  protected final void addMapReduce(String mapReduce) {
    configurer.addMapReduce(mapReduce);
  }

  /**
   * Adds Spark program to the {@link Workflow}
   * @param spark the name of the Spark program to be added
   */
  protected final void addSpark(String spark) {
    configurer.addSpark(spark);
  }

  /**
   * Adds schedule to the {@link Workflow}
   * @param schedule the name of the Schedule to be added
   */
  protected final void addSchedule(String schedule) {
    configurer.addSchedule(schedule);
  }
}
