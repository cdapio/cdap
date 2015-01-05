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
 * Configurer for configuring the {@link Workflow}
 */
public interface WorkflowConfigurer {

  /**
   * Sets the name of the {@link Workflow}
   *
   * @param name of the {@link Workflow}
   */
  void setName(String name);

  /**
   * Sets the description of the {@link Workflow}
   *
   * @param description of the {@link Workflow}
   */
  void setDescription(String description);

  /**
   * Sets the map of properties that will be available through the {@link WorkflowSpecification#getProperties()}
   * at runtime
   *
   * @param properties the properties to set
   */
  void setProperties(Map<String, String> properties);

  /**
   * Add MapReduce program to the {@link Workflow}
   *
   * @param mapReduce name of the MapReduce program to be added to the {@link Workflow}
   * Make sure to configure the MapReduce program with the Application
   */
  void addMapReduce(String mapReduce);

  /**
   * Add Spark program to the {@link Workflow}
   *
   * @param spark name of the Spark program to be added to the {@link Workflow}
   * Make sure to configure the Spark program with the Application
   */
  void addSpark(String spark);

  /**
   * Add Custom action to the {@link Workflow}
   *
   * @param action to be added to the {@link Workflow}
   */
  void addAction(WorkflowAction action);

  /**
   * Add schedule to the {@link Workflow}
   *
   * @param schedule name of the Schedule to be added to the {@link Workflow}
   * Make sure to configure the Schedule with the Application
   */
  void addSchedule(String schedule);
}
