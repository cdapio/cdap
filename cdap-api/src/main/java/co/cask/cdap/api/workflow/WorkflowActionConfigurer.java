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

import co.cask.cdap.api.dataset.Dataset;

import java.util.Map;

/**
 * Configurer for configuring {@link WorkflowAction}
 */
public interface WorkflowActionConfigurer {

  /**
   * Sets the name of the {@link WorkflowAction}
   *
   * @param name name of the WorkflowAction
   */
  void setName(String name);

  /**
   * Sets the description of the {@link WorkflowAction}
   *
   * @param description description of the WorkflowAction
   */
  void setDescription(String description);

  /**
   * Sets a map of properties that will be available through {@link WorkflowActionSpecification} at runtime.
   *
   * @param properties properties
   */
  void setProperties(Map<String, String> properties);

  /**
   * Adds the names of {@link Dataset}s used by this workflow action.
   *
   * @param datasets dataset names
   */
  void useDatasets(Iterable<String> datasets);
}
