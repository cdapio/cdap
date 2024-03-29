/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package io.cdap.cdap.api.customaction;

import io.cdap.cdap.api.DatasetConfigurer;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.workflow.Workflow;
import java.util.Map;

/**
 * Configurer for configuring the {@link CustomAction} in the {@link Workflow}.
 */
public interface CustomActionConfigurer extends PluginConfigurer, DatasetConfigurer {

  /**
   * Sets the name of the {@link CustomAction}.
   *
   * @param name name of the CustomAction
   */
  void setName(String name);

  /**
   * Sets the description of the {@link CustomAction}.
   *
   * @param description description of the CustomAction
   */
  void setDescription(String description);

  /**
   * Sets a map of properties that will be available through {@link CustomActionSpecification} at
   * runtime.
   *
   * @param properties properties
   */
  void setProperties(Map<String, String> properties);
}
