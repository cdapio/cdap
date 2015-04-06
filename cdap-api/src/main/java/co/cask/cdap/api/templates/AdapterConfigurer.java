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

package co.cask.cdap.api.templates;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.schedule.Schedule;

import java.util.Map;

/**
 * Configurer used to configure program used in the execution of the Adapter.
 * Currently, only Worker or Workflow can be used in the execution.
 */
@Beta
public interface AdapterConfigurer {

  /**
   * Set the schedule for the program. Must be set for Workflows and is not valid for other program types.
   *
   * @param schedule {@link Schedule}
   */
  public void setSchedule(Schedule schedule);

  /**
   * Set the number of instances of the program. Valid only for Workers and defaults to 1.
   *
   * @param instances number of instances
   */
  public void setInstances(int instances);

  /**
   * Set the resources the program should use.
   *
   * @param resources
   */
  public void setResources(Resources resources);

  /**
   * Add arguments to be passed to the program as runtime arguments.
   *
   * @param arguments runtime arguments
   */
  public void addRuntimeArguments(Map<String, String> arguments);

  /**
   * Add argument to be passed to the program as runtime arguments.
   *
   * @param key key
   * @param value value
   */
  public void addRuntimeArgument(String key, String value);
}
