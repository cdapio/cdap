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

import co.cask.cdap.api.schedule.Schedule;

import java.util.Map;

/**
 * Configurer used to configure program used in the execution of the Manifest.
 */
public interface ManifestConfigurer {

  /**
   * Set the program type.
   *
   * @param type program type
   */
  public void setProgramType(String type);

  /**
   * Set the name of the program.
   *
   * @param name program name
   */
  public void setProgramName(String name);

  /**
   * Set the schedule for the program.
   *
   * @param schedule {@link Schedule}
   */
  public void setSchedule(Schedule schedule);

  /**
   * Set the number of instances of the program.
   *
   * @param instances number of instances
   */
  public void setInstances(int instances);

  /**
   * Set the arguments to be passed to the program as runtime arguments.
   *
   * @param programArgs
   */
  public void setProgramArgs(Map<String, String> programArgs);
}
