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
import javax.annotation.Nullable;

/**
 * Provides the specification of the Manifest.
 */
public class ManifestSpecification {
  private final String programType;
  private final String programName;
  private final Schedule schedule;
  private final int instances;
  private final Map<String, String> programArgs;

  public ManifestSpecification(String programType, String programName, Schedule schedule, int instances,
                               Map<String, String> programArgs) {
    this.programType = programType;
    this.programName = programName;
    this.schedule = schedule;
    this.instances = instances;
    this.programArgs = programArgs;
  }

  /**
   * Type of the Program to be started.
   *
   * @return program type
   */
  public String getProgramType() {
    return programType;
  }

  /**
   * Name of the Program to be started.
   *
   * @return program name
   */
  public String getProgramName() {
    return programName;
  }

  /**
   * Schedule of the Program if applicable.
   *
   * @return schedule for the program or null
   */
  @Nullable
  public Schedule getSchedule() {
    return schedule;
  }

  /**
   * Number of instances of the Program if applicable.
   *
   * @return instances of the program or null
   */
  @Nullable
  public Integer getInstances() {
    return instances;
  }

  /**
   * Arguments to be passed to the program.
   *
   * @return program arguments
   */
  public Map<String, String> getProgramArgs() {
    return programArgs;
  }
}
