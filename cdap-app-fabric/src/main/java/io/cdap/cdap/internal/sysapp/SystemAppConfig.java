/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.sysapp;

import java.util.Collections;
import java.util.List;

/**
 * Specified operations to perform for system app in a CDAP instance.
 */
public class SystemAppConfig {
  public static final SystemAppConfig EMPTY = new SystemAppConfig(Collections.emptyList());
  private final List<SystemAppStep> steps;

  public SystemAppConfig(List<SystemAppStep> steps) {
    this.steps = steps;
  }

  public List<SystemAppStep> getSteps() {
    // can be null when deserialized through gson
    return steps == null ? Collections.emptyList() : Collections.unmodifiableList(steps);
  }

  /**
   * Check that this is a valid config, throwing an exception if not.
   *
   * @throws IllegalArgumentException if the config is invalid
   */
  public void validate() {
    steps.forEach(SystemAppStep::validate);
  }
}
