/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.internal.pipeline;

import io.cdap.cdap.api.plugin.Requirements;

import java.util.Objects;

/**
 * A wrapper class around plugin name, type and it's requirements
 */
public class PluginRequirement {
  private final String name;
  private final String type;
  private final Requirements requirements;

  public PluginRequirement(String name, String type, Requirements requirements) {
    this.name = name;
    this.type = type;
    this.requirements = requirements;
  }

  /**
   * @return the name of the plugin
   */
  public String getName() {
    return name;
  }

  /**
   * @return the type of the plugin
   */
  public String getType() {
    return type;
  }

  /**
   * @return {@link Requirements} containing requirements of the plugin
   */
  public Requirements getRequirements() {
    return requirements;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PluginRequirement that = (PluginRequirement) o;
    return Objects.equals(name, that.name) &&
      Objects.equals(type, that.type) &&
      Objects.equals(requirements, that.requirements);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, requirements);
  }

  @Override
  public String toString() {
    return "PluginRequirement{" +
      "name='" + name + '\'' +
      ", type='" + type + '\'' +
      ", requirements=" + requirements +
      '}';
  }
}
