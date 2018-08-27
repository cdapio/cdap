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

package co.cask.cdap.internal.pipeline;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * A wrapper class around plugin name, type and {@link Set} of requirements.
 */
public class PluginRequirement {
  private final String name;
  private final String type;
  private final Set<String> requirements;

  public PluginRequirement(String name, String type, Set<String> requirements) {
    this.name = name;
    this.type = type;
    this.requirements = Collections.unmodifiableSet(new HashSet<>(requirements));
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public Set<String> getRequirements() {
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
