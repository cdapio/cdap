/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.proto.v2;

import java.util.Collections;
import java.util.List;

/**
 * The mapping between triggering pipeline properties to the triggered pipeline arguments.
 */
public class TriggeringPropertyMapping {
  private final List<ArgumentMapping> arguments;
  private final List<PluginPropertyMapping> pluginProperties;

  public TriggeringPropertyMapping() {
    this.arguments = Collections.emptyList();
    this.pluginProperties = Collections.emptyList();
  }

  public TriggeringPropertyMapping(List<ArgumentMapping> arguments, List<PluginPropertyMapping> pluginProperties) {
    this.arguments = arguments;
    this.pluginProperties = pluginProperties;
  }

  /**
   * @return The list of mapping between triggering pipeline arguments to triggered pipeline arguments
   */
  public List<ArgumentMapping> getArguments() {
    return arguments;
  }

  /**
   * @return The list of mapping between triggering pipeline plugin properties to triggered pipeline arguments
   */
  public List<PluginPropertyMapping> getPluginProperties() {
    return pluginProperties;
  }

  @Override
  public String toString() {
    return "TriggeringPropertyMapping{" +
      "arguments=" + getArguments() +
      ", pluginProperties=" + getPluginProperties() +
      '}';
  }
}
