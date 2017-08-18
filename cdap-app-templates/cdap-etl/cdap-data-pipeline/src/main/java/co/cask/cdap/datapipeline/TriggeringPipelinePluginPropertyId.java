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

package co.cask.cdap.datapipeline;

import java.util.Objects;

/**
 * Identifier of plugin property from the triggering pipeline.
 */
public class TriggeringPipelinePluginPropertyId extends TriggeringPipelinePropertyId {
  private final String pluginName;
  private final String propertyKey;

  public TriggeringPipelinePluginPropertyId(String namespace, String pipelineName,
                                            String pluginName, String propertyKey) {
    super(Type.PLUGIN_PROPERTY, namespace, pipelineName);
    this.pluginName = pluginName;
    this.propertyKey = propertyKey;
  }

  /**
   * @return The name of the plugin in the triggering pipeline.
   */
  public String getPluginName() {
    return pluginName;
  }

  /**
   * @return The key of the plugin property in the triggering pipeline.
   */
  public String getPropertyKey() {
    return propertyKey;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    TriggeringPipelinePluginPropertyId that = (TriggeringPipelinePluginPropertyId) o;
    return Objects.equals(getPluginName(), that.getPluginName()) &&
      Objects.equals(getPropertyKey(), that.getPropertyKey());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), getPluginName(), getPropertyKey());
  }
}
